package io.github.hyeokjinon.banktransfer.gateway

import io.github.hyeokjinon.banktransfer.protocol.BankMessage
import io.github.hyeokjinon.banktransfer.protocol.BizType
import io.github.hyeokjinon.banktransfer.protocol.Header
import io.github.hyeokjinon.banktransfer.protocol.MessageClass
import io.github.hyeokjinon.banktransfer.protocol.InquiryRequest
import io.github.hyeokjinon.banktransfer.protocol.InquiryResponse
import io.github.hyeokjinon.banktransfer.protocol.NameInquiryRequest
import io.github.hyeokjinon.banktransfer.protocol.NameInquiryResponse
import io.github.hyeokjinon.banktransfer.protocol.PollRequest
import io.github.hyeokjinon.banktransfer.protocol.PollResponse
import io.github.hyeokjinon.banktransfer.protocol.ProtocolException
import io.github.hyeokjinon.banktransfer.protocol.ResponseCodes
import io.github.hyeokjinon.banktransfer.protocol.TransferRequest
import io.github.hyeokjinon.banktransfer.protocol.TransferResponse
import io.github.hyeokjinon.banktransfer.protocol.netty.addBankCodec
import io.netty.bootstrap.Bootstrap
import io.netty.channel.Channel
import io.netty.channel.ChannelFutureListener
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.ChannelInitializer
import io.netty.channel.ChannelOption
import io.netty.channel.MultiThreadIoEventLoopGroup
import io.netty.channel.SimpleChannelInboundHandler
import io.netty.channel.nio.NioIoHandler
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioSocketChannel
import io.netty.handler.timeout.IdleState
import io.netty.handler.timeout.IdleStateEvent
import io.netty.handler.timeout.IdleStateHandler
import org.slf4j.LoggerFactory
import java.time.Clock
import java.time.Duration
import java.time.LocalDateTime
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionException
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.AtomicInteger

data class BankGatewayConfig(
    val host: String,
    val port: Int,
    /** 응답을 기다리는 최대 시간. 넘으면 BankTimeoutException. */
    val responseTimeout: Duration = Duration.ofSeconds(3),
    /** 이 시간 동안 보낸 게 없으면 폴링 전문을 보낸다. */
    val pollInterval: Duration = Duration.ofSeconds(30),
    val reconnectMinDelay: Duration = Duration.ofSeconds(1),
    val reconnectMaxDelay: Duration = Duration.ofSeconds(30),
) {
    /** 폴링 3번 분량 동안 아무것도 못 받으면 회선이 죽은 것으로 본다. */
    val readerIdleTimeout: Duration get() = pollInterval.multipliedBy(3)
}

class BankTimeoutException(val tranNo: Long, val timeout: Duration) :
    RuntimeException("bank did not respond to tranNo=$tranNo within $timeout")

/** 은행과 통신할 수 없다. 보낸 뒤에 끊긴 경우도 포함한다 — 그때는 은행이 요청을 받았을 수 있다. */
open class BankUnavailableException(message: String, cause: Throwable? = null) : RuntimeException(message, cause)

/**
 * 전문을 소켓에 쓰기 전에 실패했다 (연결 없음). 은행에 "확실히" 가지 않았으므로 다시 보내도 안전하다.
 * [reason] = 왜 안 보냈나 (P9a 지표 `gateway.not_sent{reason}` — 메시지 문자열로 가르지 않는다, 스펙 위험 A5).
 */
class BankNotSentException(
    message: String,
    cause: Throwable? = null,
    val reason: NotSentReason = NotSentReason.OTHER,
) : BankUnavailableException(message, cause)

/**
 * "안 보냄" 의 사유 (P9a 스펙 §4-3). 던지는 곳마다 하나를 넣는다. 라벨 값은 소문자 [tag].
 * 점검 시간 보류는 [MAINTENANCE] — 컨슈머 · 정리 작업의 은행 호출은 SendGate 가 막아 이 예외로 끝난다
 * (웹 접수의 점검 503 은 BankMaintenanceException 으로 따로다 — BankMaintenanceGuard).
 */
enum class NotSentReason {
    /** 리더가 아니다 (P8-3b — 대기자 · 임기 사이). */
    NOT_LEADER,

    /** 정상 교대 중 — 새 요청 거절 단계. */
    STEPPING_DOWN,

    /** 전문번호 구간을 아직 예약하지 못했다 (임기 시작 직후 · 예약 실패 · API 서버 단절). */
    NO_TRAN_NO,

    /** 은행 회선이 없다. */
    DISCONNECTED,

    /** 은행이 3001 (전문번호 중복 — 처리 안 함, v0.4). */
    DUPLICATE_TRAN_NO,

    /** 전문번호 저장소 문제 — 깨진 기록 · 날짜 역전 · 하루 한도 소진 · 예약 충돌 반복 · 공급원의 예기치 못한 예외. */
    TRAN_NO_STORE,

    /** 호출자 쪽: 게이트웨이 앱에 연결하지 못했다 (modules/bank-gateway-http). */
    GATEWAY_UNREACHABLE,

    /** 점검 시간 — SendGate 가 닫혀 있다 (P5-1). */
    MAINTENANCE,

    OTHER;

    val tag: String get() = name.lowercase()
}

/**
 * 지금 은행에 보내도 되는지 (P5-1, 점검 시간). 닫혀 있으면 이유를 돌려준다 — 그 요청은 "연결 없음" 과 같이 [BankNotSentException] 으로 끝난다:
 * 이체는 RECEIVED 로 돌아가 재시도 · 재발행, 결과조회는 시도로 세지 않는다 (어댑터는 두 이유를 구분하지 않는다).
 */
fun interface SendGate {
    fun closedReason(): String?

    companion object {
        val OPEN = SendGate { null }
    }
}

/**
 * 은행 FEP 에 TCP 로 붙는 클라이언트.
 *
 * 전 직장 코드와 다르게 바꾼 점
 * 1. 링크 체크: java.util.Timer 대신 IdleStateHandler. 쓰기가 없으면 폴링, 읽기가 오래 없으면 연결을 끊는다.
 * 2. 재접속: 3초 고정 대신 1초부터 두 배씩 늘리는 지수 백오프(최대 30초).
 * 3. 응답 매칭: DB 폴링 대신 전문번호 -> CompletableFuture 맵. 응답이 오면 이벤트 루프가 future 를 완료시킨다.
 *    호출한 스레드는 기다리지 않는다(논블로킹).
 */
class BankGatewayClient(
    private val config: BankGatewayConfig,
    private val clock: Clock = Clock.systemDefaultZone(),
    /** 점검 시간 등으로 보내지 않을 때 (P5-1). 기본은 늘 열림. */
    private val sendGate: SendGate = SendGate.OPEN,
    /** 전문번호 발급 (P8-3b). 기본은 이 인스턴스 안의 메모리 카운터 — K8s 에서는 게이트웨이 앱이 ConfigMap 구간 예약을 넣는다. */
    private val tranNoSource: TranNoSource = InMemoryTranNoSource(),
) : BankGateway, AutoCloseable {

    private val log = LoggerFactory.getLogger(javaClass)
    private val group = MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory())
    private val pending = ConcurrentHashMap<Long, CompletableFuture<BankMessage>>()
    private val reconnectAttempts = AtomicInteger(0)

    @Volatile private var channel: Channel? = null
    @Volatile private var running = false

    private val bootstrap: Bootstrap = Bootstrap()
        .group(group)
        .channel(NioSocketChannel::class.java)
        .option(ChannelOption.TCP_NODELAY, true)
        .option(ChannelOption.SO_KEEPALIVE, true)
        .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 3_000)
        .handler(object : ChannelInitializer<SocketChannel>() {
            override fun initChannel(ch: SocketChannel) {
                ch.pipeline()
                    .addLast(
                        "idle",
                        IdleStateHandler(
                            config.readerIdleTimeout.toMillis(), config.pollInterval.toMillis(), 0, TimeUnit.MILLISECONDS,
                        ),
                    )
                    .addBankCodec()
                    .addLast("client", ClientHandler())
            }
        })

    val isConnected: Boolean get() = channel?.isActive == true

    /** 현재 응답을 기다리는 요청 수 (관측용). */
    val pendingCount: Int get() = pending.size

    fun start(): BankGatewayClient {
        running = true
        connect()
        return this
    }

    override fun inquireName(bankCode: String, accountNo: String): CompletableFuture<NameInquiryResponse> =
        request { NameInquiryRequest(newHeader(BizType.NAME_INQUIRY), bankCode, accountNo) }.thenApply { it.expect<NameInquiryResponse>() }

    override fun transfer(bankCode: String, accountNo: String, amount: Long, memo: String, reference: String): CompletableFuture<TransferResponse> =
        request { TransferRequest(newHeader(BizType.TRANSFER), bankCode, accountNo, amount, memo, reference) }.thenApply { it.expect<TransferResponse>() }

    /** 결과조회 (v0.2). 결과를 모르는 이체를 이용기관 거래번호로 묻는다 — 이체를 다시 보내지 않는다. */
    override fun inquire(reference: String): CompletableFuture<InquiryResponse> =
        request { InquiryRequest(newHeader(BizType.INQUIRY), reference) }.thenApply { it.expect<InquiryResponse>() }

    /**
     * 보낼 수 없으면 번호를 쓰기 전에 끝낸다 (구간 예약 번호를 헛되이 쓰지 않게).
     * 번호 발급 실패(BankNotSentException)도 동기로 던지지 않고 future 에 담는다 — 호출자는 실패를 한 경로로만 받는다.
     */
    private fun request(build: () -> BankMessage): CompletableFuture<BankMessage> {
        notSendable()?.let { return CompletableFuture.failedFuture(it) }
        val message = try {
            build()
        } catch (e: BankNotSentException) {
            return CompletableFuture.failedFuture(e)   // 번호 공급원이 붙인 사유 그대로 (NO_TRAN_NO · TRAN_NO_STORE)
        } catch (e: RuntimeException) {   // 번호 공급원의 예기치 못한 실패 — 아직 아무것도 쓰지 않았으므로 "안 보냄" 이 맞다
            return CompletableFuture.failedFuture(BankNotSentException("tran no source failed: ${e.message}", e, NotSentReason.TRAN_NO_STORE))
        }
        return send(message)
    }

    /** 지금 보낼 수 없으면 사유를 담은 예외. 점검(보내기 문)을 먼저 본다 — 점검 중 끊김은 점검으로 센다. */
    private fun notSendable(): BankNotSentException? {
        sendGate.closedReason()?.let { return BankNotSentException(it, reason = NotSentReason.MAINTENANCE) }
        return if (channel?.isActive == true) null else BankNotSentException("bank channel is not connected", reason = NotSentReason.DISCONNECTED)
    }

    /**
     * 요청을 보내고 즉시 future 를 돌려준다.
     * 실패는 future 안에 담긴다: 연결 없음 -> BankNotSentException (안 보냄),
     * 쓰기 실패 · 보낸 뒤 연결 끊김 -> BankUnavailableException (보냈을 수 있음), 시간 초과 -> BankTimeoutException.
     * 보내기 문이 닫혀 있으면(점검 시간, P5-1) 연결이 있어도 쓰지 않고 BankNotSentException 이다. 폴링은 send 를 거치지 않아 점검 중에도 회선은 유지된다.
     * 응답코드 `3001`(전문번호 중복, v0.4)은 은행이 처리하지 않았다는 뜻이라 BankNotSentException 이다 (P8-3b 결정 15).
     */
    fun send(request: BankMessage): CompletableFuture<BankMessage> {
        sendGate.closedReason()?.let { return CompletableFuture.failedFuture(BankNotSentException(it, reason = NotSentReason.MAINTENANCE)) }
        val ch = channel
        if (ch == null || !ch.isActive) {
            return CompletableFuture.failedFuture(BankNotSentException("bank channel is not connected", reason = NotSentReason.DISCONNECTED))
        }
        val tranNo = request.header.tranNo
        val future = CompletableFuture<BankMessage>()
        if (pending.putIfAbsent(tranNo, future) != null) {
            return CompletableFuture.failedFuture(IllegalStateException("duplicate tranNo $tranNo"))
        }
        ch.writeAndFlush(request).addListener(ChannelFutureListener { written ->
            if (!written.isSuccess) future.completeExceptionally(BankUnavailableException("write failed", written.cause()))
        })
        return future
            .orTimeout(config.responseTimeout.toMillis(), TimeUnit.MILLISECONDS)
            .handle { response, error ->
                pending.remove(tranNo, future)
                when (val cause = error?.unwrap()) {
                    null -> if (response.header.responseCode == ResponseCodes.DUPLICATE_TRAN_NO) {
                        throw BankNotSentException(
                            "bank rejected duplicate tranNo $tranNo (${ResponseCodes.DUPLICATE_TRAN_NO}) — not processed",
                            reason = NotSentReason.DUPLICATE_TRAN_NO,
                        )
                    } else {
                        response
                    }
                    is TimeoutException -> throw BankTimeoutException(tranNo, config.responseTimeout)
                    else -> throw cause
                }
            }
    }

    override fun close() {
        running = false
        failAllPending(BankUnavailableException("gateway is shutting down"))
        channel?.close()?.awaitUninterruptibly(1, TimeUnit.SECONDS)
        group.shutdownGracefully(0, 1, TimeUnit.SECONDS).awaitUninterruptibly(3, TimeUnit.SECONDS)
    }

    internal fun newHeader(bizType: BizType, messageClass: MessageClass = MessageClass.REQUEST) =
        Header(messageClass, bizType, nextTranNo(), LocalDateTime.now(clock))

    /** 8자리 전문번호 (규격 v0.4: 기관 · 일자별 유일). 발급 규칙은 [TranNoSource] 가 정한다. */
    private fun nextTranNo(): Long = tranNoSource.next()

    private fun connect() {
        if (!running) return
        bootstrap.connect(config.host, config.port).addListener(ChannelFutureListener { future ->
            if (future.isSuccess) {
                channel = future.channel()
                reconnectAttempts.set(0)
                log.info("connected to bank {}:{}", config.host, config.port)
            } else {
                log.warn("connect to bank failed: {}", future.cause()?.message)
                scheduleReconnect()
            }
        })
    }

    private fun scheduleReconnect() {
        if (!running) return
        val attempt = reconnectAttempts.getAndIncrement().coerceAtMost(10)
        val delay = config.reconnectMinDelay.multipliedBy(1L shl attempt).coerceAtMost(config.reconnectMaxDelay)
        log.info("reconnect in {} ms (attempt {})", delay.toMillis(), attempt + 1)
        group.schedule({ connect() }, delay.toMillis(), TimeUnit.MILLISECONDS)
    }

    private fun failAllPending(cause: Throwable) {
        pending.values.forEach { it.completeExceptionally(cause) }
    }

    private inner class ClientHandler : SimpleChannelInboundHandler<BankMessage>() {
        override fun channelRead0(ctx: ChannelHandlerContext, msg: BankMessage) {
            when (msg) {
                is PollResponse -> log.debug("poll ok tranNo={}", msg.header.tranNo)
                is PollRequest -> ctx.writeAndFlush(PollResponse(msg.header.toResponse()))
                else -> {
                    val waiting = pending[msg.header.tranNo]
                    if (waiting == null) {
                        // 시간 초과로 이미 포기한 요청의 늦은 응답. 실제 서비스라면 결과조회·대사로 정합성을 맞춰야 한다.
                        log.warn("late or unknown response tranNo={} type={}", msg.header.tranNo, msg::class.simpleName)
                    } else {
                        waiting.complete(msg)
                    }
                }
            }
        }

        override fun userEventTriggered(ctx: ChannelHandlerContext, evt: Any) {
            if (evt !is IdleStateEvent) return super.userEventTriggered(ctx, evt)
            when (evt.state()) {
                IdleState.WRITER_IDLE -> {
                    // 번호를 못 받으면(구간 예약 전 · API 서버 단절) 이번 폴링만 건너뛴다 — 예외가 exceptionCaught 로 가면 회선을 끊는다
                    val header = try {
                        newHeader(BizType.POLL, MessageClass.ADMIN_REQUEST)
                    } catch (e: BankNotSentException) {
                        log.warn("poll skipped: {}", e.message)
                        return
                    }
                    ctx.writeAndFlush(PollRequest(header))
                }
                IdleState.READER_IDLE -> {
                    log.warn("no data from bank for {}, closing channel", config.readerIdleTimeout)
                    ctx.close()
                }
                else -> Unit
            }
        }

        override fun channelInactive(ctx: ChannelHandlerContext) {
            log.warn("bank channel closed")
            channel = null
            failAllPending(BankUnavailableException("connection to bank lost"))
            scheduleReconnect()
            super.channelInactive(ctx)
        }

        override fun exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) {
            log.error("bank channel error, closing", cause)
            ctx.close()
        }
    }
}

private fun Throwable.unwrap(): Throwable = if (this is CompletionException && cause != null) cause!! else this

private inline fun <reified T : BankMessage> BankMessage.expect(): T =
    this as? T ?: throw ProtocolException("expected ${T::class.simpleName} but got ${this::class.simpleName}")
