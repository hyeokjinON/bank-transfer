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
import java.util.concurrent.atomic.AtomicLong

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

class BankTimeoutException(val tranNo: Long, timeout: Duration) :
    RuntimeException("bank did not respond to tranNo=$tranNo within $timeout")

/** 은행과 통신할 수 없다. 보낸 뒤에 끊긴 경우도 포함한다 — 그때는 은행이 요청을 받았을 수 있다. */
open class BankUnavailableException(message: String, cause: Throwable? = null) : RuntimeException(message, cause)

/** 전문을 소켓에 쓰기 전에 실패했다 (연결 없음). 은행에 "확실히" 가지 않았으므로 다시 보내도 안전하다. */
class BankNotSentException(message: String) : BankUnavailableException(message)

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
) : AutoCloseable {

    private val log = LoggerFactory.getLogger(javaClass)
    private val group = MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory())
    private val pending = ConcurrentHashMap<Long, CompletableFuture<BankMessage>>()
    private val tranNoSequence = AtomicLong(0)
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

    fun inquireName(bankCode: String, accountNo: String): CompletableFuture<NameInquiryResponse> =
        send(NameInquiryRequest(newHeader(BizType.NAME_INQUIRY), bankCode, accountNo)).thenApply { it.expect<NameInquiryResponse>() }

    fun transfer(bankCode: String, accountNo: String, amount: Long, memo: String, reference: String): CompletableFuture<TransferResponse> =
        send(TransferRequest(newHeader(BizType.TRANSFER), bankCode, accountNo, amount, memo, reference)).thenApply { it.expect<TransferResponse>() }

    /** 결과조회 (v0.2). 결과를 모르는 이체를 이용기관 거래번호로 묻는다 — 이체를 다시 보내지 않는다. */
    fun inquire(reference: String): CompletableFuture<InquiryResponse> =
        send(InquiryRequest(newHeader(BizType.INQUIRY), reference)).thenApply { it.expect<InquiryResponse>() }

    /**
     * 요청을 보내고 즉시 future 를 돌려준다.
     * 실패는 future 안에 담긴다: 연결 없음 -> BankNotSentException (안 보냄),
     * 쓰기 실패 · 보낸 뒤 연결 끊김 -> BankUnavailableException (보냈을 수 있음), 시간 초과 -> BankTimeoutException.
     */
    fun send(request: BankMessage): CompletableFuture<BankMessage> {
        val ch = channel
        if (ch == null || !ch.isActive) {
            return CompletableFuture.failedFuture(BankNotSentException("bank channel is not connected"))
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
                    null -> response
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

    /** 8자리 전문번호. 다 쓰면 1부터 다시 쓴다(은행과 합의해야 하는 부분). */
    private fun nextTranNo(): Long = tranNoSequence.updateAndGet { if (it >= Header.MAX_TRAN_NO) 1 else it + 1 }

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
                IdleState.WRITER_IDLE -> ctx.writeAndFlush(PollRequest(newHeader(BizType.POLL, MessageClass.ADMIN_REQUEST)))
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
