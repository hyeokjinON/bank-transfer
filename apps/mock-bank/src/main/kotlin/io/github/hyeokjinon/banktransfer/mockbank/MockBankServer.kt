package io.github.hyeokjinon.banktransfer.mockbank

import io.github.hyeokjinon.banktransfer.protocol.BankMessage
import io.github.hyeokjinon.banktransfer.protocol.InquiryRequest
import io.github.hyeokjinon.banktransfer.protocol.InquiryResponse
import io.github.hyeokjinon.banktransfer.protocol.NameInquiryRequest
import io.github.hyeokjinon.banktransfer.protocol.NameInquiryResponse
import io.github.hyeokjinon.banktransfer.protocol.PollRequest
import io.github.hyeokjinon.banktransfer.protocol.PollResponse
import io.github.hyeokjinon.banktransfer.protocol.ResponseCodes
import io.github.hyeokjinon.banktransfer.protocol.TransferRequest
import io.github.hyeokjinon.banktransfer.protocol.TransferResponse
import io.github.hyeokjinon.banktransfer.protocol.netty.addBankCodec
import io.netty.bootstrap.ServerBootstrap
import io.netty.channel.Channel
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.ChannelInitializer
import io.netty.channel.ChannelOption
import io.netty.channel.group.DefaultChannelGroup
import io.netty.util.concurrent.GlobalEventExecutor
import io.netty.channel.MultiThreadIoEventLoopGroup
import io.netty.channel.SimpleChannelInboundHandler
import io.netty.channel.nio.NioIoHandler
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioServerSocketChannel
import org.slf4j.LoggerFactory
import java.net.InetSocketAddress
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

/**
 * 모의 은행 FEP. 실제 은행 대신 같은 전문 규격으로 응답한다.
 *
 * - 성명조회: 등록된 계좌면 예금주명, 없으면 응답코드 1001
 * - 이체: 등록된 계좌면 입금 기록을 남기고 0000 (통장 거래내역 역할)
 * - 폴링: 바로 응답
 * - silentAccounts: 응답을 보내지 않는다 (타임아웃 테스트용)
 * - 결과조회(v0.2): 처리한 이체의 이용기관 거래번호면 0000, 아니면 2001
 * - silentAfterProcessingAccounts: 이체를 처리(입금 · 거래번호 기록)하고 응답만 보내지 않는다. 단 1원 이체는 정상 응답 — 1원인증으로 수취 계좌를 등록할 수 있게 (결과조회 시나리오용)
 * - pause/resume: 은행 장애 흉내 (새 연결을 받지 않고 붙은 연결도 끊었다가, 같은 포트로 다시 연다). 입금 · 처리 기록은 그대로 (P3-3b)
 */
class MockBankServer(
    private val port: Int = 0,
    private val accounts: Map<String, String> = DEFAULT_ACCOUNTS,
    private val silentAccounts: Set<String> = emptySet(),
    private val silentAfterProcessingAccounts: Set<String> = DEFAULT_SILENT_AFTER_PROCESSING,
    private val responseDelay: Duration = Duration.ZERO,
) : AutoCloseable {

    private val log = LoggerFactory.getLogger(javaClass)
    private val boss = MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory())
    private val workers = MultiThreadIoEventLoopGroup(2, NioIoHandler.newFactory())
    private val deposits = ConcurrentHashMap<String, MutableList<TransferRequest>>()
    private val processedReferences: MutableSet<String> = ConcurrentHashMap.newKeySet()
    private val pollCounter = AtomicInteger()
    private lateinit var serverChannel: Channel
    private val clients = DefaultChannelGroup(GlobalEventExecutor.INSTANCE)

    /** port = 0 이면 OS 가 고른 포트를 기억해 둔다 — resume 이 같은 포트로 다시 열어야 게이트웨이가 그대로 다시 붙는다. */
    @Volatile private var listenPort = port

    private val bootstrap: ServerBootstrap = ServerBootstrap()
        .group(boss, workers)
        .channel(NioServerSocketChannel::class.java)
        // pause 가 끊은 연결이 TIME_WAIT 로 남아도 같은 포트에 다시 bind 할 수 있게 (리눅스 CI)
        .option(ChannelOption.SO_REUSEADDR, true)
        .childHandler(object : ChannelInitializer<SocketChannel>() {
            override fun initChannel(ch: SocketChannel) {
                clients.add(ch)
                ch.pipeline().addBankCodec().addLast(BankHandler())
            }
        })

    val boundPort: Int get() = listenPort
    val pollCount: Int get() = pollCounter.get()

    fun start(): MockBankServer {
        serverChannel = bootstrap.bind(listenPort).sync().channel()
        listenPort = (serverChannel.localAddress() as InetSocketAddress).port
        log.info("mock bank listening on {}", listenPort)
        return this
    }

    /** 은행 장애 흉내: 새 연결을 받지 않고, 붙어 있던 연결도 끊는다. 입금 내역 · 처리한 거래번호는 남긴다. */
    fun pause() {
        if (serverChannel.isOpen) serverChannel.close().syncUninterruptibly()
        clients.close().awaitUninterruptibly(1, TimeUnit.SECONDS)
        log.info("mock bank paused on {}", listenPort)
    }

    /** pause 전과 같은 포트로 다시 연다. 이미 열려 있으면 아무것도 하지 않는다. */
    fun resume(): MockBankServer {
        if (!serverChannel.isOpen) {
            serverChannel = bootstrap.bind(listenPort).sync().channel()
            log.info("mock bank resumed on {}", listenPort)
        }
        return this
    }

    /** 이 계좌로 들어온 입금 내역 (받는 사람이 통장에서 보는 적요 확인용). */
    fun depositsOf(accountNo: String): List<TransferRequest> = deposits[accountNo]?.toList() ?: emptyList()

    fun hasProcessed(reference: String): Boolean = reference in processedReferences

    override fun close() {
        if (::serverChannel.isInitialized) serverChannel.close().awaitUninterruptibly(1, TimeUnit.SECONDS)
        boss.shutdownGracefully(0, 1, TimeUnit.SECONDS)
        workers.shutdownGracefully(0, 1, TimeUnit.SECONDS).awaitUninterruptibly(3, TimeUnit.SECONDS)
    }

    internal fun respond(message: BankMessage): BankMessage? = when (message) {
        is PollRequest -> {
            pollCounter.incrementAndGet()
            PollResponse(message.header.toResponse())
        }
        is NameInquiryRequest -> when {
            message.accountNo in silentAccounts -> null
            else -> {
                val name = accounts[message.accountNo]
                val code = if (name == null) ResponseCodes.ACCOUNT_NOT_FOUND else ResponseCodes.OK
                NameInquiryResponse(message.header.toResponse(code), message.bankCode, message.accountNo, name.orEmpty())
            }
        }
        is TransferRequest -> when {
            message.accountNo in silentAccounts -> null
            message.accountNo !in accounts -> message.reply(ResponseCodes.ACCOUNT_NOT_FOUND)
            else -> {
                deposit(message)
                // 처리는 했는데 응답이 사라진 상황 (결과조회로만 확정할 수 있다)
                if (message.accountNo in silentAfterProcessingAccounts && message.amount > 1) null else message.reply(ResponseCodes.OK)
            }
        }
        is InquiryRequest -> {
            val result = if (message.reference in processedReferences) ResponseCodes.OK else ResponseCodes.ORIGINAL_NOT_FOUND
            log.info("[결과조회] {} -> {}", message.reference, result)
            InquiryResponse(message.header.toResponse(), message.reference, result)
        }
        is PollResponse, is NameInquiryResponse, is TransferResponse, is InquiryResponse -> null
    }

    private fun deposit(message: TransferRequest) {
        deposits.computeIfAbsent(message.accountNo) { CopyOnWriteArrayList() }.add(message)
        processedReferences += message.reference
        log.info("[통장] {} 입금 {}원 적요='{}' 거래번호={}", message.accountNo, message.amount, message.memo, message.reference)
    }

    private fun TransferRequest.reply(code: String) =
        TransferResponse(header.toResponse(code), bankCode, accountNo, amount, memo, reference)

    private inner class BankHandler : SimpleChannelInboundHandler<BankMessage>() {
        override fun channelRead0(ctx: ChannelHandlerContext, msg: BankMessage) {
            val reply = respond(msg) ?: return
            if (responseDelay.isZero) {
                ctx.writeAndFlush(reply)
            } else {
                ctx.executor().schedule({ ctx.writeAndFlush(reply) }, responseDelay.toMillis(), TimeUnit.MILLISECONDS)
            }
        }

        override fun exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) {
            log.warn("closing client channel: {}", cause.message)
            ctx.close()
        }
    }

    companion object {
        val DEFAULT_ACCOUNTS = mapOf(
            "1002123456789" to "홍길동",
            "3333012345678" to "김카뱅",
            "1101234567890" to "이페이",
            "9990000000002" to "무응답",
        )
        val DEFAULT_SILENT_AFTER_PROCESSING = setOf("9990000000002")
    }
}

fun main(args: Array<String>) {
    val port = args.firstOrNull()?.toIntOrNull() ?: 9090
    MockBankServer(port).start()
    println("Mock bank FEP started on port $port. Accounts: ${MockBankServer.DEFAULT_ACCOUNTS}")
    Thread.currentThread().join()
}
