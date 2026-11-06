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
import java.nio.file.Path
import java.time.Clock
import java.time.Duration
import java.time.LocalDate
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
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
 * - responseDelay (P6-1): 모든 응답을 이만큼 늦춘다 (띄운 뒤에도 바꿀 수 있다). 그동안 "동시에 응답을 기다리는 요청 수" 의 최댓값을 센다 — 테스트는 [peakPendingResponses], 측정은 로그
 * - bankFiles (P5-1): 처리한 이체를 일자별 거래 기록에 남기고, 기동할 때 처리한 거래번호를 되살리고, 매일 00:05(KST)에 전날 대사 파일을 만든다 ([BankJournal]).
 *   null(테스트 기본값)이면 지금처럼 메모리만 쓴다
 */
class MockBankServer(
    private val port: Int = 0,
    private val accounts: Map<String, String> = DEFAULT_ACCOUNTS,
    private val silentAccounts: Set<String> = emptySet(),
    private val silentAfterProcessingAccounts: Set<String> = DEFAULT_SILENT_AFTER_PROCESSING,
    responseDelay: Duration = Duration.ZERO,
    /** 일자별 거래 기록 · 대사 파일 디렉터리 (P5-1). null 이면 기록하지 않는다. */
    bankFiles: Path? = null,
    private val clock: Clock = Clock.systemUTC(),
) : AutoCloseable {

    private val log = LoggerFactory.getLogger(javaClass)
    private val boss = MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory())
    private val workers = MultiThreadIoEventLoopGroup(2, NioIoHandler.newFactory())
    /**
     * 계좌별 입금 기록 (테스트가 적요의 코드를 꺼내 본다). 받은 순서대로 쌓는다.
     * ConcurrentLinkedQueue: 추가가 O(1) — CopyOnWriteArrayList 는 입금마다 목록 전체를 복사해, 측정처럼 한 계좌에 수만 건이 오면
     * 모의 은행 이벤트 루프가 그 복사에 시간을 쓴다 (P6-2 계획 "스펙과 다른 점" 7).
     */
    private val deposits = ConcurrentHashMap<String, ConcurrentLinkedQueue<TransferRequest>>()
    private val processedReferences: MutableSet<String> = ConcurrentHashMap.newKeySet()
    private val pollCounter = AtomicInteger()

    /** 응답 지연 (P6-1: 측정 · 테스트가 바꾼다). 범위 검사는 실행 인자(MockBankOptions)가 한다. */
    @Volatile
    var responseDelay: Duration = responseDelay

    /** 응답을 쓰려고 기다리는 요청 수와 그 최댓값 (폴링은 세지 않는다). 은행 쪽에서 본 "동시에 붙잡힌 출금 수" 다. */
    private val pendingResponses = AtomicInteger()
    private val peakPending = AtomicInteger()
    private lateinit var serverChannel: Channel
    private val clients = DefaultChannelGroup(GlobalEventExecutor.INSTANCE)
    private val journal: BankJournal? = bankFiles?.let { BankJournal(it, clock) }

    /** 00:05 일마감 예약 (기록할 때만). 데몬 스레드라 프로세스 종료를 막지 않는다. */
    private val closer: ScheduledExecutorService? = journal?.let {
        Executors.newSingleThreadScheduledExecutor { task -> Thread(task, "mock-bank-daily-close").apply { isDaemon = true } }
    }

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
        journal?.let {
            // 재기동해도 결과조회가 처리 기록대로 답한다 — 메모리의 처리 목록만 쓰면 재기동 뒤 모든 조회가 2001 이 된다
            processedReferences += it.processedReferences()
            log.info("mock bank restored {} processed references from the journal", processedReferences.size)
        }
        serverChannel = bootstrap.bind(listenPort).sync().channel()
        listenPort = (serverChannel.localAddress() as InetSocketAddress).port
        log.info("mock bank listening on {}", listenPort)
        scheduleDailyClose()
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

    /** 지금까지(또는 마지막 [resetPeakPendingResponses] 뒤) 동시에 응답을 기다린 요청 수의 최댓값 (P6-1). */
    fun peakPendingResponses(): Int = peakPending.get()

    /** 최댓값을 지금 기다리는 수로 되돌린다 (공유 테스트 은행에서 한 테스트의 값만 보려고). */
    fun resetPeakPendingResponses() = peakPending.set(pendingResponses.get())

    private fun scheduleDailyClose() {
        val executor = closer ?: return
        executor.schedule({
            closeYesterday()
            scheduleDailyClose()
        }, delayUntilNextClose(clock.instant()).toMillis(), TimeUnit.MILLISECONDS)
    }

    /** 일마감: 전날(KST) 기록으로 대사 파일을 만든다. 실패해도 서버는 계속 돈다 — 손으로 --close-day 로 다시 만든다. */
    private fun closeYesterday() {
        val yesterday = LocalDate.ofInstant(clock.instant(), KST).minusDays(1)
        try {
            log.info("[일마감] {} 대사 파일: {}", yesterday, journal!!.closeDay(yesterday))
        } catch (e: Exception) {
            log.error("[일마감] {} 대사 파일을 만들지 못함 — --close-day 로 다시 만든다", yesterday, e)
        }
    }

    override fun close() {
        log.info("mock bank peak pending responses: {} (closing)", peakPending.get())
        closer?.shutdownNow()
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
        // 메모리보다 먼저 — "처리함" 의 기준이 기록이다. 쓰기가 실패하면 예외 → 채널이 닫혀 응답이 없다 = 결과를 모름
        journal?.record(message)
        deposits.computeIfAbsent(message.accountNo) { ConcurrentLinkedQueue() }.add(message)
        processedReferences += message.reference
        log.info("[통장] {} 입금 {}원 적요='{}' 거래번호={}", message.accountNo, message.amount, message.memo, message.reference)
    }

    private fun TransferRequest.reply(code: String) =
        TransferResponse(header.toResponse(code), bankCode, accountNo, amount, memo, reference)

    private inner class BankHandler : SimpleChannelInboundHandler<BankMessage>() {
        override fun channelRead0(ctx: ChannelHandlerContext, msg: BankMessage) {
            val reply = respond(msg) ?: return
            val counted = msg !is PollRequest   // 회선 확인은 출금이 아니다
            if (counted) markPending()
            val send = Runnable {
                if (counted) pendingResponses.decrementAndGet()
                ctx.writeAndFlush(reply)
            }
            val delay = responseDelay
            if (delay.isZero) send.run() else ctx.executor().schedule(send, delay.toMillis(), TimeUnit.MILLISECONDS)
        }

        /**
         * 최댓값이 바뀔 때마다 한 줄 남긴다 (값이 늘 때만이라 줄 수는 최댓값 이하다).
         * 측정 스크립트가 Windows 에서 프로세스를 내리면 종료 로그(close)가 남지 않을 수 있어 이것에 기댄다.
         */
        private fun markPending() {
            val now = pendingResponses.incrementAndGet()
            if (now > peakPending.getAndAccumulate(now) { a, b -> maxOf(a, b) }) log.info("mock bank peak pending responses: {}", now)
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

        /** 모의 은행의 은행코드. 대사 파일 헤더에 쓴다 (예시 · 테스트가 모두 이 코드로 보낸다). */
        const val BANK_CODE = "090"
    }
}

fun main(args: Array<String>) {
    val options = MockBankOptions.parse(args)
    options.closeDay?.let { day ->
        // 서버를 띄우지 않고 그 날짜 파일만 만든다 (손 확인 · 재생성용). 이미 있으면 그대로 둔다
        println("Recon file for $day: ${BankJournal(options.bankFiles).closeDay(day).toAbsolutePath()}")
        return
    }
    val server = MockBankServer(options.port, responseDelay = options.responseDelay, bankFiles = options.bankFiles).start()
    // 정상 종료(Ctrl+C) 때 동시 대기 최댓값을 남긴다 (측정용). 강제 종료면 남지 않는다 — 최댓값이 바뀔 때마다 남기는 줄이 대신한다
    Runtime.getRuntime().addShutdownHook(Thread { server.close() })
    println(
        "Mock bank FEP started on port ${options.port}. Accounts: ${MockBankServer.DEFAULT_ACCOUNTS}. " +
            "Bank files: ${options.bankFiles.toAbsolutePath()}. Response delay: ${options.responseDelay.toMillis()}ms",
    )
    Thread.currentThread().join()
}
