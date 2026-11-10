package io.github.hyeokjinon.banktransfer.gateway.http.fixture

import java.io.InputStream
import java.io.OutputStream
import java.net.InetSocketAddress
import java.net.ServerSocket
import java.net.Socket
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap

/**
 * 호출자와 게이트웨이 사이에 두는 TCP 프록시 (P8-1 테스트). 바이트를 그대로 옮기되:
 * - [Mode.DROP_RESPONSE]: 게이트웨이의 응답이 오기 시작하면 옮기지 않고 양쪽 연결을 끊는다 = "요청은 갔는데 응답 전에 끊김"
 * - [Mode.DELAY_RESPONSE]: 응답 조각마다 [responseDelay] 만큼 늦게 옮긴다
 * - [pause]: 듣기 소켓과 붙은 연결을 모두 닫는다 = "게이트웨이가 꺼짐"(연결 거부). [resume] 은 같은 포트로 다시 연다.
 * 연결마다 가상 스레드 두 개(양방향 복사). keep-alive 연결도 바이트 단위라 그대로 통과한다.
 */
class TcpCutProxy(private val upstreamPort: Int, private val upstreamHost: String = "127.0.0.1") : AutoCloseable {
    enum class Mode { PASS, DROP_RESPONSE, DELAY_RESPONSE }

    @Volatile var mode: Mode = Mode.PASS
    @Volatile var responseDelay: Duration = Duration.ZERO

    @Volatile private var server: ServerSocket? = null
    private val live = ConcurrentHashMap.newKeySet<Socket>()

    var port: Int = 0
        private set

    val baseUrl: String get() = "http://127.0.0.1:$port"

    fun start(): TcpCutProxy {
        bind(0)
        return this
    }

    fun pause() {
        server?.close()
        server = null
        live.forEach { runCatching { it.close() } }
        live.clear()
    }

    fun resume(): TcpCutProxy {
        if (server == null) bind(port)
        return this
    }

    override fun close() = pause()

    private fun bind(requested: Int) {
        val socket = ServerSocket().apply { reuseAddress = true; bind(InetSocketAddress("127.0.0.1", requested)) }
        server = socket
        port = socket.localPort
        Thread.ofVirtual().name("cut-proxy-accept").start {
            while (!socket.isClosed) {
                val client = runCatching { socket.accept() }.getOrNull() ?: break
                val upstream = runCatching { Socket(upstreamHost, upstreamPort) }.getOrElse { client.close(); continue }
                live += client; live += upstream
                Thread.ofVirtual().start { pipe(client.getInputStream(), upstream.getOutputStream(), client, upstream, response = false) }
                Thread.ofVirtual().start { pipe(upstream.getInputStream(), client.getOutputStream(), client, upstream, response = true) }
            }
        }
    }

    private fun pipe(from: InputStream, to: OutputStream, client: Socket, upstream: Socket, response: Boolean) {
        val buf = ByteArray(8192)
        try {
            while (true) {
                val n = from.read(buf)
                if (n < 0) break
                if (response && mode == Mode.DROP_RESPONSE) break
                if (response && mode == Mode.DELAY_RESPONSE) Thread.sleep(responseDelay.toMillis())
                to.write(buf, 0, n)
                to.flush()
            }
        } catch (_: Exception) {
            // 한쪽이 닫히면 끝
        } finally {
            runCatching { client.close() }; runCatching { upstream.close() }
            live -= client; live -= upstream
        }
    }
}
