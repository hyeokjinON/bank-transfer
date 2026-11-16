package io.github.hyeokjinon.banktransfer.gatewayapp.k8s

import java.net.ServerSocket
import java.net.Socket
import java.net.URI
import java.util.concurrent.CopyOnWriteArrayList

/**
 * 패킷 드롭 흉내 (P8-3b 결정 6): 평소엔 대상 서버로 바이트를 그대로 전달하다가 [blackhole] 이후엔 기존 연결도 전달을 멈추고
 * 새 연결은 받기만 하고 영영 응답하지 않는다. "연결 거절" 은 빨리 실패하지만 이쪽은 요청이 타임아웃까지 매달린다.
 */
class BlackholeProxy(targetUrl: String) : AutoCloseable {
    private val target = URI(targetUrl).let { it.host to (if (it.port > 0) it.port else 80) }
    private val server = ServerSocket(0, 50, java.net.InetAddress.getLoopbackAddress())
    private val sockets = CopyOnWriteArrayList<Socket>()
    @Volatile private var dark = false
    @Volatile private var closed = false

    val url: String get() = "http://127.0.0.1:${server.localPort}"

    init {
        Thread({
            while (!closed) {
                val client = runCatching { server.accept() }.getOrNull() ?: break
                sockets += client
                if (dark) { drain(client) } else { connect(client) }
            }
        }, "blackhole-proxy-accept").apply { isDaemon = true }.start()
    }

    fun blackhole() { dark = true }

    private fun connect(client: Socket) {
        val upstream = runCatching { Socket(target.first, target.second) }.getOrElse { runCatching { client.close() }; return }
        sockets += upstream
        pump(client, upstream)
        pump(upstream, client)
    }

    private fun pump(from: Socket, to: Socket) {
        Thread({
            val buf = ByteArray(8192)
            runCatching {
                while (true) {
                    val n = from.getInputStream().read(buf)
                    if (n < 0) break
                    if (!dark) { to.getOutputStream().write(buf, 0, n); to.getOutputStream().flush() }
                }
            }
        }, "blackhole-proxy-pump").apply { isDaemon = true }.start()
    }

    private fun drain(client: Socket) {
        Thread({ runCatching { val buf = ByteArray(8192); while (client.getInputStream().read(buf) >= 0) { } } }, "blackhole-proxy-drain").apply { isDaemon = true }.start()
    }

    override fun close() {
        closed = true
        runCatching { server.close() }
        sockets.forEach { runCatching { it.close() } }
    }
}
