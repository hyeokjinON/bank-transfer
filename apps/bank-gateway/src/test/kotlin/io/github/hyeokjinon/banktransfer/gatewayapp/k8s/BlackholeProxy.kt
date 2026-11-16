package io.github.hyeokjinon.banktransfer.gatewayapp.k8s

import java.io.ByteArrayOutputStream
import java.net.ServerSocket
import java.net.Socket
import java.net.URI
import java.util.concurrent.CopyOnWriteArrayList

/**
 * 패킷 드롭 흉내 (P8-3b 결정 6): 평소엔 대상 서버로 바이트를 그대로 전달하다가 [blackhole] 이후엔 기존 · 새 연결 모두 전달을 멈춘다
 * (바이트는 붙들고 있을 뿐 응답이 없다). "연결 거절" 은 빨리 실패하지만 이쪽은 요청이 타임아웃까지 매달린다.
 * [heal] 은 TCP 재전송이 뒤늦게 통한 것처럼 붙들었던 바이트를 흘려보내고 이후 정상 전달한다 — 매달렸던 요청이 뒤늦게 성공할 수 있다.
 */
class BlackholeProxy(targetUrl: String) : AutoCloseable {
    private val target = URI(targetUrl).let { it.host to (if (it.port > 0) it.port else 80) }
    private val server = ServerSocket(0, 50, java.net.InetAddress.getLoopbackAddress())
    private val sockets = CopyOnWriteArrayList<Socket>()
    private val pumps = CopyOnWriteArrayList<Pump>()
    @Volatile private var dark = false
    @Volatile private var closed = false

    val url: String get() = "http://127.0.0.1:${server.localPort}"

    init {
        Thread({
            while (!closed) {
                val client = runCatching { server.accept() }.getOrNull() ?: break
                sockets += client
                val upstream = runCatching { Socket(target.first, target.second) }.getOrElse { runCatching { client.close() }; null } ?: continue
                sockets += upstream
                pumps += Pump(client, upstream).also { it.start() }
                pumps += Pump(upstream, client).also { it.start() }
            }
        }, "blackhole-proxy-accept").apply { isDaemon = true }.start()
    }

    fun blackhole() { dark = true }

    fun heal() {
        dark = false
        pumps.forEach { it.flush() }
    }

    private inner class Pump(private val from: Socket, private val to: Socket) {
        private val pending = ByteArrayOutputStream()

        fun start() {
            Thread({
                val buf = ByteArray(8192)
                runCatching {
                    while (true) {
                        val n = from.getInputStream().read(buf)
                        if (n < 0) break
                        synchronized(this) {
                            if (dark) pending.write(buf, 0, n) else write(buf, 0, n)
                        }
                    }
                }
            }, "blackhole-proxy-pump").apply { isDaemon = true }.start()
        }

        fun flush() = synchronized(this) {
            if (pending.size() > 0) {
                runCatching { write(pending.toByteArray(), 0, pending.size()) }
                pending.reset()
            }
        }

        private fun write(b: ByteArray, off: Int, len: Int) {
            to.getOutputStream().write(b, off, len)
            to.getOutputStream().flush()
        }
    }

    override fun close() {
        closed = true
        runCatching { server.close() }
        sockets.forEach { runCatching { it.close() } }
    }
}
