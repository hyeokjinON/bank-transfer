package io.github.hyeokjinon.banktransfer.api.alert

import org.slf4j.LoggerFactory

enum class AlertLevel { INFO, WARN, CRITICAL }

/**
 * 운영자에게 보내는 알림 한 건 (P5-1). [lines] 는 요약, [items] 는 목록(불일치 건 등) — 목록은 [AlertText.MAX_ITEMS] 건까지만 싣고 "외 n건" 으로 줄인다.
 * 사용자 id · 예금주명은 넣지 않는다. 계좌번호는 보내기 전에 가운데를 가린다 ([AlertText.render]).
 */
data class Alert(
    val level: AlertLevel,
    val title: String,
    val lines: List<String> = emptyList(),
    val items: List<String> = emptyList(),
)

/**
 * 알림 포트 (P5-1). 구현은 예외를 던지지 않는다 — 알림 실패가 대사 · 메시지 처리를 실패시키거나 되돌리지 않는다.
 * 기록의 원본은 DB(recon_issue)와 로그다.
 */
fun interface AlertNotifier {
    fun notify(alert: Alert)
}

/** 구현이 약속을 어겨 던져도 부른 쪽(대사 잡 · 컨슈머 · 정리 작업)으로 번지지 않게 한 번 더 막는다. */
fun AlertNotifier.notifySafely(alert: Alert) {
    try {
        notify(alert)
    } catch (e: Exception) {
        LoggerFactory.getLogger(AlertNotifier::class.java).error("alert '{}' could not be delivered: {}", alert.title, e.javaClass.name)
    }
}

/** 알림 본문 만들기 (텔레그램 · 로그가 같은 글을 쓴다). */
object AlertText {
    const val MAX_ITEMS = 20

    /** 텔레그램 sendMessage 의 text 한도. */
    const val MAX_CHARS = 4096

    /** 계좌번호 모양(10~16자리 숫자 덩어리). 거래번호(T… 20자)는 영문으로 시작하는 한 낱말이라 낱말 경계가 없어 걸리지 않는다. */
    private val ACCOUNT_NO = Regex("""\b\d{10,16}\b""")

    fun render(alert: Alert): String {
        val shown = alert.items.take(MAX_ITEMS)
        val text = buildString {
            append('[').append(alert.level).append("] ").append(alert.title)
            alert.lines.forEach { append('\n').append(it) }
            shown.forEach { append("\n- ").append(it) }
            if (alert.items.size > shown.size) append("\n외 ").append(alert.items.size - shown.size).append("건")
        }
        // 만든 쪽이 가리는 것을 잊어도 나가기 전에 한 번 더 가린다 (텔레그램은 외부 서비스다)
        val masked = maskAccountNumbers(text)
        return if (masked.length <= MAX_CHARS) masked else masked.take(MAX_CHARS - 1) + "…"
    }

    fun maskAccountNumbers(text: String): String = ACCOUNT_NO.replace(text) { maskAccount(it.value) }

    /** 1002123456789 → 1002****6789. 가운데는 길이와 무관하게 `****` — 자릿수도 숨긴다. 8자리 이하는 모두 가린다. */
    fun maskAccount(accountNo: String): String =
        if (accountNo.length <= 8) "*".repeat(accountNo.length) else accountNo.take(4) + "****" + accountNo.takeLast(4)
}
