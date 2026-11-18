package io.github.hyeokjinon.banktransfer.api.tracing

/**
 * 우리가 붙이는 구간 속성 이름 (P9b 스펙 §5 — 계획의 "구간 이름 · 속성 표"). 테스트 · 스모크 · 실험이 같은 이름을 grep 한다.
 * 금지 (E9): 금액 · 계좌번호 · 예금주 이름 · 1원인증 코드 · Idempotency-Key · 사용자 id.
 */
object TraceKeys {
    const val TRANSFER_ID = "transfer.id"
    const val OUTBOX_EVENT_ID = "outbox.event_id"
    const val VERIFICATION_ID = "verification.id"
    const val TRANSFER_REFERENCE = "transfer.reference"
}
