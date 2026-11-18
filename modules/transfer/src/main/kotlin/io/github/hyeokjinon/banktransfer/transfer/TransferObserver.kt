package io.github.hyeokjinon.banktransfer.transfer

import java.time.Instant

/** 결과조회 한 건의 결과 (P9a 지표 `transfer.reconciled{result}`). 연결 없음(Unavailable)은 조회가 아니라 세지 않는다. */
enum class InquiryResolution { COMPLETED, FAILED, STILL_UNKNOWN }

/**
 * 출금 사건 알림 (P9a 지표). 도메인은 지표 라이브러리를 모른다 — apps/api 의 TransferMetrics 가 구현한다.
 * 부르는 시점: 저장 메서드(transition · fail · republish · recordInquiryAttempt)가 true 로 돌아온 뒤 = 그 저장의 트랜잭션이 커밋된 뒤
 * (부르는 쪽 — 컨슈머 · 정리 작업 — 은 트랜잭션 밖이다). 저장이 예외이거나 false(다른 쪽이 먼저 바꿈)면 부르지 않는다:
 * 롤백 · 낙관 락 재시도 · 중복 메시지에 두 번 세지 않게 (스펙 위험 A4). 카운터는 근사치다 — 돈의 정답은 DB (위험 B3).
 * 구현은 예외를 던지지 않는다 — 관찰은 돈 처리 흐름을 바꾸지 않는다 (호출자는 막지 않는다).
 * 기본 구현은 아무것도 하지 않는다 ([NONE]).
 */
interface TransferObserver {
    /** 컨슈머의 은행 호출 결과. 은행이 답한 사실이라 DB 기록과 무관하게 센다. */
    fun bankResult(outcome: BankTransferOutcome) {}

    /** COMPLETED · FAILED 로 확정했다. [createdAt] = 접수 시각, [at] = 확정 시각 (같은 시계). */
    fun finalized(status: TransferStatus, createdAt: Instant, at: Instant) {}

    /** 정리 작업이 아웃박스로 다시 발행했다. */
    fun republished() {}

    /** 재발행 한도를 다 쓰고 FAILED(E001) — 사용자가 다시 요청해야 한다. */
    fun abandoned() {}

    /** 결과조회를 한 번 했고 그 결과. */
    fun inquired(resolution: InquiryResolution) {}

    companion object {
        val NONE: TransferObserver = object : TransferObserver {}
    }
}
