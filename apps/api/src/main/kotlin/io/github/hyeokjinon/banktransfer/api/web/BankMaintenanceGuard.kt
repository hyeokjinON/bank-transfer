package io.github.hyeokjinon.banktransfer.api.web

import io.github.hyeokjinon.banktransfer.gateway.BankMaintenanceWindow
import java.time.Clock

/** 점검 시간에 은행을 건드리는 요청을 받지 않는다 (P5-1). [retryAfterSeconds] = 점검이 끝날 때까지 남은 초 (올림). */
class BankMaintenanceException(val retryAfterSeconds: Long) :
    RuntimeException("bank is under maintenance, retry after $retryAfterSeconds seconds")

/**
 * 은행을 건드리는 웹 요청(출금 접수, 1원인증 요청 · 확인)의 첫 줄에서 부른다 (P5-1). 점검 중이면 503 BANK_MAINTENANCE + Retry-After.
 * 조회 · 지갑 충전 · 관리 API 는 부르지 않는다. 웹을 거치지 않는 은행 호출(컨슈머 · 정리 작업)은 게이트웨이의 보내기 문이 막는다.
 */
class BankMaintenanceGuard(private val window: BankMaintenanceWindow, private val clock: Clock) {

    fun ensureOpen() {
        val now = clock.instant()
        if (window.isActive(now)) throw BankMaintenanceException(window.remainingSeconds(now))
    }
}
