package io.github.hyeokjinon.banktransfer.api.web

import io.github.hyeokjinon.banktransfer.api.persistence.WalletLedger
import jakarta.validation.Valid
import jakarta.validation.constraints.Max
import jakarta.validation.constraints.Min
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController

/** 1회 충전 한도 (원). 관리 · 테스트용 API 라 넉넉히 1억. */
const val MAX_DEPOSIT_AMOUNT: Long = 100_000_000

data class DepositRequest(
    @field:Min(1, message = "충전 금액은 1원 이상")
    @field:Max(MAX_DEPOSIT_AMOUNT, message = "충전 금액은 1억 원 이하")
    val amount: Long,
)

/** 잔고와, 결과가 정해지지 않아 잔고에서 빠진 채 묶인 금액 (RECEIVED · SENDING · UNKNOWN 출금의 합). */
data class WalletResponse(val userId: String, val balance: Long, val pending: Long)

/**
 * 사용자 원화 지갑 (P4-1, ADR-0008).
 * 충전은 **관리 · 테스트용**이다 — 실제로는 은행 입금 통지가 채우고 멱등 처리가 필요하다. 그래서 인증 · 멱등키가 없다 (범위 밖).
 */
@RestController
@RequestMapping("/api/v1/wallet")
class WalletController(private val wallets: WalletLedger) {

    @PostMapping("/deposits")
    fun deposit(user: RequestUser, @Valid @RequestBody body: DepositRequest): WalletResponse {
        val balance = wallets.deposit(user.id, body.amount)
        return WalletResponse(user.id, balance, wallets.pendingOf(user.id))
    }

    /** 지갑이 없으면 잔고 0. */
    @GetMapping
    fun get(user: RequestUser): WalletResponse =
        WalletResponse(user.id, wallets.balanceOf(user.id), wallets.pendingOf(user.id))
}
