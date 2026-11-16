package io.github.hyeokjinon.banktransfer.gatewayapp.leader

import org.springframework.context.SmartLifecycle
import java.time.Duration

/**
 * 종료 훅의 phase. 높은 phase 가 먼저 멈춘다 — 웹 서버 graceful 종료(DEFAULT_PHASE - 1024)보다 먼저 물러나,
 * 웹 서버가 아직 응답을 돌려줄 수 있을 때 처리 중 전문을 비운다 (P8-3b 결정 12).
 */
const val LEADERSHIP_PHASE: Int = SmartLifecycle.DEFAULT_PHASE

/** 선출 꺼짐 (IntelliJ · Compose · 테스트 기본): 기동 때 이미 리더(조립에서 startLeading). 종료 때만 거절 → 비우기 → 끊기. */
class AlwaysLeader(private val owner: BankConnectionOwner, private val drainTimeout: Duration) : SmartLifecycle {
    @Volatile private var running = false

    override fun start() {
        running = true
    }

    override fun stop() {
        owner.stepDown(drainTimeout)
        running = false
    }

    override fun isRunning(): Boolean = running

    override fun getPhase(): Int = LEADERSHIP_PHASE
}
