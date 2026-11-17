package io.github.hyeokjinon.banktransfer.gateway

import io.github.hyeokjinon.banktransfer.protocol.Header
import java.util.concurrent.atomic.AtomicLong

/**
 * 전문번호 발급 (P8-3b, 규격 v0.4: 기관 · 일자별 유일). 유일성의 범위는 구현이 정한다 —
 * [InMemoryTranNoSource] 는 이 인스턴스 안, K8s 의 ConfigMap 구간 예약(apps/bank-gateway)은 게이트웨이 여러 대 사이.
 * 통신 모듈은 이 포트만 안다 (ADR-0001 포트/어댑터 — K8s 를 모른다).
 */
fun interface TranNoSource {
    /** 다음 번호. 줄 수 없으면 [BankNotSentException] — 아직 은행에 보내지 않았으므로 다시 보내도 안전하다. 막히지 않아야 한다 (이벤트 루프에서 부른다). */
    fun next(): Long

    /** 리더 임기 시작 때 한 번 (구간을 미리 예약). 실패해도 던지지 않는다 — next() 가 다시 시도한다. */
    fun prepare() {}

    /** 예약해 둔 남은 번호 (P9a 지표). 개념이 없는 출처(메모리 카운터)는 null → Gauge 는 NaN. */
    fun remaining(): Long? = null
}

/** 메모리 카운터 (P8-3a 까지의 동작). 선출이 꺼진 환경(IntelliJ · Compose · 테스트)의 기본값. 다 쓰면 1부터. */
class InMemoryTranNoSource : TranNoSource {
    private val sequence = AtomicLong(0)

    override fun next(): Long = sequence.updateAndGet { if (it >= Header.MAX_TRAN_NO) 1 else it + 1 }
}
