package io.github.hyeokjinon.banktransfer.api.dailyrecon

import java.time.LocalDate
import kotlin.random.Random

/**
 * 공유 DB 의 대사 테스트는 테스트마다 다른 대사일을 쓴다 (설계 §10): 잡 인스턴스 · 이슈 · 적재 줄이 날짜로 갈리고,
 * ③ 은 그날(KST) 보낸 출금만 본다. 진짜 날짜와 겹치지 않게 먼 미래(2100 ~ 7575년)에서 고른다. 앞뒤 하루도 쓸 수 있게 여유를 둔다.
 */
object ReconTestDates {
    fun unique(): LocalDate = LocalDate.of(2100, 1, 1).plusDays(Random.nextLong(0, 2_000_000))
}
