package io.github.hyeokjinon.banktransfer.api.config

import io.github.hyeokjinon.banktransfer.api.persistence.ConditionalWalletDebit
import io.github.hyeokjinon.banktransfer.api.persistence.WalletDebit
import io.github.hyeokjinon.banktransfer.api.support.ApiIntegrationTest
import org.springframework.beans.factory.annotation.Autowired
import kotlin.test.Test
import kotlin.test.assertIs
import kotlin.test.assertTrue

/** 실제 application.yml 로 뜬 앱의 전략 (P4-2). 측정 스크립트(load/run.sh)는 /actuator/info 로 띄운 전략을 확인한 뒤에만 잰다. */
class WalletDebitWiringTest : ApiIntegrationTest() {

    @Autowired private lateinit var debit: WalletDebit

    @Test
    fun `기본 설정이면 conditional 로 뜨고 actuator info 에 전략이 보인다`() {
        assertIs<ConditionalWalletDebit>(debit)

        val info = client.get().uri("/actuator/info").retrieve().body(String::class.java)!!

        assertTrue("\"debit-strategy\":\"conditional\"" in info, info)
    }
}
