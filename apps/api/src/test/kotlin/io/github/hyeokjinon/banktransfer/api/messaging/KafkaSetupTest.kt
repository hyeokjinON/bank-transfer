package io.github.hyeokjinon.banktransfer.api.messaging

import io.github.hyeokjinon.banktransfer.api.support.ApiIntegrationTest
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.kafka.core.KafkaAdmin
import org.springframework.kafka.core.KafkaTemplate
import kotlin.test.Test
import kotlin.test.assertEquals

class KafkaSetupTest : ApiIntegrationTest() {

    @Autowired private lateinit var kafka: KafkaTemplate<String, String>
    @Autowired private lateinit var admin: KafkaAdmin

    @Test
    fun `프로듀서는 acks=all, 멱등 프로듀서로 설정된다 (설정이 실제로 적용됐는지)`() {
        val config = kafka.producerFactory.configurationProperties

        assertEquals("all", config["acks"].toString())
        assertEquals("true", config["enable.idempotence"].toString())
    }

    @Test
    fun `transfer_requested 토픽이 파티션 3개로 만들어진다`() {
        val description = admin.describeTopics(TransferTopics.REQUESTED).getValue(TransferTopics.REQUESTED)

        assertEquals(3, description.partitions().size)
    }
}
