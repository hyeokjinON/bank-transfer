package io.github.hyeokjinon.banktransfer.api.support

import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.core.KafkaAdmin
import org.springframework.kafka.support.KafkaHeaders
import java.time.Duration
import java.util.UUID
import kotlin.test.assertTrue

/** 토픽에 지금까지 쌓인 레코드를 처음부터 끝까지 읽는다. assign 으로 읽어 그룹에 참여하지 않고 오프셋도 커밋하지 않는다. */
fun ConsumerFactory<String, String>.readAll(topic: String): List<ConsumerRecord<String, String>> =
    createConsumer("topic-reader-${UUID.randomUUID()}", null).use { consumer ->
        val partitions = consumer.partitionsFor(topic, Duration.ofSeconds(5)).map { TopicPartition(topic, it.partition()) }
        consumer.assign(partitions)
        consumer.seekToBeginning(partitions)
        val end = consumer.endOffsets(partitions)
        val records = mutableListOf<ConsumerRecord<String, String>>()
        val deadline = System.currentTimeMillis() + 10_000
        while (partitions.any { consumer.position(it) < end.getValue(it) }) {
            check(System.currentTimeMillis() < deadline) { "could not read $topic to the end" }
            records += consumer.poll(Duration.ofMillis(200))
        }
        records
    }

/** [topic] 에 key 가 [key] 인 레코드가 [count] 개 이상 쌓일 때까지 기다려 돌려준다. */
fun ConsumerFactory<String, String>.awaitRecords(
    topic: String,
    key: String,
    count: Int = 1,
    timeout: Duration = Duration.ofSeconds(30),
): List<ConsumerRecord<String, String>> {
    var found = emptyList<ConsumerRecord<String, String>>()
    await.atMost(timeout).pollInterval(Duration.ofMillis(200)) untilAsserted {
        found = readAll(topic).filter { it.key() == key }
        assertTrue(found.size >= count, "$topic 에 key=$key 가 ${found.size}개 (기다리는 수 $count)")
    }
    return found
}

/** 그룹 [group] 이 [partition] 에 커밋한 오프셋 (= 다음에 읽을 위치). 커밋이 없으면 null. */
fun KafkaAdmin.committedOffset(group: String, partition: TopicPartition): Long? =
    AdminClient.create(configurationProperties).use { admin ->
        admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get()[partition]?.offset()
    }

/** DLT · 재시도 레코드에 붙은 예외 클래스 이름들 (리스너 예외와 그 원인). */
fun ConsumerRecord<String, String>.exceptionClassNames(): List<String> =
    listOf(KafkaHeaders.EXCEPTION_FQCN, KafkaHeaders.EXCEPTION_CAUSE_FQCN)
        .mapNotNull { headers().lastHeader(it)?.value()?.toString(Charsets.UTF_8) }
