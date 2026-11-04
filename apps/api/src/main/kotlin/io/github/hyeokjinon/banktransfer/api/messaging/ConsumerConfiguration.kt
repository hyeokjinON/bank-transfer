package io.github.hyeokjinon.banktransfer.api.messaging

import io.github.hyeokjinon.banktransfer.api.alert.AlertNotifier
import io.github.hyeokjinon.banktransfer.api.config.SuspendBankTransferAdapter
import io.github.hyeokjinon.banktransfer.gateway.BankGatewayClient
import io.github.hyeokjinon.banktransfer.transfer.TransferProcessor
import kotlinx.coroutines.sync.Semaphore
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

/**
 * 출금 컨슈머 설정 (P6-1, ADR-0012). 범위를 벗어나면 기동에 실패한다.
 * [mode] 는 소문자 그대로만 받는다 (wallet.debit-strategy 의 enum 과 다르다): 리스너 빈은 @ConditionalOnProperty 가 고르는데,
 * enum 으로 받으면 바인딩은 느슨해("co-routine" 도 COROUTINE) 두 리스너가 모두 빈이 되지 않은 채 — 컨슈머 없이 — 조용히 뜰 수 있다.
 */
@ConfigurationProperties("transfer.consumer")
data class TransferConsumerProperties(
    /** blocking(기본) | coroutine. */
    val mode: String = BLOCKING,
    val coroutine: Coroutine = Coroutine(),
) {
    init {
        require(mode in MODES) { "transfer.consumer.mode must be one of $MODES (lower case): '$mode'" }
    }

    data class Coroutine(
        /** 코루틴 모드에서 은행에 동시에 보내는 최대 수. 처리 중 메시지 수(최대 3 × 50)와 별개로 은행 쪽을 지킨다. */
        val maxInFlightBankCalls: Int = 64,
    ) {
        init {
            require(maxInFlightBankCalls in 1..MAX_IN_FLIGHT) {
                "transfer.consumer.coroutine.max-in-flight-bank-calls must be within 1..$MAX_IN_FLIGHT: $maxInFlightBankCalls"
            }
        }
    }

    companion object {
        const val BLOCKING = "blocking"
        const val COROUTINE = "coroutine"
        val MODES = listOf(BLOCKING, COROUTINE)
        const val MAX_IN_FLIGHT = 1024
    }
}

/**
 * 컨슈머 조립 (P6-1). transfer.consumer.mode 하나로 두 리스너 중 하나만 빈이 된다 — 같은 토픽 · 같은 그룹이라 둘 다 뜨면 메시지를 나눠 가진다.
 * 판정(TransferProcessor)은 두 모드가 같이 쓴다 (MessagingConfiguration). 측정 · 비교용으로 고르는 것이고 기본은 블로킹이다.
 * (TransferConsumerProperties 는 @ConfigurationPropertiesScan 도 등록하지만, 이 클래스만 떼어 쓰는 테스트를 위해 여기서도 켠다 — 같은 이름이라 한 번만 등록된다.)
 */
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(TransferConsumerProperties::class)
class ConsumerConfiguration {

    @Bean
    @ConditionalOnProperty(prefix = "transfer.consumer", name = ["mode"], havingValue = TransferConsumerProperties.BLOCKING, matchIfMissing = true)
    fun transferRequestedListener(processor: TransferProcessor, alerts: AlertNotifier) = TransferRequestedListener(alerts, processor::process)

    /** 어댑터 · 세마포어는 빈으로 올리지 않는다: 원래 토픽 · 재시도 토픽 컨테이너가 이 리스너 빈 하나 = 같은 상한을 쓴다. */
    @Bean
    @ConditionalOnProperty(prefix = "transfer.consumer", name = ["mode"], havingValue = TransferConsumerProperties.COROUTINE)
    fun suspendingTransferRequestedListener(
        processor: TransferProcessor,
        client: BankGatewayClient,
        alerts: AlertNotifier,
        props: TransferConsumerProperties,
    ) = SuspendingTransferRequestedListener(
        processor,
        SuspendBankTransferAdapter(client),
        Semaphore(props.coroutine.maxInFlightBankCalls),
        alerts = alerts,
    )
}
