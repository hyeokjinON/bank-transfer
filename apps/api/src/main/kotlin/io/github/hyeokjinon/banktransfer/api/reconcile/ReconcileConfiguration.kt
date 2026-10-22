package io.github.hyeokjinon.banktransfer.api.reconcile

import io.github.hyeokjinon.banktransfer.api.config.BankInquiryAdapter
import io.github.hyeokjinon.banktransfer.gateway.BankGatewayClient
import io.github.hyeokjinon.banktransfer.transfer.ReconcileResult
import io.github.hyeokjinon.banktransfer.transfer.TransferReconciler
import io.github.hyeokjinon.banktransfer.transfer.TransferStore
import org.slf4j.LoggerFactory
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.scheduling.annotation.Scheduled
import java.time.Clock
import java.time.Duration

@ConfigurationProperties("transfer.reconcile")
data class ReconcileProperties(
    val enabled: Boolean = true,
    val intervalMs: Long = 10_000,
    val sendingTimeout: Duration = Duration.ofSeconds(30),
    val maxInquiries: Int = 10,
    val batchSize: Int = 100,
    /** 마지막 변경 후 이만큼 지나야 재발행한다 (P3-3b). 재시도 토픽 체인(약 95초)이 끝난 뒤에 보도록. */
    val republishAfter: Duration = Duration.ofMinutes(5),
    /** 재발행 한도. 다 쓰고도 은행에 못 보내면 FAILED(E001). */
    val maxRepublish: Int = 3,
)

/** 정리 작업을 주기적으로 돌린다. 테스트는 이 빈을 끄고 TransferReconciler.reconcile() 을 직접 부른다. */
class TransferReconcilerScheduler(private val reconciler: TransferReconciler) {
    private val log = LoggerFactory.getLogger(javaClass)

    @Scheduled(fixedDelayString = "\${transfer.reconcile.interval-ms:10000}")
    fun run() {
        try {
            val result = reconciler.reconcile()
            if (result != NOTHING) log.info("reconcile: {}", result)
        } catch (e: Exception) {
            // DB 장애 등. 상태 변경은 모두 조건부라 다음 주기에 그대로 다시 하면 된다.
            log.warn("reconcile failed, retrying next round: {}", e.message)
        }
    }

    private companion object {
        val NOTHING = ReconcileResult(0, 0, 0, 0, 0, 0, 0)
    }
}

@Configuration(proxyBeanMethods = false)
class ReconcileConfiguration {
    private val log = LoggerFactory.getLogger(javaClass)

    @Bean
    fun transferReconciler(store: TransferStore, client: BankGatewayClient, props: ReconcileProperties) =
        TransferReconciler(
            store = store,
            bank = BankInquiryAdapter(client),
            clock = Clock.systemUTC(),
            sendingTimeout = props.sendingTimeout,
            maxInquiries = props.maxInquiries,
            batchSize = props.batchSize,
            republishAfter = props.republishAfter,
            maxRepublish = props.maxRepublish,
            // Transfer.toString 은 계좌번호를 가린다
            onGiveUp = { log.warn("still UNKNOWN after {} inquiries, leave it to daily reconciliation: {}", it.inquiryAttempts, it) },
            onAbandon = {
                log.warn("transfer abandoned after {} republishes, the bank was never reached (FAILED E001): {}", it.republishCount, it)
            },
        )

    @Bean
    @ConditionalOnProperty(prefix = "transfer.reconcile", name = ["enabled"], havingValue = "true", matchIfMissing = true)
    fun transferReconcilerScheduler(reconciler: TransferReconciler) = TransferReconcilerScheduler(reconciler)
}
