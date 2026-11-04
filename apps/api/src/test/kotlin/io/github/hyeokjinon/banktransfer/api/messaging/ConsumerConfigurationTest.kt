package io.github.hyeokjinon.banktransfer.api.messaging

import io.github.hyeokjinon.banktransfer.api.alert.AlertNotifier
import io.github.hyeokjinon.banktransfer.gateway.BankGatewayClient
import io.github.hyeokjinon.banktransfer.gateway.BankGatewayConfig
import io.github.hyeokjinon.banktransfer.transfer.BankTransferPort
import io.github.hyeokjinon.banktransfer.transfer.TransferProcessor
import io.github.hyeokjinon.banktransfer.transfer.TransferStore
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.springframework.boot.test.context.runner.ApplicationContextRunner
import org.springframework.context.ApplicationContext
import java.lang.reflect.Proxy
import java.util.function.Supplier
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull

/**
 * 컨슈머 모드 설정 (P6-1). 같은 토픽 · 같은 그룹이라 두 리스너가 함께 뜨면 메시지를 나눠 가진다 — 모드마다 정확히 하나.
 * 모드 값은 소문자 그대로만: 느슨한 enum 바인딩과 @ConditionalOnProperty 비교가 어긋나 리스너 없이 조용히 뜨는 일을 막는다.
 * 빈 선택만 보므로 저장소 · 게이트웨이는 쓰지 않는 객체를 넣는다 (게이트웨이는 start() 하지 않아 은행에 붙지 않는다).
 */
class ConsumerConfigurationTest {

    private val unusedStore = Proxy.newProxyInstance(TransferStore::class.java.classLoader, arrayOf(TransferStore::class.java)) { _, method, _ ->
        error("설정 테스트는 저장소를 부르지 않는다: ${method.name}")
    } as TransferStore

    private val runner = ApplicationContextRunner()
        .withUserConfiguration(ConsumerConfiguration::class.java)
        .withBean(TransferProcessor::class.java, Supplier { TransferProcessor(unusedStore, BankTransferPort { _, _, _, _, _ -> error("not used") }) })
        .withBean(BankGatewayClient::class.java, Supplier { BankGatewayClient(BankGatewayConfig(host = "localhost", port = 1)) })
        .withBean(AlertNotifier::class.java, Supplier { AlertNotifier { } })

    private fun ApplicationContext.listenerCounts() =
        getBeanNamesForType(TransferRequestedListener::class.java).size to getBeanNamesForType(SuspendingTransferRequestedListener::class.java).size

    @Test
    fun `설정이 없으면 blocking - 블로킹 리스너 하나만, 상한 기본 64`() {
        runner.run { ctx ->
            assertNull(ctx.startupFailure)
            assertEquals(1 to 0, ctx.listenerCounts())
            val props = ctx.getBean(TransferConsumerProperties::class.java)
            assertEquals(TransferConsumerProperties.BLOCKING, props.mode)
            assertEquals(64, props.coroutine.maxInFlightBankCalls)
        }
    }

    @Test
    fun `coroutine 이면 코루틴 리스너 하나만`() {
        runner.withPropertyValues("transfer.consumer.mode=coroutine").run { ctx ->
            assertNull(ctx.startupFailure)
            assertEquals(0 to 1, ctx.listenerCounts())
        }
    }

    @ParameterizedTest(name = "mode={0}")
    @ValueSource(strings = ["Coroutine", "COROUTINE", "co-routine", "virtual", ""])
    fun `모르는 모드 · 대소문자가 다른 모드는 기동 실패 - 리스너 없이 조용히 뜨지 않게`(value: String) {
        runner.withPropertyValues("transfer.consumer.mode=$value").run { ctx ->
            assertNotNull(ctx.startupFailure)
        }
    }

    @ParameterizedTest(name = "max-in-flight-bank-calls={0}")
    @ValueSource(ints = [0, -1, 1025])
    fun `상한이 1 ~ 1024 밖이면 기동 실패 (모드와 무관)`(value: Int) {
        runner.withPropertyValues("transfer.consumer.coroutine.max-in-flight-bank-calls=$value").run { ctx ->
            assertNotNull(ctx.startupFailure)
        }
    }

    @ParameterizedTest(name = "max-in-flight-bank-calls={0}")
    @ValueSource(ints = [1, 1024])
    fun `상한 경계 1 · 1024 는 받는다`(value: Int) {
        runner.withPropertyValues("transfer.consumer.mode=coroutine", "transfer.consumer.coroutine.max-in-flight-bank-calls=$value").run { ctx ->
            assertNull(ctx.startupFailure)
            assertEquals(value, ctx.getBean(TransferConsumerProperties::class.java).coroutine.maxInFlightBankCalls)
        }
    }
}
