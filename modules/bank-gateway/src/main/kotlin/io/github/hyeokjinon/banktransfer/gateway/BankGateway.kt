package io.github.hyeokjinon.banktransfer.gateway

import io.github.hyeokjinon.banktransfer.protocol.InquiryResponse
import io.github.hyeokjinon.banktransfer.protocol.NameInquiryResponse
import io.github.hyeokjinon.banktransfer.protocol.TransferResponse
import java.util.concurrent.CompletableFuture

/**
 * 은행 호출 세 가지 (P8-1). 처리 코드(어댑터 · 처리기 · 정리 작업 · 리액티브 흐름)는 이것만 본다.
 * 구현: [BankGatewayClient] (프로세스 안에서 Netty 로 FEP 에 직결, mode=direct) · HTTP 클라이언트 (게이트웨이 앱을 부름, mode=http — modules/bank-gateway-http).
 *
 * 실패는 future 안에 담긴다. 구현이 달라도 뜻은 같아야 한다 — 처리 코드가 이 셋으로 "다시 보내도 되나" 를 정한다:
 * - [BankNotSentException]: 은행에 확실히 가지 않았다 (다시 보내도 안전)
 * - [BankTimeoutException]: 보냈는데 응답이 없다 (모름 → 결과조회)
 * - [BankUnavailableException] 과 그 밖: 보냈을 수 있다 (모름)
 * 은행의 거절(응답코드 0000 이 아님)은 실패가 아니라 정상 응답이다.
 */
interface BankGateway {
    fun inquireName(bankCode: String, accountNo: String): CompletableFuture<NameInquiryResponse>

    fun transfer(bankCode: String, accountNo: String, amount: Long, memo: String, reference: String): CompletableFuture<TransferResponse>

    /** 결과조회 (v0.2). 결과를 모르는 이체를 이용기관 거래번호로 묻는다 — 이체를 다시 보내지 않는다. */
    fun inquire(reference: String): CompletableFuture<InquiryResponse>
}
