package io.github.hyeokjinon.banktransfer.api.dailyrecon

import io.github.hyeokjinon.banktransfer.api.alert.Alert
import io.github.hyeokjinon.banktransfer.api.alert.AlertLevel
import io.github.hyeokjinon.banktransfer.api.dailyrecon.DailyRecon.MATCH_BANK_LINES
import io.github.hyeokjinon.banktransfer.api.dailyrecon.DailyRecon.MATCH_TRANSFERS
import io.github.hyeokjinon.banktransfer.transfer.ReconIssueType
import io.github.hyeokjinon.banktransfer.transfer.ReconVerdict
import org.springframework.batch.core.job.JobInstance
import org.springframework.batch.core.repository.JobRepository
import java.time.LocalDate

/** 대사 한 번의 요약 (보고 · 관리 API 응답). */
data class ReconSummary(
    val reconDate: LocalDate,
    /** ③ 대상: D일(KST)에 은행에 보낸 출금. */
    val transfers: Int,
    /** 파일 D 의 1원인증(V 이면서 1원)이 아닌 줄 — 출금(T) · 낯선 거래번호 · 1원이 아닌 V. ④ 가 대조하는 범위와 같다. */
    val bankLines: Int,
    /** 파일 D 의 1원인증 줄(V 이면서 1원) — 대조하지 않고 건수만. */
    val verificationLines: Int,
    val matched: Int,
    val retrying: Int,
    val autoCompleted: Int,
    val autoFailed: Int,
    /** 사람이 봐야 하는 어긋남, 종류별 (recon_issue 기준 — 재시작해도 늘지 않는다). */
    val mismatches: Map<ReconIssueType, Int>,
    /** ③ 이 D일 송신으로 읽었지만 확정 전에 다시 보내져 sending_at 이 다른 날로 옮겨 간 출금 — 확정하지 않았다 (그 날짜의 대사가 맡는다). */
    val resentLater: Int = 0,
) {
    val mismatchCount: Int get() = mismatches.values.sum()
}

/**
 * 요약 만들기 (P5-1). 일치 · 재시도 중 · 대상 수는 ③ ④ 단계의 ExecutionContext(판정별 건수)에서, 자동 확정 · 불일치는 recon_issue 에서 센다 —
 * 재시작으로 단계가 여러 실행에 걸쳐도 마지막 단계 실행의 저장된 건수가 누적값이고, 이슈는 유니크라 늘지 않는다.
 */
class ReconSummaryReader(private val jobRepository: JobRepository, private val repo: ReconRepository) {

    fun read(instance: JobInstance, date: LocalDate): ReconSummary {
        val transfers = countsOf(instance, MATCH_TRANSFERS)
        val bankOnly = countsOf(instance, MATCH_BANK_LINES)
        val issues = repo.issueCounts(date)
        return ReconSummary(
            reconDate = date,
            transfers = transfers.total,
            bankLines = repo.countLines(date, verification = false),
            verificationLines = repo.countLines(date, verification = true),
            matched = transfers[ReconVerdict.MATCHED] + bankOnly[ReconVerdict.MATCHED],
            retrying = transfers[ReconVerdict.RETRYING] + bankOnly[ReconVerdict.RETRYING],
            autoCompleted = issues[ReconIssueType.AUTO_COMPLETED] ?: 0,
            autoFailed = issues[ReconIssueType.AUTO_FAILED] ?: 0,
            mismatches = issues.filterKeys { it.mismatch },
            resentLater = transfers[ReconVerdict.RESENT_LATER],
        )
    }

    /** 대사 완료 알림: 불일치가 없으면 INFO, 있으면 CRITICAL (요약 + 불일치 목록, 긴급 먼저). 자동 확정은 요약 건수로만. */
    fun completionAlert(summary: ReconSummary): Alert {
        val mismatched = repo.issuesOf(summary.reconDate).filter { it.type.mismatch }.sortedByDescending { it.type.urgent }
        val urgent = mismatched.count { it.type.urgent }
        val title = if (mismatched.isEmpty()) {
            "일마감 대사 ${summary.reconDate} 완료 — 불일치 없음"
        } else {
            "일마감 대사 ${summary.reconDate} 완료 — 불일치 ${mismatched.size}건" + if (urgent > 0) " (긴급 $urgent)" else ""
        }
        val lines = listOf(
            "출금 ${summary.transfers}건 (D일 송신): 일치 ${summary.matched} · 자동 완료 ${summary.autoCompleted} · " +
                "자동 실패(E002, 환불) ${summary.autoFailed} · 재시도 중 ${summary.retrying}" +
                if (summary.resentLater > 0) " · 다른 날 재송신(확정 안 함) ${summary.resentLater}" else "",
            "은행 줄 ${summary.bankLines}건 (1원인증 ${summary.verificationLines}건은 대조하지 않음)",
        ) + if (summary.mismatches.isEmpty()) emptyList() else listOf("불일치: " + summary.mismatches.entries.joinToString(" · ") { "${it.key} ${it.value}" })
        return Alert(
            level = if (mismatched.isEmpty()) AlertLevel.INFO else AlertLevel.CRITICAL,
            title = title,
            lines = lines,
            items = mismatched.map { it.alertLine() },
        )
    }

    private fun countsOf(instance: JobInstance, step: String): VerdictCounts =
        VerdictCounts.from(jobRepository.getLastStepExecution(instance, step)?.executionContext)
}

/** 알림 목록 한 줄. 사용자 id 는 없다 (ReconIssueRow 에 싣지 않는다). 계좌는 AlertText 가 가린다. 지갑 이슈는 이슈 id 로 가리킨다. */
internal fun ReconIssueRow.alertLine(): String =
    (if (type.urgent) "[긴급] " else "") + "$type ${reference ?: "지갑(issue #$id)"} 우리=${ourValue ?: "-"} 은행=${bankValue ?: "-"}"
