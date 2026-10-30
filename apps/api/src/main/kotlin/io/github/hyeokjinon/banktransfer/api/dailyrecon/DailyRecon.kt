package io.github.hyeokjinon.banktransfer.api.dailyrecon

import io.github.hyeokjinon.banktransfer.protocol.ReconFileFormat
import org.springframework.batch.core.job.parameters.JobParameters
import org.springframework.batch.core.job.parameters.JobParametersBuilder
import org.springframework.boot.context.properties.ConfigurationProperties
import java.nio.file.Path
import java.time.Instant
import java.time.LocalDate
import java.time.ZoneId

/** 일마감 대사의 이름 · 상수 (P5-1). 운영 문서 · 테스트가 이름을 여기서 본다. 10초 주기 "정리 작업"(reconcile)과 구분해 dailyrecon. */
object DailyRecon {
    const val JOB_NAME = "dailyReconciliationJob"
    const val RECON_DATE = "reconDate"

    const val VALIDATE_FILE = "validateFile"
    const val LOAD_STATEMENT = "loadStatement"
    const val MATCH_TRANSFERS = "matchTransfers"
    const val MATCH_BANK_LINES = "matchBankLines"
    const val CHECK_LEDGER = "checkLedger"
    const val REPORT = "report"

    /** ② 적재 청크 (설계 §5-1). ③ ④ 는 daily-recon.chunk-size (기본 100). */
    const val LOAD_CHUNK_SIZE = 500

    /** ① 의 실패 이유 — 실행 결과 · 관리 API 응답에 그대로 나간다. */
    const val FILE_NOT_READY = "FILE_NOT_READY"
    const val FILE_INVALID = "FILE_INVALID"

    /** 대사일 · 은행 파일의 날짜는 KST 다. */
    val KST: ZoneId = ZoneId.of("Asia/Seoul")

    /** 날짜마다 잡 인스턴스 하나 (식별 파라미터 reconDate). */
    fun parameters(date: LocalDate): JobParameters = JobParametersBuilder().addLocalDate(RECON_DATE, date).toJobParameters()

    /** 대사일 D 의 KST 하루 [시작, 끝). 점검 시간에는 아무것도 은행에 가지 않으므로 이 범위에 보낸 출금 = 파일 D 의 범위다. */
    fun dayRange(date: LocalDate): Pair<Instant, Instant> =
        date.atStartOfDay(KST).toInstant() to date.plusDays(1).atStartOfDay(KST).toInstant()

    /**
     * 수동 대사가 받는 날짜: 마감된 날(어제 이전, KST)만. 오늘을 대사하면 방금 보내 아직 은행 파일에 없는 출금을 "은행이 처리하지 않았다" 로
     * 보고 AUTO_FAIL(E002 + 환불)하는데, 은행은 그 출금을 아직 처리할 수 있다 → 이중 지급 (ADR-0010).
     * [allowToday] = daily-recon.allow-today, 로컬 손 확인용(실행 구성 6) — 오늘도 받는다. 내일 이후는 언제나 거부. 스케줄은 늘 전날이라 무관하다.
     */
    fun requireClosed(date: LocalDate, now: Instant, allowToday: Boolean) {
        val today = LocalDate.ofInstant(now, KST)
        val lastAllowed = if (allowToday) today else today.minusDays(1)
        if (date.isAfter(lastAllowed)) throw ReconDateNotClosedException(date, today)
    }
}

/** 일마감 대사 설정 (P5-1, 설계 §9). */
@ConfigurationProperties("daily-recon")
data class DailyReconProperties(
    /** 은행 파일 디렉터리 (`recon/RECON_yyyyMMdd.dat`). 실제로는 은행 SFTP 에서 받아 오는 자리. 상대 경로는 작업 디렉터리(저장소 루트) 기준. */
    val fileDir: String = "./bank-files",
    /** ③ · ④ 청크 크기. ② 적재는 500 고정. */
    val chunkSize: Int = 100,
    /** 수동 대사에 오늘(KST)을 허용 — 로컬 손 확인 전용(실행 구성 6). 운영에서 켜면 이중 지급 위험 ([DailyRecon.requireClosed]). */
    val allowToday: Boolean = false,
    val schedule: Schedule = Schedule(),
) {
    data class Schedule(
        val enabled: Boolean = true,
        /** 01:00 ~ 06:00 매시 정각. 파일이 아직 없으면 다음 정각에 다시, 마지막 회차(다음 실행이 다음 날)까지 못 끝내면 긴급 알림. */
        val cron: String = "0 0 1-6 * * *",
        val zone: String = "Asia/Seoul",
    )

    fun reconFile(date: LocalDate): Path = Path.of(fileDir, "recon", ReconFileFormat.fileName(date))
}
