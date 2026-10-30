package io.github.hyeokjinon.banktransfer.mockbank

import io.github.hyeokjinon.banktransfer.protocol.ReconDetail
import io.github.hyeokjinon.banktransfer.protocol.ReconFileFormat
import io.github.hyeokjinon.banktransfer.protocol.TransferRequest
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.nio.file.StandardOpenOption
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.LocalDate
import java.time.LocalTime
import java.time.ZoneId

/** 은행의 날짜는 KST 다. 시계(Instant)는 시간대를 모르므로 날짜 · 시각은 늘 이 시간대로 읽는다. */
internal val KST: ZoneId = ZoneId.of("Asia/Seoul")

/**
 * 일마감 시각: 매일 00:05 (KST) 에 전날 기록을 닫는다.
 * API 의 은행 점검 시간(23:50 ~ 00:10)에는 아무 이체도 오지 않으므로 이때 닫아도 전날 기록이 더 늘지 않는다.
 */
internal val DAILY_CLOSE_AT: LocalTime = LocalTime.of(0, 5)

/**
 * 모의 은행의 일자별 거래 기록과 일마감 (P5-1). 실제 은행의 원장 · 대사 파일 생성을 흉내 낸다.
 * - [record]: 이체를 처리할 때마다 `{dir}/journal/yyyyMMdd.log` 에 상세 레코드 한 줄 (날짜 = 처리 시각의 KST 날짜)
 * - [processedReferences]: 기동할 때 기록에서 처리한 거래번호를 되살린다 → 재기동해도 결과조회가 맞게 답한다
 * - [closeDay]: 그날 기록으로 `{dir}/recon/RECON_yyyyMMdd.dat` 를 만든다. 임시 파일에 쓰고 같은 디렉터리 안에서 원자적으로 이름을 바꾼다 —
 *   API 가 반쯤 쓴 파일을 읽지 않게. 이미 있으면 덮어쓰지 않는다 (은행이 한 번 보낸 파일은 바뀌지 않는다. 다시 만들려면 지우고 --close-day).
 * 디렉터리는 저장소 루트의 `./bank-files` (git 제외). 실제로는 은행 SFTP 에서 받아 오는 자리다.
 */
class BankJournal(
    dir: Path,
    private val clock: Clock = Clock.systemUTC(),
    private val bankCode: String = MockBankServer.BANK_CODE,
) {
    private val journalDir: Path = dir.resolve("journal")
    private val reconDir: Path = dir.resolve("recon")

    /** 처리한 이체 한 건을 그날 기록에 덧붙인다. 같은 프로세스의 [closeDay] 와 겹치지 않게 잠근다. */
    @Synchronized
    fun record(request: TransferRequest) {
        val now = clock.instant().atZone(KST)
        val detail = ReconDetail(request.reference, request.bankCode, request.accountNo, request.amount, now.toLocalTime().withNano(0))
        Files.createDirectories(journalDir)
        Files.write(
            journalDir.resolve(ReconFileFormat.journalFileName(now.toLocalDate())),
            ReconFileFormat.journalLine(detail),
            StandardOpenOption.CREATE, StandardOpenOption.APPEND,
        )
    }

    /** 기록된 모든 날짜의 거래번호. 기록이 없으면 빈 집합. 줄이 깨졌으면 예외 — 기동을 멈추고 사람이 본다. */
    @Synchronized
    fun processedReferences(): Set<String> {
        if (!Files.isDirectory(journalDir)) return emptySet()
        val journals = Files.list(journalDir).use { files -> files.filter { it.fileName.toString().endsWith(".log") }.toList() }
        return journals.flatMap { ReconFileFormat.parseJournal(Files.readAllBytes(it)) }.map { it.reference }.toSet()
    }

    /** [date] 의 대사 파일을 만들고 경로를 돌려준다. 이미 있으면 그대로 돌려준다. */
    @Synchronized
    fun closeDay(date: LocalDate): Path {
        val target = reconDir.resolve(ReconFileFormat.fileName(date))
        if (Files.exists(target)) return target
        val journal = journalDir.resolve(ReconFileFormat.journalFileName(date))
        val details = if (Files.exists(journal)) ReconFileFormat.parseJournal(Files.readAllBytes(journal)) else emptyList()
        Files.createDirectories(reconDir)
        // 같은 디렉터리의 임시 파일 → 원자적 이름 바꾸기. 읽는 쪽은 완성된 파일만 본다
        val temp = reconDir.resolve("${target.fileName}.tmp")
        Files.write(temp, ReconFileFormat.write(date, bankCode, details))
        Files.move(temp, target, StandardCopyOption.ATOMIC_MOVE)
        return target
    }
}

/** [now] 다음의 [at] (KST) 까지 남은 시간. 정확히 그 시각이면 다음 날 그 시각. */
internal fun delayUntilNextClose(now: Instant, at: LocalTime = DAILY_CLOSE_AT): Duration {
    val zoned = now.atZone(KST)
    var next = zoned.toLocalDate().atTime(at).atZone(KST)
    if (!next.isAfter(zoned)) next = next.plusDays(1)
    return Duration.between(zoned, next)
}
