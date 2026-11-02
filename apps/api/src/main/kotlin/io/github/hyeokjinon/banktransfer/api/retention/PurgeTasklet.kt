package io.github.hyeokjinon.banktransfer.api.retention

import org.springframework.batch.core.scope.context.ChunkContext
import org.springframework.batch.core.step.StepContribution
import org.springframework.batch.core.step.tasklet.Tasklet
import org.springframework.batch.infrastructure.repeat.RepeatStatus
import java.time.Duration
import java.time.Instant

/** 삭제 한 문장: 기준 시각 전 대상을 최대 limit 행 지우고 지운 수를 돌려준다. */
typealias PurgeQuery = (cutoff: Instant, limit: Int) -> Int

/**
 * 표 하나를 지우는 단계 (P5-2, 설계 §4). 한 번 불릴 때 DELETE 한 문장 = 트랜잭션 하나 — 읽었다가 지우는 reader/writer 는 낭비고, 트랜잭션이 짧아 접수를 막지 않는다.
 * 지운 수가 묶음과 같으면 더 있을 수 있어 다시 불린다 (묶음의 배수면 마지막에 0건으로 한 번 더).
 * 기준 시각은 날짜로만 정해 재시작해도 같다. 건수는 DELETE 뒤에 올린다 — 단계는 예외가 나도 contribution 을 반영한다.
 */
class PurgeTasklet(
    private val retention: Duration,
    private val batchSize: Int,
    private val query: PurgeQuery,
) : Tasklet {

    override fun execute(contribution: StepContribution, chunkContext: ChunkContext): RepeatStatus {
        val date = checkNotNull(chunkContext.stepContext.stepExecution.jobParameters.getLocalDate(DataRetention.RETENTION_DATE)) {
            "job parameter ${DataRetention.RETENTION_DATE} is missing"
        }
        val deleted = query(RetentionPolicy.cutoff(date, retention), batchSize)
        contribution.incrementWriteCount(deleted.toLong())
        return if (deleted == batchSize) RepeatStatus.CONTINUABLE else RepeatStatus.FINISHED
    }
}
