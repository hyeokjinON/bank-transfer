# P5-2 보존 배치 — 구현 계획

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 발행된 `outbox_event` 와 `VERIFIED` 가 아닌 만료 `account_verification`, 일회성 데이터 두 종류만 매일 06:30 KST 에 Spring Batch 잡(`dataRetentionJob`)으로 나눠 지우고, 나머지 표를 왜 지우지 않는지 정책표로 남긴다.

**Architecture:** 대사(P5-1)의 까다로운 시작 경합 처리를 복사하지 않고 `api/batch` 의 `DatedJobLauncher`(날짜별 잡 시작 → 결과 종류) · `DatedJobSingleRunGuard` 로 뽑아 두 잡이 같이 쓴다 — 대사의 알림 · 실패 해석은 `DailyReconLauncher` 에 남는다. 잡은 표마다 tasklet 한 단계(① `purgeOutbox` ② `purgeVerifications`)이고, tasklet 은 호출마다 `DELETE … LIMIT n` 한 문장 = READ COMMITTED 트랜잭션 하나다. 실행은 `DataRetentionLauncher` 하나를 스케줄(`0 30 6 * * *`)과 관리 API(`POST /admin/retention-runs`)가 같이 쓰고, 삭제 건수는 그 날짜 인스턴스의 모든 실행의 `WRITE_COUNT` 를 더한다.

**Tech Stack:** Kotlin 2.3.21 · Java 21 · Spring Boot 4.1.1 (Spring 7.0.9, `spring-boot-starter-batch-jdbc`) · Spring Batch 6.0.5 (`JobOperator`, `TaskletStepBuilder`, `Tasklet`) · Hibernate 7.4.5 · Flyway 12.4.0 · MySQL 8.4 · Jackson 3.1.5 · Testcontainers 2.0.5 · JUnit 5 · Gradle 9.7.1 Kotlin DSL

**Spec:** [docs/superpowers/specs/2026-11-02-p5-2-data-retention-design.md](../specs/2026-11-02-p5-2-data-retention-design.md) (승인 2026-11-02. 앞 단계: [P5-1 설계](../specs/2026-10-29-p5-1-daily-reconciliation-design.md) · [P3 설계](../specs/2026-10-19-p3-transfer-design.md), 결정 기록은 ADR-0011 — Task 8 에서 작성)

## 스펙과 다른 점 / 확인 필요

승인된 결정은 바꾸지 않는다. 아래는 코드와 실제 jar 를 확인하다 드러난 스펙의 빈틈과 이 계획의 해결책이다 — 리뷰에서 확인해 달라.

1. **§4 "기존 `DailyReconLauncherTest` 는 그대로 통과" 는 지키지만, 테스트 지원 파일 한 줄은 바꿔야 한다.** `Job` 빈이 둘이 되면 `@Autowired lateinit var job: Job`(`DailyReconTestSupport.kt:36`)과 `dailyReconLauncher(…, job: Job, …)`(`DailyReconJobConfiguration.kt:211`)가 `NoUniqueBeanDefinitionException` 으로 컨텍스트 기동을 깨뜨린다 (이름 `job` 은 빈 이름 `dailyReconciliationJob` 과 맞지 않아 이름 대체도 안 된다). 해결: 두 곳에 `@Qualifier(DailyRecon.JOB_NAME)` 한 줄씩 (Task 4). `@Primary` 는 쓰지 않는다 — 앞으로 `Job` 을 주입하는 코드가 조용히 대사 잡을 받게 된다. `DailyReconLauncherTest.kt` 와 `DailyReconJobTest.kt` 는 고치지 않는다.
2. **§6 변이 검사 "격리 수준 제거 → 잠금 테스트 실패" 는 잠금 테스트가 자기 격리 수준을 직접 들면 성립하지 않는다.** `StatementSettlerIsolationTest` 처럼 `TransactionTemplate` 에 격리 수준을 적어 넣으면 잡 설정을 지워도 테스트는 그대로 통과한다. 해결: `RetentionLockTest` 의 READ COMMITTED 쪽은 **잡의 실제 단계에서 꺼낸 `transactionAttribute`** 로 트랜잭션을 연다 (Task 5). REPEATABLE READ 쪽만 격리 수준을 직접 적는다.
3. **§6 의 테스트들은 공유 DB 에서 그대로는 건수를 단언할 수 없다.** 삭제 조건은 표 전체에 걸린다. `ReconTestDates.unique()`(먼 미래) 를 쓰면 기준 시각이 2100년 이후가 되어 다른 테스트의 발행된 행을 다 지우고, API 는 400 으로 거른다. 오늘 날짜를 쓰면 `JpaVerificationRepositoryTest` 가 남긴 `SENT`(만료 2026-10-19)까지 지워 건수가 흔들린다. 해결: `RetentionTestDates.next()` — **2000-01-01 부터 40일씩 과거로만 내려가는 날짜** (Task 3). 앞 테스트가 남긴 행은 모두 다음 테스트의 기준 시각보다 뒤라 걸리지 않는다. 또 `OutboxRelayTest` 가 표 전체의 미발행 건수를 세므로(`pendingCount`) **테스트가 심은 미발행 행은 테스트 끝에 지운다** (`RetentionTestSupport`).
4. **§6 "① 뒤 ② 실패 → 재시작" 은 실패를 끼우는 지점이 필요하다.** 대사 테스트처럼 행 락으로 실패를 만들면 묶음 순서가 옵티마이저 선택(인덱스 / 전체 훑기)에 달려 "첫 묶음은 커밋, 둘째에서 실패" 가 흔들린다. 해결: 잡 조립을 `internal fun buildDataRetentionJob(…, purgeOutbox, purgeVerifications)` 로 빼고 `@Bean` 은 이것을 부른다. 테스트는 같은 함수에 실패를 끼운 삭제 함수를 넘긴다 (같은 잡 이름 · 같은 단계 · 같은 트랜잭션 설정).
5. **§4 의 `DefaultTransactionAttribute` 그대로는 tasklet 에서 위험하다.** `TaskletStep` 의 기본 속성은 `rollbackOn = true`(모든 예외)다. 대사의 청크처럼 `DefaultTransactionAttribute().apply { isolationLevel = … }` 를 넘기면 검사 예외에 `rollbackOn` 이 false 가 되고, `ChunkTransactionCallback` 은 그 예외를 **삼키고 초깃값 `CONTINUABLE` 을 돌려줘 같은 호출이 끝없이 돈다** (6.0.5 소스 `TaskletStep.java`). Kotlin 은 검사 예외를 막지 않는다. 해결: `rollbackOn` 을 true 로 고정한 속성을 쓰고 리플렉션 테스트에 함께 단언한다 (Task 4).
6. **§8 이 말하지 않은 낡은 문장.** `docs/database.md` §10 · `docs/adr/0010` 결과 절 · HANDOVER 가 "`bank_statement_line` · `recon_issue` · 원장 보존은 P5-2" 라고 적고 있다. 스펙 §1 은 이것들을 범위 밖(지우지 않음)으로 정했으므로 문장을 고친다 (Task 8). `OPERATION.md:43` 에도 스케줄 스레드 3 이 있고, `OPERATION.md:249` 의 부하 측정 명령줄은 `--daily-recon.schedule.enabled=false` 조차 빠져 있어 둘 다 넣는다. §8 은 "SETUP 변화 없음" 이지만 `SETUP.md:26` 의 테스트 수는 고친다.

실패 알림의 범위(스펙은 스케줄 · 수동을 가르지 않는다)와 `RESTART_REFUSED` 처리는 아래 "결정(계획) 모음" 에 적었다.

## Global Constraints

스펙에서 그대로 옮긴 프로젝트 전체 요구 (모든 Task 의 요구사항에 암묵적으로 포함된다):

- **지우는 조건 (§3, 이 단계의 핵심)**: `outbox_event` = `published_at < :cutoff` (기본 보관 7일). `account_verification` = `status IN ('SENT','EXPIRED','LOCKED') AND expires_at < :cutoff` (기본 보관 30일). **`VERIFIED` 는 절대 지우지 않는다.** 미발행(`published_at IS NULL`)은 아무리 오래돼도 남는다.
- **잡**: `dataRetentionJob`, 식별 파라미터 `retentionDate: LocalDate`, ① `purgeOutbox` ② `purgeVerifications`. 호출마다 `DELETE … LIMIT batchSize` 한 문장, `contribution.incrementWriteCount(n)`, `n == batchSize` 면 `CONTINUABLE` 아니면 `FINISHED`. `ORDER BY` 없음. 두 단계 모두 트랜잭션 관리자를 명시하고 `READ COMMITTED`.
- **기준 시각**: `RetentionPolicy` 순수 함수 `date.atStartOfDay(Asia/Seoul) − 보관 기간`. 시각 바인딩은 `ReconRepository` 와 같이 `Timestamp.from(Instant)`.
- **인덱스**: `V5__data_retention.sql` 이 `idx_verification_purge (status, expires_at)` 추가. 아웃박스는 기존 `idx_outbox_unpublished (published_at, id)`.
- **설정 (§5)**: `data-retention.outbox-retention: 7d` · `verification-retention: 30d` · `batch-size: 1000` · `schedule.enabled: true` · `schedule.cron: "0 30 6 * * *"` · `schedule.zone: Asia/Seoul`. 보관 기간 1일 미만 또는 `batch-size` 1 ~ 10000 밖이면 **기동 실패**. 스케줄 풀 3 → **4**.
- **실행 · 실패 (§4)**: 스케줄은 오늘 날짜로, 이미 끝난 날짜는 INFO 로 넘어간다. 실패는 멈춘 단계 · 지운 만큼 그대로 두고 텔레그램 WARN `보존 배치 {date} 실패` (예외 종류만, 내부 메시지는 로그). 자동 재시도 없음. 성공은 표별 건수 INFO 로그, 텔레그램 없음.
- **수동 실행 (§4)**: `POST /admin/retention-runs?date=yyyy-MM-dd`, 요청 스레드에서 끝까지. 200 `{"date","status":"COMPLETED","reason":null,"deleted":{"outboxEvents":n,"verifications":m}}` (건수는 이 날짜 인스턴스의 모든 실행의 합) · 200 `{"status":"FAILED","reason":{"code","message"},"deleted":null}` · 409 `RETENTION_ALREADY_COMPLETED` · 409 `RETENTION_RUNNING` · 400 `RETENTION_DATE_IN_FUTURE`(내일 이후, KST. 오늘과 지난 날은 받는다).
- **대사 동작은 바뀌지 않는다**: `DailyReconLauncher` 의 관찰 가능한 동작 그대로, `DailyReconLauncherTest` 는 고치지 않고 통과한다.
- **문서 · 주석 · 커밋은 한국어**, 주석은 "왜" 를 쓰고 주변 코드의 밀도와 말투를 따른다 (CLAUDE.md). 도메인 모듈(`modules/*`)은 건드리지 않는다. TDD. 설정 파일은 먼저 읽고 합친다(덮어쓰기 금지). 커밋 `type(scope): 한국어 요약`, 마지막 줄 `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`. push 하지 않는다. 비밀(텔레그램 토큰 등)은 환경 변수로만, 저장소에 넣지 않는다.
- **테스트는 Testcontainers 만** (`ApiIntegrationTest` 상속 — 컨텍스트 하나 공유). **개발자 PC 의 로컬 인프라 MySQL(3306)을 바꾸거나 거기서 무엇도 지우지 않는다.** `bootRun` 으로 앱을 띄워 확인하지 않는다 — 기본 설정이 로컬 3306 을 본다. 손 확인(`http/data-retention.http`, Postman 7 폴더)은 **PR 뒤 사용자 몫**이고 구현자는 실행하지 않는다.
- **명령 (Windows, Git Bash, 저장소 루트)**: 통합 테스트 전에 Docker Desktop 을 띄우고 `export PATH="/c/Users/user/AppData/Local/Programs/DockerDesktop/resources/bin:$PATH"`. 대상 테스트 `./gradlew :apps:api:test --tests '*이름*'` (첫 컨테이너 기동 포함 약 1분, 추정). 전체 `./gradlew build` — `apps:api` 지난 리포트의 테스트 시간 합이 223개 약 130초였고, 컴파일 · 컨테이너 기동 · 다른 모듈 · 이 계획의 새 테스트까지 **5분 안팎으로 추정** (측정값 아님. Bash 도구 기본 시간 제한 2분을 넘으므로 긴 시간 제한으로 돌린다).
- **Postman**: `postman/collections/bank-transfer/` 는 기존 목록형 YAML(`scripts:` 아래 `- type: …`)을 유지한다. Postman 앱이 다른 형식으로 다시 쓴 파일은 커밋하지 않는다 — 커밋 전에 `git status` 로 확인하고 `git add` 는 경로를 적는다.

**버전 · API 확인 결과 (계획 작성 때 실제 jar 로 확인함 — 구현자는 다시 찾지 않아도 된다)**

확인 방법: `~/.gradle/caches/modules-2/files-2.1/` 의 `spring-batch-core-6.0.5.jar` · `-sources.jar` · `spring-boot-4.1.1.jar` 를 `javap` 와 소스로 읽었다. 버전은 `spring-boot-dependencies-4.1.1.pom` 과 `build.gradle.kts`. **코드는 컴파일하지 않았고 Docker 데몬이 꺼져 있어 MySQL 동작(잠금 가설)은 돌려 보지 않았다.**

1. **버전**: 지시받은 그대로다 — Kotlin 2.3.21, Boot 4.1.1, Spring Batch 6.0.5, Spring 7.0.9, Hibernate 7.4.5.Final, Flyway 12.4.0, Jackson 3.1.5, Testcontainers 2.0.5. Gradle 래퍼는 9.7.1. Flyway 파일은 V1 ~ V4, 다음은 V5.
2. **tasklet 단계 빌더**: `StepBuilder(name, repo).tasklet(Tasklet, PlatformTransactionManager): TaskletStepBuilder`. `transactionAttribute(TransactionAttribute)` 는 부모 `AbstractTaskletStepBuilder<B>` 에 있고 `B`(= `TaskletStepBuilder`)를 돌려준다. `build(): TaskletStep`. `tasklet(Tasklet)` 만 쓰면 트랜잭션 관리자가 없다.
3. **리플렉션 필드 이름**: `TaskletStep` 의 private 필드 `transactionManager` · `transactionAttribute` (대사의 `ChunkOrientedStep` 과 같은 이름). `SimpleJob.getStep(name)` 도 그대로다.
4. **트랜잭션 경계**: `TaskletStep.doExecute` 가 호출마다 `TransactionTemplate(transactionManager, transactionAttribute).execute(ChunkTransactionCallback)`. 그 안에서 `tasklet.execute` → (`finally`) `stepExecution.apply(contribution)` → `incrementCommitCount()` → `jobRepository.update(stepExecution)`. 즉 **`commitCount` = 호출 수**, 메타 표 갱신도 같은 트랜잭션이다. 커밋이 실패하면 `afterCompletion` 이 건수를 이전 값으로 되돌린다. `apply` 는 예외 때도 불리므로 **`incrementWriteCount` 는 DELETE 뒤에** 부른다.
5. **기본 속성의 함정** ("스펙과 다른 점" 5): `TaskletStep.transactionAttribute` 기본값은 `rollbackOn` 이 언제나 true 인 익명 `DefaultTransactionAttribute` 다. `rollbackOn` 이 false 인 예외는 콜백이 삼키고 `result` 는 초깃값 `CONTINUABLE` 로 남는다.
6. **`StepContribution.incrementWriteCount(long)`**: 인자 없는 판은 없다 (`incrementReadCount()` 와 다르다). `Int` 를 넘기려면 `.toLong()`. `StepExecution.getWriteCount()` · `getCommitCount()` 는 `long`.
7. **모든 실행의 건수**: `jobRepository.getJobExecutions(JobInstance): List<JobExecution>` (`SimpleJobExplorer`) 는 실행마다 `stepExecutionDao.getStepExecutions` 까지 채워 준다 → `execution.stepExecutions`. **`findJobExecutions` 는 폐기 예정이고 단계를 채우지 않는다 — 쓰지 않는다.** 재시작하면 멈춘 단계에 건수 0 인 새 `StepExecution` 이 생기고(`SimpleStepHandler.handleStep`), 끝난 단계는 새 행이 없다(`shouldStart` false) → 같은 단계 이름의 `writeCount` 합 = 정답. `getLastStepExecution` 만 보면 재시작 뒤 건수가 모자란다.
8. **실행**: `JobOperator.start(Job, JobParameters)` → `SimpleJobOperator.start` → `run(job, params)`. 넘긴 `Job` 객체를 그대로 쓰고 레지스트리를 보지 않는다 → 테스트가 조립한 (빈이 아닌) 같은 이름의 잡도 된다. `Job.execute(JobExecution)` 은 public 이고, `AbstractJob` 은 `beforeJob` 의 예외를 잡아 그 실행을 FAILED 로 끝낸다. 레지스트리는 기동 때 `getBeansOfType(Job)` 으로 채워지고 이름이 다르면 충돌하지 않는다.
9. **`JobRepository.findRunningJobExecutions(jobName)` 은 잡 이름 전체를 돌려준다** — 가드는 `jobInstanceId` 로 거른다. 그래서 어제 날짜의 실행이 `STARTED` 로 남아도 오늘 인스턴스는 막지 않는다 (§4 "다음 날 인스턴스는 막지 않는다" 확인. `TaskExecutorJobLauncher.createJobExecution` 도 그 인스턴스의 실행만 본다).
10. **스텝 범위 빈은 필요 없다**: tasklet 은 `chunkContext.stepContext.stepExecution.jobParameters.getLocalDate(name)` 으로 날짜를 읽는다 (`reconDateOf` 와 같다). 그래서 "반환형은 인터페이스" 함정을 밟지 않는다.
11. **Boot**: `Binder.bind(String, Class<T>): BindResult<T>`, `ConfigurationPropertySources.from(Iterable<PropertySource<?>>)`, `YamlPropertySourceLoader`, `ApplicationContextRunner` 모두 Boot 4.1.1 의 원래 패키지에 있다 (`org.springframework.boot.context.properties.bind` · `….source` · `org.springframework.boot.env` · `org.springframework.boot.test.context.runner`).
12. **코드 사실**: `VerificationStatus` = `SENT, VERIFIED, EXPIRED, LOCKED`. `existsVerified` 는 `existsByUserIdAndBankCodeAndAccountNoAndStatus(…, VERIFIED)` — `expires_at` 을 보지 않는다. `EXPIRED` 는 `AccountVerificationService.confirm` 이 만료 뒤 확인을 시도했을 때만 된다. Hikari `connection-init-sql` 이 모든 커넥션을 `innodb_lock_wait_timeout = 5` 로 둔다. `outbox_event.id` 는 `BIGINT AUTO_INCREMENT` 라 새 행의 인덱스 키는 `(NULL, 가장 큰 id)` 다. Kafka 보존은 `infra/docker-compose.yml` 이 덮어쓰지 않아 브로커 기본(7일). 테스트 JVM 은 `-Duser.timezone=UTC`, MySQL 세션은 KST.

## 결정(계획) 모음

스펙이 정하지 않은 곳의 판단이다. 각 Task 안에도 같은 표시로 다시 적는다.

- 결정(계획): 공용 코드 이름 = 패키지 `api.batch`, `DatedJobLauncher.start(date): DatedJobStart`, `DatedJobSingleRunGuard`, 예외 부모 `DatedJobRunningException`. 결과 종류 `DatedJobStart` = `Ended(execution)` · `AlreadyCompleted` · `Running` · `RestartRefused(message)` · `NotStarted(cause)`. `Ended` 는 완료 · 실패를 가르지 않는다 — 실패 해석은 각 실행기 몫이다.
- 결정(계획): 가드가 멈춘 실행의 판별은 `DatedJobRunningException` 하위 타입 검사 하나로 공용 코드에서 한다 (콜백을 받지 않는다). `ReconRunningException` · `RetentionRunningException` 이 하위 타입이다 — 관리 API 의 409 코드가 잡마다 다르므로 타입은 따로 둔다.
- 결정(계획): `DailyReconLauncherTest` 를 고치지 않으려고 `dailyrecon` 패키지에 **같은 이름의 팩토리 함수** `fun SingleRunGuard(jobRepository)` 를 둔다 (테스트 · `DailyReconJobConfiguration` 의 호출 그대로). 대사 쪽 예외 · 파라미터 이름은 그 안에 묶는다. `DailyReconLauncher` 생성자 시그니처도 그대로다.
- 결정(계획): 실패 WARN 알림은 스케줄 · 수동 모두 보낸다 — 스펙 §4 가 가르지 않고, 수동 실행자가 응답을 봐도 기록이 남아 해롭지 않다. `Running` · `AlreadyCompleted` 는 알리지 않는다 (대사의 "마지막 회차" 같은 개념이 없다 — 다음 날 새 인스턴스가 돈다).
- 결정(계획): 실패 이유 코드 = `ERROR`(메시지 = 예외 클래스 이름) · `RESTART_REFUSED`(메시지 = 고정 문장, `e.message` 는 로그에만 — §4 "예외 종류만"). 대사의 `RESTART_REFUSED` 는 지금처럼 `e.message` 를 그대로 둔다 (동작을 바꾸지 않는다).
- 결정(계획): 테스트 날짜 `RetentionTestDates.next()` = 2000-01-01 에서 40일씩 과거로 ("스펙과 다른 점" 3). 40 > 30(인증 보관) + 1(심는 여유). 1988년 뒤로는 KST 에 일광 절약이 없고, 60번 불러도 1993년이라 안전하다.
- 결정(계획): 재시작 · 동시 실행 테스트의 실패 · 멈춤은 `buildDataRetentionJob` 에 끼운 함수로 만든다 ("스펙과 다른 점" 4). 삭제 함수 타입은 `typealias PurgeQuery = (cutoff: Instant, limit: Int) -> Int`.
- 결정(계획): `NOW(6)` 비교 테스트는 한 트랜잭션 안에서 지우고 **되돌린다** — 실제 시각 기준 삭제는 공유 DB 의 다른 테스트 행까지 건다.
- 결정(계획): `RetentionLockTest` 의 INSERT 는 전용 커넥션에서 `SET SESSION innodb_lock_wait_timeout = 2`, 끝나면 풀 값 5 로 되돌린다. 바깥에 `Future.get(20초)` 를 한 겹 더 둔다 — 어떤 경우에도 매달리지 않는다.
- 결정(계획): 설정 하한 검사는 `DataRetentionProperties` 의 `init` — 바인딩 실패 = 기동 실패. `Duration` 상수 `MIN_RETENTION = 1일`, `MAX_BATCH_SIZE = 10_000`.
- 결정(계획): 변이 검사는 일회성 — Task 8 에서 돌리고 결과를 ADR 에 적는다. 변이는 커밋하지 않는다.

## 파일 구조

| 파일 | 책임 |
|---|---|
| `apps/api/.../api/batch/DatedJobLauncher.kt` (신규, Task 1) | 날짜별 잡 시작 · 시작 경합 처리 · 결과 종류, `DatedJobSingleRunGuard`, `DatedJobRunningException`, `failureCauses()` |
| `apps/api/.../api/dailyrecon/DailyReconLauncher.kt` (수정, Task 1) | 대사의 실패 해석 · 알림만 남긴다. `fun SingleRunGuard(…)` |
| `apps/api/.../api/dailyrecon/DailyReconJobConfiguration.kt` (수정, Task 4) | `job` 에 `@Qualifier` |
| `apps/api/.../api/retention/DataRetention.kt` (신규, Task 2) | 이름 · 상수 · 파라미터, `RetentionPolicy`, `DataRetentionProperties`, 예외 3종 |
| `apps/api/src/main/resources/db/migration/V5__data_retention.sql` (신규, Task 3) | `idx_verification_purge` |
| `apps/api/.../api/retention/RetentionRepository.kt` (신규, Task 3) | 삭제 SQL 두 문장 (이 단계의 핵심 조건) |
| `apps/api/.../api/retention/DataRetentionJobConfiguration.kt` (신규, Task 3 · 4 · 6) | 빈 조립, `buildDataRetentionJob`, 단계 트랜잭션 속성 |
| `apps/api/.../api/retention/PurgeTasklet.kt` (신규, Task 4) | 한 번 불릴 때 DELETE 한 문장 |
| `apps/api/.../api/retention/DataRetentionLauncher.kt` (신규, Task 6) | 실행 결과 · 건수 합 · 실패 알림 |
| `apps/api/.../api/retention/DataRetentionScheduler.kt` (신규, Task 6) | 06:30 스케줄 |
| `apps/api/.../api/web/RetentionController.kt` (신규, Task 7), `ApiExceptionHandler.kt` (수정) | `POST /admin/retention-runs`, 409 · 400 |
| `apps/api/src/main/resources/application.yml` (수정, Task 2 · 6) | `data-retention.*`, 스케줄 풀 4 |
| 테스트 `apps/api/src/test/.../api/retention/` | `RetentionTestDates` · `RetentionTestSupport`(Task 3), `RetentionPolicyTest`(2), `RetentionRepositoryTest`(3), `DataRetentionJobTest`(4), `RetentionLockTest`(5), `DataRetentionLauncherTest` · `DataRetentionSchedulerTest`(6) |
| 테스트 (수정 · 그 밖) | `DatabaseSetupTest`(3), `DailyReconTestSupport` 한 줄(4), `support/ApiIntegrationTest` 한 줄(6), `web/RetentionApiTest`(7, 신규) |
| 문서 (Task 8) | ADR-0011, `docs/database.md`, OPERATION, ARCHITECTURE, README, SETUP(테스트 수), HANDOVER, CHANGELOG, `docs/adr/0010` 두 줄, `http/data-retention.http`, Postman `7-data-retention/` + `.resources/definition.yaml`, `.run/5` |

`…` 경로는 모두 `src/main/kotlin/io/github/hyeokjinon/banktransfer` (테스트는 `src/test/kotlin/…`) 아래다.

---

### Task 1: 시작 경합 처리를 `DatedJobLauncher` 로 뽑는다 (대사 동작 변화 없음, 리팩터링만)

리뷰어가 이 Task 만 따로 거절할 수 있게 다른 일을 섞지 않는다. 새 테스트는 없다 — 바뀌지 않은 대사 테스트가 안전망이다.

**Files:**
- Create: `apps/api/src/main/kotlin/io/github/hyeokjinon/banktransfer/api/batch/DatedJobLauncher.kt`
- Modify: `apps/api/src/main/kotlin/io/github/hyeokjinon/banktransfer/api/dailyrecon/DailyReconLauncher.kt` (전체)
- Test (고치지 않는다): `dailyrecon/DailyReconLauncherTest.kt` · `DailyReconJobTest.kt` · `DailyReconSchedulerTest.kt` · `StatementSettlerIsolationTest.kt`, `web/ReconciliationApiTest.kt`

**Interfaces:**
- Consumes: 기존 `DailyRecon.parameters(date)` · `DailyRecon.RECON_DATE`, `ReconSummaryReader`, `AlertNotifier`
- Produces (패키지 `io.github.hyeokjinon.banktransfer.api.batch`):
  - `open class DatedJobRunningException(message: String) : RuntimeException`
  - `sealed interface DatedJobStart { data class Ended(val execution: JobExecution); data object AlreadyCompleted; data object Running; data class RestartRefused(val message: String); data class NotStarted(val cause: IllegalStateException) }`
  - `class DatedJobLauncher(jobOperator: JobOperator, job: Job, jobRepository: JobRepository, parameters: (LocalDate) -> JobParameters, pauseBeforeRetry: () -> Unit = …) { fun start(date: LocalDate): DatedJobStart; companion object { const val RETRY_PAUSE_MS = 500L } }`
  - `class DatedJobSingleRunGuard(jobRepository: JobRepository, dateParameter: String, running: (LocalDate) -> DatedJobRunningException) : JobExecutionListener`
  - `fun JobExecution.failureCauses(): List<Throwable>`
- 그대로 유지: `DailyReconLauncher(jobOperator, job, jobRepository, summaries, alerts, pauseBeforeRetry)` 시그니처, `run(date, trigger): ReconRun`, `ReconRunningException(date)`, `dailyrecon.SingleRunGuard(jobRepository)` (이제 함수)

- [ ] **Step 1: 기준선 — 바꾸기 전에 대사 테스트가 통과하는지 본다**

Run: `./gradlew :apps:api:test --tests '*DailyReconLauncherTest*' --tests '*DailyReconJobTest*' --tests '*DailyReconSchedulerTest*' --tests '*ReconciliationApiTest*' --tests '*StatementSettlerIsolationTest*'`
Expected: BUILD SUCCESSFUL

- [ ] **Step 2: 공용 코드 작성**

`batch/DatedJobLauncher.kt`:
```kotlin
package io.github.hyeokjinon.banktransfer.api.batch

import org.slf4j.LoggerFactory
import org.springframework.batch.core.BatchStatus
import org.springframework.batch.core.job.Job
import org.springframework.batch.core.job.JobExecution
import org.springframework.batch.core.job.parameters.JobParameters
import org.springframework.batch.core.launch.JobExecutionAlreadyRunningException
import org.springframework.batch.core.launch.JobInstanceAlreadyCompleteException
import org.springframework.batch.core.launch.JobOperator
import org.springframework.batch.core.launch.JobRestartException
import org.springframework.batch.core.listener.JobExecutionListener
import org.springframework.batch.core.repository.JobRepository
import org.springframework.dao.DuplicateKeyException
import org.springframework.dao.PessimisticLockingFailureException
import java.time.LocalDate

/** [DatedJobSingleRunGuard] 가 나중 실행을 멈출 때 던지는 예외의 부모. 관리 API 의 409 코드가 잡마다 달라 타입은 잡마다 하나씩 둔다. */
open class DatedJobRunningException(message: String) : RuntimeException(message)

/** 날짜별 잡을 한 번 시작한 결과의 종류. 알림 · 실패 해석은 각 잡의 실행기가 한다. */
sealed interface DatedJobStart {
    /** 이 스레드에서 끝까지 돌았다. 완료인지 실패인지는 [execution] 의 status 를 본다. */
    data class Ended(val execution: JobExecution) : DatedJobStart

    /** 끝난 날짜 — 돌리지 않았다. */
    data object AlreadyCompleted : DatedJobStart

    /** 다른 곳이 돌고 있다 — 돌리지 않았거나, 가드가 이 실행을 멈췄다. */
    data object Running : DatedJobStart

    /** 지난 실행이 UNKNOWN(롤백할 수 없는 실패)으로 끝났다 — 사람이 BATCH_* 상태를 확인해야 한다. */
    data class RestartRefused(val message: String) : DatedJobStart

    /** 다시 시도해도 실행 없는 인스턴스다 — 만들던 쪽이 그 사이에 죽었다. 사람이 BATCH_JOB_INSTANCE 를 정리해야 돈다. */
    data class NotStarted(val cause: IllegalStateException) : DatedJobStart
}

/**
 * 날짜 파라미터 하나로 인스턴스가 갈리는 잡을 이 스레드에서 끝까지 돌린다 (P5-1 에서 시작, P5-2 에서 뽑음). Spring Batch 6 의 JobOperator 는 기본이 동기 실행(SyncTaskExecutor)이다.
 * 같은 날짜는 잡 저장소가 인스턴스 하나로 둔다: 끝났으면 [DatedJobStart.AlreadyCompleted], 돌고 있으면 [DatedJobStart.Running], 실패했으면 멈춘 곳부터 다시.
 * 처음 시작하는 날짜를 두 곳이 동시에 시작하면 진 쪽은 잡 인스턴스 UNIQUE 에서 DuplicateKeyException(직렬화 격리라 데드락일 수도)을 받는다 → Running.
 * 인스턴스와 실행 생성 사이에 온 쪽은 "실행 없는 인스턴스" 를 본다 → 잠깐 기다려 한 번 더 ([startOrRetry]).
 * 실패한 날짜를 두 곳이 동시에 재시작하는 경우는 [DatedJobSingleRunGuard] 가 나중 실행을 멈춘다 → Running.
 * 이 경합들은 까다로워 잡마다 복사하지 않는다 — 대사와 보존 배치가 이 코드 하나를 쓴다.
 */
class DatedJobLauncher(
    private val jobOperator: JobOperator,
    private val job: Job,
    private val jobRepository: JobRepository,
    private val parameters: (LocalDate) -> JobParameters,
    /** 실행 없는 인스턴스를 만났을 때 다시 시도하기 전 기다림 (테스트는 이 사이에 실행을 만들어 끼운다). */
    private val pauseBeforeRetry: () -> Unit = { Thread.sleep(RETRY_PAUSE_MS) },
) {
    private val log = LoggerFactory.getLogger(javaClass)

    fun start(date: LocalDate): DatedJobStart {
        val execution = try {
            startOrRetry(parameters(date))
        } catch (e: JobInstanceAlreadyCompleteException) {
            return DatedJobStart.AlreadyCompleted
        } catch (e: JobExecutionAlreadyRunningException) {
            return DatedJobStart.Running
        } catch (e: DuplicateKeyException) {
            return DatedJobStart.Running
        } catch (e: PessimisticLockingFailureException) {
            return DatedJobStart.Running
        } catch (e: JobRestartException) {
            return DatedJobStart.RestartRefused(e.message.orEmpty())
        } catch (e: IllegalStateException) {
            return DatedJobStart.NotStarted(e)
        }
        val stoppedByGuard = execution.status != BatchStatus.COMPLETED && execution.failureCauses().any { it is DatedJobRunningException }
        return if (stoppedByGuard) DatedJobStart.Running else DatedJobStart.Ended(execution)
    }

    /**
     * Spring Batch 6.0.5 의 시작은 인스턴스와 실행을 서로 다른 트랜잭션에서 만든다(TaskExecutorJobLauncher.createJobExecution).
     * 처음 시작하는 날짜를 다른 곳이 막 시작해 인스턴스만 보이는 순간에 오면 IllegalStateException("Cannot find any job execution …") 이다 —
     * 여러 인스턴스의 같은 정각 회차가 흔한 경우. 인스턴스가 있으면 잠깐 기다려 한 번 더: 그때는 "돌고 있음"(→ Running)이나 "이미 완료" 다.
     */
    private fun startOrRetry(params: JobParameters): JobExecution =
        try {
            jobOperator.start(job, params)
        } catch (e: IllegalStateException) {
            if (jobRepository.getJobInstance(job.name, params) == null) throw e
            log.info("{} instance {} has no execution yet, retrying once", job.name, params)
            pauseBeforeRetry()
            jobOperator.start(job, params)
        }

    companion object {
        /** 인스턴스 커밋과 실행 커밋 사이는 보통 수 ms — 넉넉히. */
        const val RETRY_PAUSE_MS = 500L
    }
}

/** 실패 원인을 안쪽 원인까지 펼친다 (바깥부터). */
fun JobExecution.failureCauses(): List<Throwable> =
    allFailureExceptions.flatMap { generateSequence(it) { cause -> cause.cause }.toList() }

/**
 * 같은 날짜의 잡이 동시에 둘 돌지 않게 하는 마지막 확인 (P5-1, 설계 "스펙과 다른 점" 1 — ADR-0010 결정 5).
 * Spring Batch 6.0.5 의 시작 경로는 "돌고 있는 실행이 있나" 를 본 뒤 별도 트랜잭션에서 실행을 만든다 — 실패한 날짜를 두 곳이 동시에
 * 재시작하면 둘 다 확인을 통과할 수 있다(새 날짜는 잡 인스턴스 UNIQUE 가 막는다). 그래서 실행이 시작된 직후, 같은 인스턴스에 먼저 만든(작은 id)
 * 실행이 아직 돌고 있으면 이 실행을 멈춘다 — 이 실행만 FAILED 로 끝나고 [DatedJobLauncher] 는 "실행 중" 으로 답한다.
 * 먼저 실행이 끝난 뒤의 재시작은 막지 않는다 (돌고 있는 실행이 없다). 다른 날짜(다른 인스턴스)의 실행도 막지 않는다.
 *
 * @param dateParameter 예외 메시지에 쓸 날짜의 식별 파라미터 이름
 * @param running 이 잡의 "실행 중" 예외를 만든다
 */
class DatedJobSingleRunGuard(
    private val jobRepository: JobRepository,
    private val dateParameter: String,
    private val running: (LocalDate) -> DatedJobRunningException,
) : JobExecutionListener {

    override fun beforeJob(jobExecution: JobExecution) {
        val earlierRunning = jobRepository.findRunningJobExecutions(jobExecution.jobInstance.jobName)
            .any { it.jobInstanceId == jobExecution.jobInstanceId && it.id < jobExecution.id }
        if (earlierRunning) {
            throw running(checkNotNull(jobExecution.jobParameters.getLocalDate(dateParameter)))
        }
    }
}
```

- [ ] **Step 3: `DailyReconLauncher.kt` 를 대사의 해석 · 알림만 남긴다**

`dailyrecon/DailyReconLauncher.kt` 전체:
```kotlin
package io.github.hyeokjinon.banktransfer.api.dailyrecon

import io.github.hyeokjinon.banktransfer.api.alert.Alert
import io.github.hyeokjinon.banktransfer.api.alert.AlertLevel
import io.github.hyeokjinon.banktransfer.api.alert.AlertNotifier
import io.github.hyeokjinon.banktransfer.api.alert.notifySafely
import io.github.hyeokjinon.banktransfer.api.batch.DatedJobLauncher
import io.github.hyeokjinon.banktransfer.api.batch.DatedJobRunningException
import io.github.hyeokjinon.banktransfer.api.batch.DatedJobSingleRunGuard
import io.github.hyeokjinon.banktransfer.api.batch.DatedJobStart
import io.github.hyeokjinon.banktransfer.api.batch.failureCauses
import org.slf4j.LoggerFactory
import org.springframework.batch.core.BatchStatus
import org.springframework.batch.core.job.Job
import org.springframework.batch.core.job.JobExecution
import org.springframework.batch.core.launch.JobOperator
import org.springframework.batch.core.repository.JobRepository
import java.time.LocalDate

/** 누가 실행했나 — 실패 알림이 갈린다. */
enum class ReconTrigger { SCHEDULE, SCHEDULE_FINAL, MANUAL }

/** 실패 이유. code = FILE_NOT_READY · FILE_INVALID · RESTART_REFUSED · ERROR. ERROR 는 예외 종류만 싣는다 (내부 메시지는 로그에). */
data class ReconFailure(val code: String, val message: String)

/** 실행 한 번의 결과. */
sealed interface ReconRun {
    val date: LocalDate

    data class Completed(override val date: LocalDate, val summary: ReconSummary) : ReconRun
    data class Failed(override val date: LocalDate, val failure: ReconFailure) : ReconRun
    data class AlreadyCompleted(override val date: LocalDate) : ReconRun
    data class Running(override val date: LocalDate) : ReconRun
}

class ReconRunningException(date: LocalDate) : DatedJobRunningException("daily reconciliation for $date is already running")

class ReconAlreadyCompletedException(date: LocalDate) : RuntimeException("daily reconciliation for $date is already completed")

/** 아직 마감되지 않은 날(오늘 이후, KST)의 수동 대사 — 지난 날만 대사할 수 있다 ([DailyRecon.requireClosed]). */
class ReconDateNotClosedException(date: LocalDate, today: LocalDate) :
    RuntimeException("recon date $date is not closed yet (today is $today, KST) - only past days can be reconciled")

/**
 * 대사일 하나의 잡을 이 스레드에서 끝까지 돌리고 대사의 말로 옮긴다 (P5-1). 시작 경합 처리는 [DatedJobLauncher] 가 한다.
 *
 * 실패 알림 (설계 §8): 스케줄의 마지막 회차는 이유와 무관하게 "대사 미완료" CRITICAL — 실패든 "아직 돌고 있음"(Running)이든,
 * 그 전 회차 · 수동의 FILE_NOT_READY 는 알리지 않음 (은행 파일은 보통 00:05 에 온다), 그 밖 실패는 CRITICAL. 완료 알림은 잡의 ⑥ 보고가 보낸다.
 */
class DailyReconLauncher(
    jobOperator: JobOperator,
    job: Job,
    jobRepository: JobRepository,
    private val summaries: ReconSummaryReader,
    private val alerts: AlertNotifier,
    /** 실행 없는 인스턴스를 만났을 때 다시 시도하기 전 기다림 (테스트는 이 사이에 실행을 만들어 끼운다). */
    pauseBeforeRetry: () -> Unit = { Thread.sleep(DatedJobLauncher.RETRY_PAUSE_MS) },
) {
    private val log = LoggerFactory.getLogger(javaClass)
    private val starter = DatedJobLauncher(jobOperator, job, jobRepository, DailyRecon::parameters, pauseBeforeRetry)

    fun run(date: LocalDate, trigger: ReconTrigger): ReconRun =
        when (val start = starter.start(date)) {
            is DatedJobStart.Ended -> outcomeOf(date, start.execution, trigger)
            DatedJobStart.AlreadyCompleted -> {
                log.info("daily reconciliation {} is already completed, skipped ({})", date, trigger)
                ReconRun.AlreadyCompleted(date)
            }
            DatedJobStart.Running -> running(date, trigger)
            // 지난 실행이 UNKNOWN(롤백할 수 없는 실패)으로 끝났다 — 사람이 BATCH_* 상태를 확인해야 한다 (OPERATION §9)
            is DatedJobStart.RestartRefused -> failed(date, ReconFailure("RESTART_REFUSED", start.message), trigger)
            // 다시 시도해도 실행 없는 인스턴스다 — 만들던 쪽이 그 사이에 죽었다. 이 날짜는 사람이 BATCH_JOB_INSTANCE 를 정리해야 돈다 (OPERATION §9)
            is DatedJobStart.NotStarted -> {
                log.error("daily reconciliation {} could not be started ({})", date, trigger, start.cause)
                failed(date, ReconFailure("ERROR", start.cause.javaClass.name), trigger)
            }
        }

    private fun outcomeOf(date: LocalDate, execution: JobExecution, trigger: ReconTrigger): ReconRun {
        if (execution.status == BatchStatus.COMPLETED) return ReconRun.Completed(date, summaries.read(execution.jobInstance, date))
        val causes = execution.failureCauses()
        val failure = causes.firstNotNullOfOrNull { it as? ReconFileException }?.let { ReconFailure(it.code, it.message.orEmpty()) }
            ?: ReconFailure("ERROR", causes.firstOrNull()?.javaClass?.name ?: execution.exitStatus.exitCode)
        return failed(date, failure, trigger)
    }

    private fun running(date: LocalDate, trigger: ReconTrigger): ReconRun {
        log.info("daily reconciliation {} is already running elsewhere, skipped ({})", date, trigger)
        // 스펙 §8 "06:00 회차까지 완료하지 못하면 알림" 은 "돌고 있음" 도 예외로 두지 않는다 — 대사 도중 JVM 이 죽어 실행이 STARTED 로 남으면
        // 회차마다 Running 만 돌아오고 아무도 모른다
        if (trigger == ReconTrigger.SCHEDULE_FINAL) {
            alerts.notifySafely(
                incompleteAlert(
                    date,
                    ReconFailure("RUNNING", "다른 실행이 아직 돌고 있다 — 곧 끝나면 완료 알림이 따로 온다. 오지 않으면 STARTED 로 남은 실행을 확인"),
                ),
            )
        }
        return ReconRun.Running(date)
    }

    private fun incompleteAlert(date: LocalDate, failure: ReconFailure) = Alert(
        AlertLevel.CRITICAL, "일마감 대사 $date 미완료 — 마지막 회차까지 끝내지 못함",
        listOf("이유: ${failure.code} ${failure.message}", "확인 뒤 POST /admin/reconciliations?date=$date 로 다시 실행 (OPERATION §9)"),
    )

    private fun failed(date: LocalDate, failure: ReconFailure, trigger: ReconTrigger): ReconRun {
        when {
            trigger == ReconTrigger.SCHEDULE_FINAL -> alerts.notifySafely(incompleteAlert(date, failure))
            failure.code == DailyRecon.FILE_NOT_READY ->
                log.info("recon file for {} is not ready yet ({}), the next round retries", date, trigger)
            else -> alerts.notifySafely(
                Alert(
                    AlertLevel.CRITICAL, "일마감 대사 $date 실패",
                    listOf("이유: ${failure.code} ${failure.message}", "멈춘 단계부터 다시: 다음 정각 회차 또는 POST /admin/reconciliations?date=$date"),
                ),
            )
        }
        log.warn("daily reconciliation {} failed ({}): {} {}", date, trigger, failure.code, failure.message)
        return ReconRun.Failed(date, failure)
    }
}

/**
 * 대사 잡의 동시 실행 가드. 나중 실행은 [ReconRunningException] 으로 멈춘다.
 * 이름이 클래스처럼 대문자인 이유: 일반화하기 전의 호출(`SingleRunGuard(jobRepository)` — 잡 구성 · DailyReconLauncherTest)을 그대로 두려고.
 */
fun SingleRunGuard(jobRepository: JobRepository) =
    DatedJobSingleRunGuard(jobRepository, DailyRecon.RECON_DATE, ::ReconRunningException)
```

- [ ] **Step 4: 대사 테스트를 다시 돌린다 — 테스트 파일은 고치지 않는다**

Run: `./gradlew :apps:api:test --tests '*DailyReconLauncherTest*' --tests '*DailyReconJobTest*' --tests '*DailyReconSchedulerTest*' --tests '*ReconciliationApiTest*' --tests '*StatementSettlerIsolationTest*'`
Expected: BUILD SUCCESSFUL

Run: `git status --short apps/api/src/test`
Expected: 출력 없음

바뀌는 관찰 가능한 것은 재시도 INFO 로그의 로거(`DatedJobLauncher`)와 문구(`dailyReconciliationJob instance … has no execution yet, retrying once`)뿐이다. 이 로그를 잡는 테스트 · 문서는 없다.

- [ ] **Step 5: 커밋**

```bash
git add apps/api/src/main/kotlin/io/github/hyeokjinon/banktransfer/api/batch/DatedJobLauncher.kt apps/api/src/main/kotlin/io/github/hyeokjinon/banktransfer/api/dailyrecon/DailyReconLauncher.kt
git commit -m "refactor(batch): 날짜별 잡의 시작 경합 처리를 DatedJobLauncher 로 뽑는다

보존 배치(P5-2)가 같은 경합 처리를 쓰는데 까다로워 복사하지 않는다. 대사의 실패 해석 · 알림은 DailyReconLauncher 에 남는다.
대사 동작은 바뀌지 않는다 — 테스트 파일을 고치지 않고 통과한다.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 2: 기준 시각 · 설정 — `RetentionPolicy` · `DataRetentionProperties`

**Files:**
- Create: `apps/api/src/main/kotlin/io/github/hyeokjinon/banktransfer/api/retention/DataRetention.kt`
- Modify: `apps/api/src/main/resources/application.yml` (`daily-recon:` 절 뒤에 추가)
- Test: `apps/api/src/test/kotlin/io/github/hyeokjinon/banktransfer/api/retention/RetentionPolicyTest.kt`

**Interfaces:**
- Consumes: Task 1 의 `DatedJobRunningException`
- Produces (패키지 `io.github.hyeokjinon.banktransfer.api.retention`):
  - `object DataRetention { const val JOB_NAME = "dataRetentionJob"; const val RETENTION_DATE = "retentionDate"; const val PURGE_OUTBOX = "purgeOutbox"; const val PURGE_VERIFICATIONS = "purgeVerifications"; fun parameters(date: LocalDate): JobParameters }`
  - `object RetentionPolicy { fun cutoff(date: LocalDate, retention: Duration): Instant; fun requireNotFuture(date: LocalDate, now: Instant) }`
  - `data class DataRetentionProperties(outboxRetention: Duration = 7d, verificationRetention: Duration = 30d, batchSize: Int = 1000, schedule: Schedule = Schedule())`, `data class Schedule(enabled: Boolean = true, cron: String = "0 30 6 * * *", zone: String = "Asia/Seoul")`
  - `class RetentionRunningException(date: LocalDate) : DatedJobRunningException`, `class RetentionAlreadyCompletedException(date: LocalDate) : RuntimeException`, `class RetentionDateInFutureException(date: LocalDate, today: LocalDate) : RuntimeException`

결정(계획): 설정 하한은 `init` 의 `require` — 바인딩 실패 = 기동 실패.

- [ ] **Step 1: 실패하는 테스트**

`retention/RetentionPolicyTest.kt`:
```kotlin
package io.github.hyeokjinon.banktransfer.api.retention

import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.boot.context.properties.bind.Binder
import org.springframework.boot.context.properties.source.ConfigurationPropertySources
import org.springframework.boot.env.YamlPropertySourceLoader
import org.springframework.boot.test.context.runner.ApplicationContextRunner
import org.springframework.context.annotation.Configuration
import org.springframework.core.io.ClassPathResource
import org.springframework.scheduling.support.CronExpression
import java.time.Duration
import java.time.Instant
import java.time.LocalDate
import java.time.LocalTime
import java.time.ZoneId
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull
import kotlin.test.assertNull

/** 기준 시각 · 받는 날짜 · 설정 하한 (P5-2, 설계 §4 · §5 · §6). 날짜로만 정해지는 순수 함수라 재시작해도 기준이 같다. */
class RetentionPolicyTest {

    private val seoul = ZoneId.of("Asia/Seoul")
    private val today = LocalDate.of(2026, 11, 2)

    private fun kst(date: LocalDate, hour: Int, minute: Int = 0): Instant = date.atTime(hour, minute).atZone(seoul).toInstant()

    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(DataRetentionProperties::class)
    class PropertiesOnly

    private val runner = ApplicationContextRunner().withUserConfiguration(PropertiesOnly::class.java)

    @Test
    fun `기준 시각 = 그 날짜 00시(KST) − 보관 기간`() {
        assertEquals(Instant.parse("2026-10-25T15:00:00Z"), RetentionPolicy.cutoff(today, Duration.ofDays(7)))
        assertEquals(Instant.parse("2026-10-02T15:00:00Z"), RetentionPolicy.cutoff(today, Duration.ofDays(30)))
    }

    @Test
    fun `내일 이후는 거부, 오늘과 지난 날은 받는다 - 내일을 받으면 기준이 앞당겨져 정책보다 일찍 지운다`() {
        val noon = kst(today, 12)

        RetentionPolicy.requireNotFuture(today, noon)
        RetentionPolicy.requireNotFuture(today.minusDays(400), noon)

        val tomorrow = assertFailsWith<RetentionDateInFutureException> { RetentionPolicy.requireNotFuture(today.plusDays(1), noon) }
        assertEquals("retention date 2026-11-03 is in the future (today is 2026-11-02, KST)", tomorrow.message)
    }

    @Test
    fun `오늘은 KST 로 정한다 - KST 00시 30분(UTC 전날 15시 30분)이면 KST 오늘을 받고 KST 내일은 거부`() {
        val justAfterMidnight = kst(today, 0, 30)
        assertEquals(Instant.parse("2026-11-01T15:30:00Z"), justAfterMidnight)

        RetentionPolicy.requireNotFuture(today, justAfterMidnight)   // UTC 날짜(11-01)로 보면 내일이라 거부하게 된다
        assertFailsWith<RetentionDateInFutureException> { RetentionPolicy.requireNotFuture(today.plusDays(1), justAfterMidnight) }
    }

    @Test
    fun `설정 하한 - 보관 기간은 1일 이상, 묶음은 1 ~ 10000`() {
        DataRetentionProperties(outboxRetention = Duration.ofDays(1), verificationRetention = Duration.ofDays(1))
        DataRetentionProperties(batchSize = 1)
        DataRetentionProperties(batchSize = 10_000)

        assertFailsWith<IllegalArgumentException> { DataRetentionProperties(outboxRetention = Duration.ZERO) }
        assertFailsWith<IllegalArgumentException> { DataRetentionProperties(outboxRetention = Duration.ofHours(23)) }
        assertFailsWith<IllegalArgumentException> { DataRetentionProperties(verificationRetention = Duration.ofHours(23)) }
        assertFailsWith<IllegalArgumentException> { DataRetentionProperties(batchSize = 0) }
        assertFailsWith<IllegalArgumentException> { DataRetentionProperties(batchSize = 10_001) }
    }

    @Test
    fun `잘못된 설정이면 기동에 실패한다 - 0d 같은 실수로 방금 발행한 행을 지우지 않게`() {
        for (bad in listOf(
            "data-retention.outbox-retention=0d",
            "data-retention.verification-retention=12h",
            "data-retention.batch-size=0",
            "data-retention.batch-size=10001",
        )) {
            runner.withPropertyValues(bad).run { ctx -> assertNotNull(ctx.startupFailure, bad) }
        }
        runner.run { ctx -> assertNull(ctx.startupFailure) }
    }

    @Test
    fun `application yml 의 값이 바인딩된다 - 7일 · 30일 · 1000 · 켜짐 · 매일 06시 30분 KST`() {
        val yml = Binder(ConfigurationPropertySources.from(YamlPropertySourceLoader().load("application", ClassPathResource("application.yml"))))

        val props = yml.bind("data-retention", DataRetentionProperties::class.java).get()

        assertEquals(Duration.ofDays(7), props.outboxRetention)
        assertEquals(Duration.ofDays(30), props.verificationRetention)
        assertEquals(1000, props.batchSize)
        assertEquals(DataRetentionProperties.Schedule(enabled = true, cron = "0 30 6 * * *", zone = "Asia/Seoul"), props.schedule)
        val next = CronExpression.parse(props.schedule.cron).next(kst(today, 0).atZone(seoul))
        assertEquals(LocalTime.of(6, 30), next!!.toLocalTime())
    }
}
```

- [ ] **Step 2: 실패 확인**

Run: `./gradlew :apps:api:test --tests '*RetentionPolicyTest*'`
Expected: FAIL (컴파일: `Unresolved reference 'RetentionPolicy'`, `Unresolved reference 'DataRetentionProperties'`)

- [ ] **Step 3: 구현**

`retention/DataRetention.kt`:
```kotlin
package io.github.hyeokjinon.banktransfer.api.retention

import io.github.hyeokjinon.banktransfer.api.batch.DatedJobRunningException
import org.springframework.batch.core.job.parameters.JobParameters
import org.springframework.batch.core.job.parameters.JobParametersBuilder
import org.springframework.boot.context.properties.ConfigurationProperties
import java.time.Duration
import java.time.Instant
import java.time.LocalDate
import java.time.ZoneId

/** 보존 배치의 이름 · 상수 (P5-2). 운영 문서 · 테스트가 이름을 여기서 본다. */
object DataRetention {
    const val JOB_NAME = "dataRetentionJob"
    const val RETENTION_DATE = "retentionDate"

    const val PURGE_OUTBOX = "purgeOutbox"
    const val PURGE_VERIFICATIONS = "purgeVerifications"

    /** 날짜마다 잡 인스턴스 하나 (식별 파라미터 retentionDate). */
    fun parameters(date: LocalDate): JobParameters = JobParametersBuilder().addLocalDate(RETENTION_DATE, date).toJobParameters()
}

/** 무엇을 언제부터 지워도 되나 (P5-2, 설계 §4). 시계를 보지 않고 날짜로만 정해 재시작해도 기준이 같다. */
object RetentionPolicy {
    private val KST: ZoneId = ZoneId.of("Asia/Seoul")

    /** 이 시각보다 이전이면 지운다. [date] 00:00(KST) − [retention]. */
    fun cutoff(date: LocalDate, retention: Duration): Instant = date.atStartOfDay(KST).toInstant().minus(retention)

    /**
     * 수동 실행이 받는 날짜: 오늘과 지난 날(KST). 내일 이후를 받으면 기준 시각이 앞당겨져 정책보다 일찍 지우게 된다.
     * 지난 날은 기준이 더 과거라 지우는 범위가 좁아질 뿐이다.
     */
    fun requireNotFuture(date: LocalDate, now: Instant) {
        val today = LocalDate.ofInstant(now, KST)
        if (date.isAfter(today)) throw RetentionDateInFutureException(date, today)
    }
}

class RetentionRunningException(date: LocalDate) : DatedJobRunningException("data retention for $date is already running")

class RetentionAlreadyCompletedException(date: LocalDate) : RuntimeException("data retention for $date is already completed")

/** 내일 이후(KST)의 수동 실행 ([RetentionPolicy.requireNotFuture]). */
class RetentionDateInFutureException(date: LocalDate, today: LocalDate) :
    RuntimeException("retention date $date is in the future (today is $today, KST)")

/**
 * 보존 배치 설정 (P5-2, 설계 §5). 범위를 벗어나면 기동에 실패한다 — 0d 같은 실수로 방금 발행한 행이나 막 만료된 인증을 지우는 일을 막는다.
 * 7일 · 30일은 법이 아니라 이 프로젝트의 운영 판단이다 (database.md §4-9).
 */
@ConfigurationProperties("data-retention")
data class DataRetentionProperties(
    /** 발행 뒤 이만큼 지난 outbox_event 를 지운다. 브로커 기본 보존과 같아 그동안 Kafka 메시지와 대조할 수 있다. */
    val outboxRetention: Duration = Duration.ofDays(7),
    /** 만료(expires_at) 뒤 이만큼 지난 VERIFIED 가 아닌 account_verification 을 지운다. */
    val verificationRetention: Duration = Duration.ofDays(30),
    /** 트랜잭션 하나에 지우는 최대 행 수. */
    val batchSize: Int = 1000,
    val schedule: Schedule = Schedule(),
) {
    init {
        require(outboxRetention >= MIN_RETENTION) { "data-retention.outbox-retention must be at least $MIN_RETENTION: $outboxRetention" }
        require(verificationRetention >= MIN_RETENTION) { "data-retention.verification-retention must be at least $MIN_RETENTION: $verificationRetention" }
        require(batchSize in 1..MAX_BATCH_SIZE) { "data-retention.batch-size must be within 1..$MAX_BATCH_SIZE: $batchSize" }
    }

    data class Schedule(
        val enabled: Boolean = true,
        /** 매일 06:30 — 대사 마지막 회차(06:00) 뒤. 재시도 회차는 없다: 다음 날 실행이 밀린 행까지 지운다. */
        val cron: String = "0 30 6 * * *",
        val zone: String = "Asia/Seoul",
    )

    private companion object {
        val MIN_RETENTION: Duration = Duration.ofDays(1)
        const val MAX_BATCH_SIZE = 10_000
    }
}
```

`application.yml` — `alert:` 앞(`daily-recon:` 절 뒤)에 합친다 (파일을 먼저 읽고, 나머지는 그대로):
```yaml
data-retention:                # 보존 배치 (P5-2, ADR-0011). 일회성 데이터 두 종류만 지운다 — 금융 기록은 지우지 않는다
  outbox-retention: 7d         # 발행 뒤 이만큼 지난 outbox_event. 1일 미만이면 기동 실패
  verification-retention: 30d  # 만료 뒤 이만큼 지난 account_verification (VERIFIED 는 지우지 않는다). 1일 미만이면 기동 실패
  batch-size: 1000             # 트랜잭션 하나에 지우는 최대 행 수. 1 ~ 10000 밖이면 기동 실패
  schedule:
    enabled: true              # 부하 측정 실행 구성 · 테스트는 false
    cron: "0 30 6 * * *"       # 매일 06:30 오늘 날짜로 (대사 마지막 회차 06:00 뒤). 재시도 회차 없음 — 다음 날 실행이 밀린 행까지 지운다
    zone: Asia/Seoul

```

- [ ] **Step 4: 통과 확인**

Run: `./gradlew :apps:api:test --tests '*RetentionPolicyTest*'`
Expected: PASS (6개)

- [ ] **Step 5: 커밋**

```bash
git add apps/api/src/main/kotlin/io/github/hyeokjinon/banktransfer/api/retention/DataRetention.kt apps/api/src/main/resources/application.yml apps/api/src/test/kotlin/io/github/hyeokjinon/banktransfer/api/retention/RetentionPolicyTest.kt
git commit -m "feat(retention): 기준 시각 · 받는 날짜 · 설정 하한 — 잘못된 보관 기간이면 기동 실패

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 3: 지우는 조건 — `V5` 인덱스 · `RetentionRepository` (이 단계의 핵심 조건)

**Files:**
- Create: `apps/api/src/main/resources/db/migration/V5__data_retention.sql`
- Create: `apps/api/src/main/kotlin/io/github/hyeokjinon/banktransfer/api/retention/RetentionRepository.kt`
- Create: `apps/api/src/main/kotlin/io/github/hyeokjinon/banktransfer/api/retention/DataRetentionJobConfiguration.kt` (이 Task 는 저장소 빈만)
- Create: `apps/api/src/test/kotlin/io/github/hyeokjinon/banktransfer/api/retention/RetentionTestDates.kt`
- Create: `apps/api/src/test/kotlin/io/github/hyeokjinon/banktransfer/api/retention/RetentionTestSupport.kt`
- Test: `apps/api/src/test/kotlin/io/github/hyeokjinon/banktransfer/api/retention/RetentionRepositoryTest.kt`
- Modify: `apps/api/src/test/kotlin/io/github/hyeokjinon/banktransfer/api/DatabaseSetupTest.kt` (테스트 하나 추가)

**Interfaces:**
- Consumes: Task 2 의 `RetentionPolicy.cutoff`, `DataRetentionProperties`
- Produces:
  - `class RetentionRepository(jdbc: NamedParameterJdbcTemplate) { fun deleteOutboxEvents(cutoff: Instant, limit: Int): Int; fun deleteVerifications(cutoff: Instant, limit: Int): Int }`, 빈 `retentionRepository`
  - `@Configuration class DataRetentionJobConfiguration` (`@EnableConfigurationProperties(DataRetentionProperties::class)`)
  - 테스트: `object RetentionTestDates { fun next(): LocalDate }`
  - 테스트: `abstract class RetentionTestSupport : ApiIntegrationTest` — `protected` 필드 `jdbc: JdbcTemplate`, `retention: RetentionRepository`, `props: DataRetentionProperties`, `jobRepository`, `jobOperator`, `transactionManager`; 함수 `nextCutoff(): Instant`, `outboxCutoff(date): Instant`, `verificationCutoff(date): Instant`, `outboxRow(publishedAt: Instant?, createdAt: Instant = Instant.now()): String`(aggregate_id), `outboxRows(count: Int, publishedAt: Instant)`, `outboxExists(aggregateId: String): Boolean`, `verificationRow(status: String, expiresAt: Instant): String`(id), `verificationRows(count: Int, status: String, expiresAt: Instant)`, `verificationExists(id: String): Boolean`

결정(계획): 공유 DB 규칙 — 테스트 날짜는 `RetentionTestDates.next()` 로만 ("스펙과 다른 점" 3), 심은 미발행 행은 `@AfterEach` 가 지운다 (`OutboxRelayTest.pendingCount` 가 표 전체를 센다).

- [ ] **Step 1: 테스트 지원 파일**

`retention/RetentionTestDates.kt`:
```kotlin
package io.github.hyeokjinon.banktransfer.api.retention

import java.time.LocalDate
import java.util.concurrent.atomic.AtomicLong

/**
 * 공유 DB 의 보존 배치 테스트는 이 날짜만 쓴다 (P5-2). 삭제 조건은 표 전체에 걸리므로 날짜가 앞뒤로 섞이면 다른 테스트의 행까지 지운다:
 * - 먼 미래(ReconTestDates)면 기준 시각이 미래라 발행된 행을 다 지우고, 관리 API 도 400 으로 거른다
 * - 오늘이면 JpaVerificationRepositoryTest 가 남긴 SENT(2026-10-19 만료)까지 지워 건수가 흔들린다
 * 그래서 2000-01-01 에서 과거로만 간다: 앞 테스트가 남긴 행은 모두 다음 테스트의 기준 시각보다 뒤다.
 * 간격 40일 > 인증 보관 30일 + 심는 여유 1일.
 */
object RetentionTestDates {
    private const val STEP_DAYS = 40L
    private val calls = AtomicLong()

    fun next(): LocalDate = LocalDate.of(2000, 1, 1).minusDays(STEP_DAYS * calls.getAndIncrement())
}
```

`retention/RetentionTestSupport.kt`:
```kotlin
package io.github.hyeokjinon.banktransfer.api.retention

import io.github.hyeokjinon.banktransfer.api.support.ApiIntegrationTest
import org.junit.jupiter.api.AfterEach
import org.springframework.batch.core.launch.JobOperator
import org.springframework.batch.core.repository.JobRepository
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.transaction.PlatformTransactionManager
import java.sql.Timestamp
import java.time.Duration
import java.time.Instant
import java.time.LocalDate
import java.util.UUID

/**
 * 보존 배치 테스트 공용 (P5-2, 공유 DB). 날짜는 [RetentionTestDates] 로만 고른다.
 * 심은 미발행 행은 테스트 끝에 지운다 — 다른 테스트의 발행기가 집어 가거나 OutboxRelayTest 의 미발행 건수에 섞이지 않게.
 * 시각은 앱과 같이 Timestamp(Instant) 로 넘긴다 (KST 변환은 JDBC 설정, database.md §2).
 */
abstract class RetentionTestSupport : ApiIntegrationTest() {

    @Autowired protected lateinit var jdbc: JdbcTemplate
    @Autowired protected lateinit var retention: RetentionRepository
    @Autowired protected lateinit var props: DataRetentionProperties
    @Autowired protected lateinit var jobRepository: JobRepository
    @Autowired protected lateinit var jobOperator: JobOperator
    @Autowired protected lateinit var transactionManager: PlatformTransactionManager

    private val unpublishedSeeds = mutableListOf<String>()

    @AfterEach
    fun removeUnpublishedSeeds() {
        unpublishedSeeds.forEach { jdbc.update("DELETE FROM outbox_event WHERE aggregate_id = ?", it) }
        unpublishedSeeds.clear()
    }

    /** 이 테스트만의 기준 시각 — 저장소를 직접 부르는 테스트용. */
    protected fun nextCutoff(): Instant = RetentionPolicy.cutoff(RetentionTestDates.next(), Duration.ZERO)

    protected fun outboxCutoff(date: LocalDate): Instant = RetentionPolicy.cutoff(date, props.outboxRetention)

    protected fun verificationCutoff(date: LocalDate): Instant = RetentionPolicy.cutoff(date, props.verificationRetention)

    /** 아웃박스 한 행을 심어 aggregate_id 를 돌려준다. [publishedAt] 이 null 이면 미발행. */
    protected fun outboxRow(publishedAt: Instant?, createdAt: Instant = Instant.now()): String {
        val aggregateId = UUID.randomUUID().toString()
        jdbc.update(
            "INSERT INTO outbox_event (aggregate_id, event_type, payload, created_at, published_at) VALUES (?, 'TransferRequested', '{}', ?, ?)",
            aggregateId, Timestamp.from(createdAt), publishedAt?.let { Timestamp.from(it) },
        )
        if (publishedAt == null) unpublishedSeeds += aggregateId
        return aggregateId
    }

    protected fun outboxRows(count: Int, publishedAt: Instant) {
        val at = Timestamp.from(publishedAt)
        jdbc.batchUpdate(
            "INSERT INTO outbox_event (aggregate_id, event_type, payload, created_at, published_at) VALUES (?, 'TransferRequested', '{}', ?, ?)",
            List(count) { arrayOf<Any>(UUID.randomUUID().toString(), at, at) },
        )
    }

    protected fun outboxExists(aggregateId: String): Boolean =
        jdbc.queryForObject("SELECT COUNT(*) FROM outbox_event WHERE aggregate_id = ?", Int::class.java, aggregateId) == 1

    /** 1원인증 한 행을 심어 id 를 돌려준다. 계좌는 모의 은행에 없는 번호. */
    protected fun verificationRow(status: String, expiresAt: Instant): String {
        val id = UUID.randomUUID().toString()
        jdbc.update(VERIFICATION_INSERT, id, newUserId(), status, Timestamp.from(expiresAt))
        return id
    }

    protected fun verificationRows(count: Int, status: String, expiresAt: Instant) {
        val at = Timestamp.from(expiresAt)
        jdbc.batchUpdate(VERIFICATION_INSERT, List(count) { arrayOf<Any>(UUID.randomUUID().toString(), newUserId(), status, at) })
    }

    protected fun verificationExists(id: String): Boolean =
        jdbc.queryForObject("SELECT COUNT(*) FROM account_verification WHERE id = ?", Int::class.java, id) == 1

    private companion object {
        const val VERIFICATION_INSERT =
            "INSERT INTO account_verification (id, user_id, bank_code, account_no, holder_name, code_hash, status, failed_attempts, expires_at, created_at) " +
                "VALUES (?, ?, '090', '5500000000000', '홍길동', REPEAT('0', 64), ?, 0, ?, NOW(6))"
    }
}
```

- [ ] **Step 2: 실패하는 테스트**

`retention/RetentionRepositoryTest.kt`:
```kotlin
package io.github.hyeokjinon.banktransfer.api.retention

import org.springframework.transaction.TransactionDefinition
import org.springframework.transaction.support.TransactionTemplate
import java.time.Duration
import java.time.Instant
import java.time.temporal.ChronoUnit
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/** 지우는 조건 (P5-2, 설계 §3 · §6). 되돌릴 수 없는 삭제라 조건을 줄마다 고정한다. 실제 MySQL. */
class RetentionRepositoryTest : RetentionTestSupport() {

    private val anyLimit = 10_000

    @Test
    fun `아웃박스 - 기준 전에 발행된 행만 지운다, 기준 −1µs 는 삭제 · 기준 그대로는 유지`() {
        val cutoff = nextCutoff()
        val justBefore = outboxRow(cutoff.minus(1, ChronoUnit.MICROS))
        val exactly = outboxRow(cutoff)

        assertEquals(1, retention.deleteOutboxEvents(cutoff, anyLimit))

        assertFalse(outboxExists(justBefore))
        assertTrue(outboxExists(exactly))
    }

    @Test
    fun `아웃박스 - 미발행은 아무리 오래돼도 남는다 (NULL 은 비교에 걸리지 않는다)`() {
        val cutoff = nextCutoff()
        val unpublished = outboxRow(publishedAt = null, createdAt = cutoff.minus(Duration.ofDays(365)))

        assertEquals(0, retention.deleteOutboxEvents(cutoff, anyLimit))

        assertTrue(outboxExists(unpublished))
    }

    @Test
    fun `아웃박스 - 한 번에 limit 행까지만 지운다`() {
        val cutoff = nextCutoff()
        outboxRows(3, cutoff.minusSeconds(1))

        assertEquals(2, retention.deleteOutboxEvents(cutoff, 2))
        assertEquals(1, retention.deleteOutboxEvents(cutoff, 2))
        assertEquals(0, retention.deleteOutboxEvents(cutoff, 2))
    }

    @Test
    fun `NOW(6) 로 쓴 published_at 이 KST 로 비교된다 - 1시간 전 기준이면 남고 1시간 뒤 기준이면 지워진다 (테스트 JVM 은 UTC)`() {
        val row = outboxRow(publishedAt = null)
        jdbc.update("UPDATE outbox_event SET published_at = NOW(6) WHERE aggregate_id = ?", row)
        val now = Instant.now()

        // 실제 시각 기준 삭제는 공유 DB 의 다른 테스트 행까지 건다 → 한 트랜잭션 안에서 보고 되돌린다
        TransactionTemplate(transactionManager).apply { isolationLevel = TransactionDefinition.ISOLATION_READ_COMMITTED }.executeWithoutResult { status ->
            status.setRollbackOnly()

            // 바인딩이 KST 로 어긋나 9시간 뒤로 밀리면 여기서 지워진다
            retention.deleteOutboxEvents(now.minus(Duration.ofHours(1)), 1_000_000)
            assertTrue(outboxExists(row))

            // UTC 벽시계로 바인딩되면 기준이 9시간 앞이라 여기서 남는다
            retention.deleteOutboxEvents(now.plus(Duration.ofHours(1)), 1_000_000)
            assertFalse(outboxExists(row))
        }

        assertTrue(outboxExists(row))
    }

    @Test
    fun `1원인증 - SENT · EXPIRED · LOCKED 는 기준 −1µs 면 지우고 기준 그대로면 남긴다`() {
        val cutoff = nextCutoff()
        val justBefore = cutoff.minus(1, ChronoUnit.MICROS)
        for (status in listOf("SENT", "EXPIRED", "LOCKED")) {
            val old = verificationRow(status, justBefore)
            val exactly = verificationRow(status, cutoff)

            assertEquals(1, retention.deleteVerifications(cutoff, anyLimit), status)

            assertFalse(verificationExists(old), status)
            assertTrue(verificationExists(exactly), status)
        }
    }

    @Test
    fun `1원인증 - 코드를 입력하지 않고 시간만 지나 SENT 로 남은 행도 지운다 (EXPIRED 만 지우면 영원히 남는다)`() {
        val cutoff = nextCutoff()
        val neverConfirmed = verificationRow("SENT", cutoff.minus(Duration.ofDays(1)))

        assertEquals(1, retention.deleteVerifications(cutoff, anyLimit))

        assertFalse(verificationExists(neverConfirmed))
    }

    @Test
    fun `1원인증 - VERIFIED 는 expires_at 이 아무리 지나도 지우지 않는다 (출금 인증의 근거)`() {
        val cutoff = nextCutoff()
        val verified = verificationRow("VERIFIED", cutoff.minus(Duration.ofDays(365)))

        assertEquals(0, retention.deleteVerifications(cutoff, anyLimit))

        assertTrue(verificationExists(verified))
    }

    @Test
    fun `1원인증 - 한 번에 limit 행까지만 지운다`() {
        val cutoff = nextCutoff()
        verificationRows(3, "SENT", cutoff.minusSeconds(1))

        assertEquals(2, retention.deleteVerifications(cutoff, 2))
        assertEquals(1, retention.deleteVerifications(cutoff, 2))
    }
}
```

`DatabaseSetupTest.kt` — 클래스 안 마지막에 추가:
```kotlin
    @Test
    fun `V5 가 보존 배치 삭제용 인덱스를 만들었다 (status, expires_at)`() {
        val columns = jdbc.queryForList(
            "SELECT column_name FROM information_schema.statistics WHERE table_schema = DATABASE() " +
                "AND table_name = 'account_verification' AND index_name = 'idx_verification_purge' ORDER BY seq_in_index",
            String::class.java,
        )
        assertEquals(listOf("status", "expires_at"), columns)
    }
```

- [ ] **Step 3: 실패 확인**

Run: `./gradlew :apps:api:test --tests '*RetentionRepositoryTest*' --tests '*DatabaseSetupTest*'`
Expected: FAIL (컴파일: `Unresolved reference 'RetentionRepository'`)

- [ ] **Step 4: 구현**

`db/migration/V5__data_retention.sql`:
```sql
-- P5-2 보존 배치. 정의와 이유는 docs/database.md §4-1 · §4-9 와 ADR-0011. V1 ~ V4 는 고치지 않는다.
-- 없으면 삭제 묶음마다 표 전체를 훑는다. 아웃박스는 기존 idx_outbox_unpublished (published_at, id) 를 탄다.
ALTER TABLE account_verification
  ADD INDEX idx_verification_purge (status, expires_at);
```

`retention/RetentionRepository.kt`:
```kotlin
package io.github.hyeokjinon.banktransfer.api.retention

import org.springframework.jdbc.core.namedparam.MapSqlParameterSource
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import java.sql.Timestamp
import java.time.Instant

/**
 * 보존 배치의 삭제 SQL (P5-2, 설계 §3). 되돌릴 수 없으므로 조건을 이 두 문장에만 둔다.
 * 시각은 Timestamp(Instant) 로 넘기고 KST 변환은 JDBC 설정이 한다 (database.md §2). 부르는 쪽(단계)의 트랜잭션에 합류한다.
 * ORDER BY 는 두지 않는다: 어느 행부터 지워도 결과가 같고, MySQL 8 기본 binlog_format=ROW 라 LIMIT 의 비결정성이 복제에 문제가 되지 않는다.
 */
class RetentionRepository(private val jdbc: NamedParameterJdbcTemplate) {

    /** 발행된 지 오래된 아웃박스. 미발행(published_at IS NULL)은 NULL 이라 `<` 비교에 걸리지 않아 아무리 오래돼도 남는다. */
    fun deleteOutboxEvents(cutoff: Instant, limit: Int): Int =
        jdbc.update("DELETE FROM outbox_event WHERE published_at < :cutoff LIMIT :limit", params(cutoff, limit))

    /**
     * 끝나지 않고 만료된 1원인증. **VERIFIED 는 절대 지우지 않는다** — expires_at 은 코드 입력 시한이라 VERIFIED 도 지나 있고,
     * 출금 접수는 existsVerified(status = VERIFIED 행이 있는가)만 본다. expires_at 만으로 지우면 인증한 사용자 전부의 출금이 422 다.
     * SENT 도 지운다: 코드를 입력하지 않고 시간만 지난 건은 상태가 SENT 그대로다 (EXPIRED 는 만료 뒤 확인을 시도했을 때만 된다).
     * 상태를 "VERIFIED 가 아닌 것" 이 아니라 목록으로 적는 이유: 나중에 생길 새 상태를 묻지 않고 지우지 않게.
     */
    fun deleteVerifications(cutoff: Instant, limit: Int): Int =
        jdbc.update(
            "DELETE FROM account_verification WHERE status IN ('SENT', 'EXPIRED', 'LOCKED') AND expires_at < :cutoff LIMIT :limit",
            params(cutoff, limit),
        )

    private fun params(cutoff: Instant, limit: Int) =
        MapSqlParameterSource().addValue("cutoff", Timestamp.from(cutoff)).addValue("limit", limit)
}
```

`retention/DataRetentionJobConfiguration.kt`:
```kotlin
package io.github.hyeokjinon.banktransfer.api.retention

import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate

/** 보존 배치 조립 (P5-2, 설계 §4). */
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(DataRetentionProperties::class)
class DataRetentionJobConfiguration {

    @Bean
    fun retentionRepository(jdbc: NamedParameterJdbcTemplate) = RetentionRepository(jdbc)
}
```

- [ ] **Step 5: 통과 확인**

Run: `./gradlew :apps:api:test --tests '*RetentionRepositoryTest*' --tests '*DatabaseSetupTest*'`
Expected: PASS

- [ ] **Step 6: 커밋**

```bash
git add apps/api/src/main/resources/db/migration/V5__data_retention.sql apps/api/src/main/kotlin/io/github/hyeokjinon/banktransfer/api/retention/RetentionRepository.kt apps/api/src/main/kotlin/io/github/hyeokjinon/banktransfer/api/retention/DataRetentionJobConfiguration.kt apps/api/src/test/kotlin/io/github/hyeokjinon/banktransfer/api/retention/RetentionTestDates.kt apps/api/src/test/kotlin/io/github/hyeokjinon/banktransfer/api/retention/RetentionTestSupport.kt apps/api/src/test/kotlin/io/github/hyeokjinon/banktransfer/api/retention/RetentionRepositoryTest.kt apps/api/src/test/kotlin/io/github/hyeokjinon/banktransfer/api/DatabaseSetupTest.kt
git commit -m "feat(retention): 지우는 조건 — 발행된 아웃박스 · VERIFIED 가 아닌 만료 인증만, V5 인덱스

VERIFIED 도 expires_at 이 지나 있어 expires_at 만으로 지우면 인증한 사용자 전부의 출금이 422 가 된다.
코드를 입력하지 않은 건은 SENT 그대로라 EXPIRED 만 지우면 영원히 남는다. 두 함정을 조건으로 막는다.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 4: 잡 `dataRetentionJob` — tasklet 두 단계 · READ COMMITTED · 핵심 테스트

**Files:**
- Create: `apps/api/src/main/kotlin/io/github/hyeokjinon/banktransfer/api/retention/PurgeTasklet.kt`
- Modify: `apps/api/src/main/kotlin/io/github/hyeokjinon/banktransfer/api/retention/DataRetentionJobConfiguration.kt`
- Modify: `apps/api/src/main/kotlin/io/github/hyeokjinon/banktransfer/api/dailyrecon/DailyReconJobConfiguration.kt:210-212`
- Modify: `apps/api/src/test/kotlin/io/github/hyeokjinon/banktransfer/api/dailyrecon/DailyReconTestSupport.kt:36`
- Test: `apps/api/src/test/kotlin/io/github/hyeokjinon/banktransfer/api/retention/DataRetentionJobTest.kt`

**Interfaces:**
- Consumes: Task 1 의 `DatedJobSingleRunGuard`, Task 2 의 `DataRetention.*` · `RetentionPolicy.cutoff` · `DataRetentionProperties` · `RetentionRunningException`, Task 3 의 `RetentionRepository` · `RetentionTestSupport` · `RetentionTestDates`
- Produces:
  - `typealias PurgeQuery = (cutoff: Instant, limit: Int) -> Int`
  - `class PurgeTasklet(retention: Duration, batchSize: Int, query: PurgeQuery) : Tasklet`
  - `internal fun buildDataRetentionJob(jobRepository: JobRepository, transactionManager: PlatformTransactionManager, props: DataRetentionProperties, purgeOutbox: PurgeQuery, purgeVerifications: PurgeQuery): Job`
  - 빈 `dataRetentionJob` (이름 = `DataRetention.JOB_NAME`). 주입은 `@Qualifier(DataRetention.JOB_NAME)`

**왜 다른 파일을 고치나:** `Job` 빈이 둘이 되면 이름 없는 `job: Job` 주입이 기동을 깨뜨린다 ("스펙과 다른 점" 1). `DailyReconLauncherTest.kt` · `DailyReconJobTest.kt` 는 고치지 않는다.

- [ ] **Step 1: 실패하는 테스트**

`retention/DataRetentionJobTest.kt`:
```kotlin
package io.github.hyeokjinon.banktransfer.api.retention

import io.github.hyeokjinon.banktransfer.account.AccountVerification
import io.github.hyeokjinon.banktransfer.account.VerificationRepository
import io.github.hyeokjinon.banktransfer.account.VerificationStatus
import io.github.hyeokjinon.banktransfer.api.web.USER_ID_HEADER
import org.springframework.batch.core.BatchStatus
import org.springframework.batch.core.job.Job
import org.springframework.batch.core.job.JobExecution
import org.springframework.batch.core.job.SimpleJob
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.http.MediaType
import org.springframework.orm.jpa.JpaTransactionManager
import org.springframework.test.util.ReflectionTestUtils
import org.springframework.transaction.TransactionDefinition
import org.springframework.transaction.interceptor.TransactionAttribute
import java.time.Duration
import java.time.Instant
import java.time.LocalDate
import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertNotNull
import kotlin.test.assertSame
import kotlin.test.assertTrue

/** 보존 배치 잡 (P5-2, 설계 §4 · §6). 실제 Spring Batch · Testcontainers MySQL. */
class DataRetentionJobTest : RetentionTestSupport() {

    @Autowired @Qualifier(DataRetention.JOB_NAME) private lateinit var job: Job
    @Autowired private lateinit var verifications: VerificationRepository

    private fun start(day: LocalDate, target: Job = job): JobExecution = jobOperator.start(target, DataRetention.parameters(day))

    private fun stepOf(execution: JobExecution, name: String) = checkNotNull(jobRepository.getLastStepExecution(execution.jobInstance, name)) { "step $name is missing" }

    private fun postTransfer(user: String, account: String): Int =
        client.post().uri("/api/v1/transfers")
            .contentType(MediaType.APPLICATION_JSON)
            .header(USER_ID_HEADER, user)
            .header("Idempotency-Key", UUID.randomUUID().toString())
            .body(mapOf("bankCode" to "090", "accountNo" to account, "amount" to 1_000, "memo" to "보존"))
            .exchange { _, response -> response.statusCode.value() }

    @Test
    fun `핵심 - expires_at 이 지난 VERIFIED 는 남고, 잡 뒤에도 그 사용자의 출금이 202 다`() {
        val day = RetentionTestDates.next()
        val user = fundedUser()
        val account = unregisteredAccount()   // RECEIVED 를 남긴다 — 나중에 처리돼도 공용 계좌에 입금이 생기지 않게
        val verified = verifications.save(
            AccountVerification(
                id = UUID.randomUUID(), userId = user, bankCode = "090", accountNo = account, holderName = "홍길동",
                codeHash = "0".repeat(64), status = VerificationStatus.VERIFIED, failedAttempts = 0,
                expiresAt = verificationCutoff(day).minus(Duration.ofDays(1)), verifiedAt = Instant.now(),
            ),
        )
        assertEquals(202, postTransfer(user, account))   // 잡 전에도 202 — 아래 202 가 다른 이유로 달라지지 않는지 먼저 본다

        assertEquals(BatchStatus.COMPLETED, start(day).status)

        assertNotNull(verifications.findById(verified.id))
        assertTrue(verifications.existsVerified(user, "090", account))
        assertEquals(202, postTransfer(user, account))
    }

    @Test
    fun `대상만 지운다 - 표마다 자기 보관 기간, 미발행 · 기준 뒤 · VERIFIED 는 남는다`() {
        val day = RetentionTestDates.next()
        val outboxCut = outboxCutoff(day)
        val verificationCut = verificationCutoff(day)
        val oldPublished = outboxRow(outboxCut.minusSeconds(1))
        val recentPublished = outboxRow(outboxCut)
        val oldUnpublished = outboxRow(publishedAt = null, createdAt = verificationCut.minus(Duration.ofDays(1)))
        val oldVerifications = listOf("SENT", "EXPIRED", "LOCKED").map { verificationRow(it, verificationCut.minusSeconds(1)) }
        val withinThirtyDays = verificationRow("SENT", outboxCut.minusSeconds(1))   // 7일은 지났지만 30일은 아니다 — 아웃박스 기간으로 지우면 여기서 잡힌다
        val oldVerified = verificationRow("VERIFIED", verificationCut.minus(Duration.ofDays(1)))

        val execution = start(day)

        assertEquals(BatchStatus.COMPLETED, execution.status)
        assertEquals(1L, stepOf(execution, DataRetention.PURGE_OUTBOX).writeCount)
        assertEquals(3L, stepOf(execution, DataRetention.PURGE_VERIFICATIONS).writeCount)
        assertFalse(outboxExists(oldPublished))
        assertTrue(outboxExists(recentPublished))
        assertTrue(outboxExists(oldUnpublished))
        oldVerifications.forEach { assertFalse(verificationExists(it)) }
        assertTrue(verificationExists(withinThirtyDays))
        assertTrue(verificationExists(oldVerified))
    }

    @Test
    fun `2,500행 + 묶음 1,000 → 3번 커밋 · 건수 2,500`() {
        val day = RetentionTestDates.next()
        assertEquals(1000, props.batchSize)                                  // 실제 잡 빈 · application.yml 값으로 잰다
        outboxRows(2_500, outboxCutoff(day).minusSeconds(1))

        val execution = start(day)

        val step = stepOf(execution, DataRetention.PURGE_OUTBOX)
        assertEquals(2_500L to 3L, step.writeCount to step.commitCount)
        val other = stepOf(execution, DataRetention.PURGE_VERIFICATIONS)
        assertEquals(0L to 1L, other.writeCount to other.commitCount)        // 지울 게 없어도 한 번은 부른다
    }

    @Test
    fun `지운 수가 묶음의 배수면 한 번 더 불러 0건으로 끝낸다`() {
        val day = RetentionTestDates.next()
        outboxRows(20, outboxCutoff(day).minusSeconds(1))
        val small = buildDataRetentionJob(
            jobRepository, transactionManager, props.copy(batchSize = 10), retention::deleteOutboxEvents, retention::deleteVerifications,
        )

        val execution = start(day, small)

        val step = stepOf(execution, DataRetention.PURGE_OUTBOX)
        assertEquals(20L to 3L, step.writeCount to step.commitCount)         // 10 · 10 · 0
    }

    @Test
    fun `단계 트랜잭션 - 두 단계 모두 JPA 트랜잭션 관리자 · READ COMMITTED · 모든 예외에 롤백`() {
        // Batch 6 은 트랜잭션 관리자를 빼먹어도 조용히 트랜잭션 없이 돈다 — 설정 자체를 고정한다 (계획 "버전 · API 확인 결과" 2 · 3 · 5). 필드 이름은 6.0.5 소스 기준
        assertIs<JpaTransactionManager>(transactionManager)
        for (name in listOf(DataRetention.PURGE_OUTBOX, DataRetention.PURGE_VERIFICATIONS)) {
            val step = checkNotNull((job as SimpleJob).getStep(name)) { "step $name is missing" }
            assertSame(transactionManager, ReflectionTestUtils.getField(step, "transactionManager"), name)
            val attribute = ReflectionTestUtils.getField(step, "transactionAttribute") as TransactionAttribute
            assertEquals(TransactionDefinition.ISOLATION_READ_COMMITTED, attribute.isolationLevel, name)
            // rollbackOn 이 false 면 검사 예외를 삼키고 CONTINUABLE 로 끝없이 돈다
            assertTrue(attribute.rollbackOn(Exception("checked")), name)
        }
    }
}
```

`DataRetentionJobTest` 는 `io.github.hyeokjinon.banktransfer.account.VerificationRepository` 의 `existsVerified` 를 부른다 (`ApiIntegrationTest` 와 같은 포트, `AccountVerification.kt:49`).

- [ ] **Step 2: 실패 확인**

Run: `./gradlew :apps:api:test --tests '*DataRetentionJobTest*'`
Expected: FAIL (컴파일: `Unresolved reference 'buildDataRetentionJob'`)

- [ ] **Step 3: 구현**

`retention/PurgeTasklet.kt`:
```kotlin
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
```

`retention/DataRetentionJobConfiguration.kt` 전체:
```kotlin
package io.github.hyeokjinon.banktransfer.api.retention

import io.github.hyeokjinon.banktransfer.api.batch.DatedJobSingleRunGuard
import org.springframework.batch.core.job.Job
import org.springframework.batch.core.job.builder.JobBuilder
import org.springframework.batch.core.job.parameters.DefaultJobParametersValidator
import org.springframework.batch.core.repository.JobRepository
import org.springframework.batch.core.step.builder.StepBuilder
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import org.springframework.transaction.PlatformTransactionManager
import org.springframework.transaction.TransactionDefinition
import org.springframework.transaction.interceptor.DefaultTransactionAttribute
import java.time.Duration

/**
 * 보존 배치 조립 (P5-2, 설계 §4). ① purgeOutbox ② purgeVerifications.
 * 잡 저장소(JDBC)가 날짜마다 인스턴스 하나를 두어 끝난 날짜는 거부하고, 실패하면 멈춘 단계부터 잇는다.
 */
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(DataRetentionProperties::class)
class DataRetentionJobConfiguration {

    @Bean
    fun retentionRepository(jdbc: NamedParameterJdbcTemplate) = RetentionRepository(jdbc)

    @Bean
    fun dataRetentionJob(
        jobRepository: JobRepository,
        transactionManager: PlatformTransactionManager,
        props: DataRetentionProperties,
        repo: RetentionRepository,
    ): Job = buildDataRetentionJob(jobRepository, transactionManager, props, repo::deleteOutboxEvents, repo::deleteVerifications)
}

/** 잡 모양. 삭제 함수를 받는 이유: 재시작 · 동시 실행 테스트가 같은 잡에 실패 · 멈춤을 끼운다 (행 락으로 만들면 묶음 순서가 옵티마이저에 달려 흔들린다). */
internal fun buildDataRetentionJob(
    jobRepository: JobRepository,
    transactionManager: PlatformTransactionManager,
    props: DataRetentionProperties,
    purgeOutbox: PurgeQuery,
    purgeVerifications: PurgeQuery,
): Job {
    fun purgeStep(name: String, retention: Duration, query: PurgeQuery) =
        StepBuilder(name, jobRepository)
            // 트랜잭션 관리자를 넘기지 않으면 Spring Batch 6 은 트랜잭션 없이 돈다 (P5-1 과 같은 함정)
            .tasklet(PurgeTasklet(retention, props.batchSize, query), transactionManager)
            .transactionAttribute(purgeTransaction())
            .build()

    return JobBuilder(DataRetention.JOB_NAME, jobRepository)
        .validator(DefaultJobParametersValidator(arrayOf(DataRetention.RETENTION_DATE), emptyArray()))
        // 실패한 날짜의 동시 재시작 — Spring Batch 6 의 시작 확인 사이의 틈을 막는다
        .listener(DatedJobSingleRunGuard(jobRepository, DataRetention.RETENTION_DATE, ::RetentionRunningException))
        .start(purgeStep(DataRetention.PURGE_OUTBOX, props.outboxRetention, purgeOutbox))
        .next(purgeStep(DataRetention.PURGE_VERIFICATIONS, props.verificationRetention, purgeVerifications))
        .build()
}

/**
 * READ COMMITTED: MySQL 기본 REPEATABLE READ 의 범위 삭제는 next-key 락으로 "마지막 미발행 행과 첫 발행 행 사이" 를 잠가,
 * 새 미발행 행(NULL, 가장 큰 id)을 넣는 출금 접수가 삭제 트랜잭션을 기다릴 수 있다. 발행기가 READ COMMITTED 인 이유와 같은 뿌리다 (MessagingConfiguration, 재현은 RetentionLockTest).
 *
 * rollbackOn 을 true 로 두는 이유: TaskletStep 의 기본 속성이 그렇다. DefaultTransactionAttribute 그대로면 검사 예외에 false 라
 * 단계가 그 예외를 삼키고 CONTINUABLE 로 같은 호출을 끝없이 돈다 (Spring Batch 6.0.5 TaskletStep.ChunkTransactionCallback).
 */
private fun purgeTransaction() = object : DefaultTransactionAttribute() {
    init {
        isolationLevel = TransactionDefinition.ISOLATION_READ_COMMITTED
    }

    override fun rollbackOn(ex: Throwable) = true
}
```

`RetentionLockTest`(Task 5)를 쓰기 전까지 주석의 "기다릴 수 있다" 는 가설이다. Task 5 에서 결과가 다르면 이 주석도 고친다.

`dailyrecon/DailyReconJobConfiguration.kt` — 실행기 빈의 `job` 에 이름을 준다 (import `org.springframework.beans.factory.annotation.Qualifier` 는 이미 있다):
```kotlin
    @Bean
    fun dailyReconLauncher(jobOperator: JobOperator, @Qualifier(JOB_NAME) job: Job, jobRepository: JobRepository, summaries: ReconSummaryReader, alerts: AlertNotifier) =
        DailyReconLauncher(jobOperator, job, jobRepository, summaries, alerts)
```

`dailyrecon/DailyReconTestSupport.kt:36` — 필드 하나 (import `org.springframework.beans.factory.annotation.Qualifier` 추가):
```kotlin
    @Autowired @Qualifier(DailyRecon.JOB_NAME) protected lateinit var job: Job
```

- [ ] **Step 4: 통과 확인 — 대사 테스트도 같이 (컨텍스트에 `Job` 이 둘이다)**

Run: `./gradlew :apps:api:test --tests '*DataRetentionJobTest*' --tests '*DailyReconLauncherTest*' --tests '*DailyReconJobTest*'`
Expected: PASS

Run: `git status --short apps/api/src/test/kotlin/io/github/hyeokjinon/banktransfer/api/dailyrecon/DailyReconLauncherTest.kt apps/api/src/test/kotlin/io/github/hyeokjinon/banktransfer/api/dailyrecon/DailyReconJobTest.kt`
Expected: 출력 없음

- [ ] **Step 5: 커밋**

```bash
git add apps/api/src/main/kotlin/io/github/hyeokjinon/banktransfer/api/retention/PurgeTasklet.kt apps/api/src/main/kotlin/io/github/hyeokjinon/banktransfer/api/retention/DataRetentionJobConfiguration.kt apps/api/src/main/kotlin/io/github/hyeokjinon/banktransfer/api/dailyrecon/DailyReconJobConfiguration.kt apps/api/src/test/kotlin/io/github/hyeokjinon/banktransfer/api/dailyrecon/DailyReconTestSupport.kt apps/api/src/test/kotlin/io/github/hyeokjinon/banktransfer/api/retention/DataRetentionJobTest.kt
git commit -m "feat(retention): dataRetentionJob — 표마다 tasklet 한 단계, READ COMMITTED 로 나눠 삭제

호출마다 DELETE … LIMIT n 한 문장 = 트랜잭션 하나. Job 빈이 둘이 되어 대사 쪽 주입 두 곳에 @Qualifier.
격리 수준만 바꾼 DefaultTransactionAttribute 는 검사 예외를 삼키고 끝없이 도니 rollbackOn 도 고정한다.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 5: 잠금 재현 — `RetentionLockTest` (READ COMMITTED 보장 + REPEATABLE READ 관찰)

**Files:**
- Test: `apps/api/src/test/kotlin/io/github/hyeokjinon/banktransfer/api/retention/RetentionLockTest.kt`
- Modify (관찰이 가설과 다를 때만): `retention/DataRetentionJobConfiguration.kt` 의 `purgeTransaction` 주석

**Interfaces:**
- Consumes: Task 3 의 `RetentionTestSupport`(`nextCutoff` · `outboxRow` · `outboxExists` · `retention` · `jdbc` · `transactionManager`), Task 4 의 빈 `dataRetentionJob` · `DataRetention.PURGE_OUTBOX`
- Produces: 관찰 결과 — 이 Task 커밋 본문의 `관찰:` 줄 두 개. Task 8 의 ADR-0011 이 그대로 옮긴다.

이 Task 의 두 테스트는 무게가 다르다:
- **READ COMMITTED 쪽은 보장이다.** 잡의 실제 단계에서 꺼낸 `transactionAttribute` 로 연다 — 잡 설정을 지우면 이 테스트가 깨진다 (Task 8 변이 3).
- **REPEATABLE READ 쪽은 가설을 재는 것이다.** 설계 §4 가 "가설 — 테스트로 확인" 이라고 적었다. 재 본 값을 표준 출력에 남기고, 처음에는 가설(막힌다)을 단언한다.

- [ ] **Step 1: 테스트 작성**

`retention/RetentionLockTest.kt`:
```kotlin
package io.github.hyeokjinon.banktransfer.api.retention

import org.springframework.batch.core.job.Job
import org.springframework.batch.core.job.SimpleJob
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.test.util.ReflectionTestUtils
import org.springframework.transaction.TransactionDefinition
import org.springframework.transaction.support.DefaultTransactionDefinition
import org.springframework.transaction.support.TransactionTemplate
import java.sql.SQLException
import java.util.UUID
import java.util.concurrent.CompletableFuture
import java.util.concurrent.TimeUnit
import javax.sql.DataSource
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * 왜 READ COMMITTED 인가 (P5-2, 설계 §4 · §6): 삭제 트랜잭션을 열어 둔 채 출금 접수의 아웃박스 INSERT 를 해 본다.
 * 미발행 행은 published_at 이 NULL 이라 인덱스 맨 앞에 모이고, 새 행 (NULL, 가장 큰 id) 이 들어갈 자리는 "마지막 NULL 행과 첫 발행 행 사이" 다.
 * StatementSettlerIsolationTest 처럼 두 격리 수준을 나란히 재현한다. 어느 쪽이든 2초 락 대기 + 20초 Future 로 끝이 있다.
 */
class RetentionLockTest : RetentionTestSupport() {

    @Autowired @Qualifier(DataRetention.JOB_NAME) private lateinit var job: Job
    @Autowired private lateinit var dataSource: DataSource

    private enum class InsertOutcome { INSERTED, LOCK_WAIT_TIMEOUT }

    /** 잡이 실제로 쓰는 트랜잭션 속성 — 잡 설정에서 격리 수준을 지우면 아래 READ COMMITTED 테스트가 깨진다 (변이 검사). */
    private fun stepTransaction(): TransactionDefinition {
        val step = checkNotNull((job as SimpleJob).getStep(DataRetention.PURGE_OUTBOX)) { "step is missing" }
        return ReflectionTestUtils.getField(step, "transactionAttribute") as TransactionDefinition
    }

    private fun repeatableRead(): TransactionDefinition =
        DefaultTransactionDefinition().apply { isolationLevel = TransactionDefinition.ISOLATION_REPEATABLE_READ }

    /** 다른 커넥션에서 미발행 행 하나를 넣는다 (autocommit). 락 대기는 [LOCK_WAIT_SECONDS] 까지 — 넘으면 1205. */
    private fun insertUnpublished(aggregateId: String): InsertOutcome =
        dataSource.connection.use { connection ->
            connection.createStatement().use { it.execute("SET SESSION innodb_lock_wait_timeout = $LOCK_WAIT_SECONDS") }
            try {
                connection.prepareStatement(
                    "INSERT INTO outbox_event (aggregate_id, event_type, payload, created_at) VALUES (?, 'TransferRequested', '{}', NOW(6))",
                ).use {
                    it.setString(1, aggregateId)
                    it.executeUpdate()
                }
                InsertOutcome.INSERTED
            } catch (e: SQLException) {
                if (e.errorCode != ER_LOCK_WAIT_TIMEOUT) throw e
                InsertOutcome.LOCK_WAIT_TIMEOUT
            } finally {
                // 풀로 돌아가는 커넥션이다 — connection-init-sql 값으로 되돌린다
                connection.createStatement().use { it.execute("SET SESSION innodb_lock_wait_timeout = $POOL_LOCK_WAIT_SECONDS") }
            }
        }

    /** [definition] 으로 삭제 트랜잭션을 연 채 INSERT 해 본다. 지운 행은 되돌린다 — 잠금만 본다. */
    private fun insertWhileDeleteIsOpen(definition: TransactionDefinition): InsertOutcome {
        val cutoff = nextCutoff()
        val published = outboxRow(cutoff.minusSeconds(60))
        outboxRow(publishedAt = null)   // 마지막 미발행 행 — 새 행은 이 행과 위 발행 행 사이로 들어간다
        val inserted = UUID.randomUUID().toString()
        try {
            val outcome = TransactionTemplate(transactionManager, definition).execute { status ->
                status.setRollbackOnly()
                assertEquals(1, retention.deleteOutboxEvents(cutoff, 1_000))
                CompletableFuture.supplyAsync { insertUnpublished(inserted) }.get(WAIT_BOUND_SECONDS, TimeUnit.SECONDS)
            }!!
            assertTrue(outboxExists(published))
            return outcome
        } finally {
            jdbc.update("DELETE FROM outbox_event WHERE aggregate_id = ?", inserted)
        }
    }

    private fun record(label: String, outcome: InsertOutcome) = println("[RetentionLockTest] observed: $label = $outcome")

    @Test
    fun `READ COMMITTED (잡의 실제 설정) - 삭제를 열어 둔 채 미발행 아웃박스 INSERT 가 들어간다`() {
        val outcome = insertWhileDeleteIsOpen(stepTransaction())

        record("READ COMMITTED", outcome)
        assertEquals(InsertOutcome.INSERTED, outcome)
    }

    @Test
    fun `REPEATABLE READ 였다면 next-key 락에 막혀 INSERT 가 락 대기로 끝난다 - 단계를 READ COMMITTED 로 두는 이유`() {
        val outcome = insertWhileDeleteIsOpen(repeatableRead())

        record("REPEATABLE READ", outcome)
        assertEquals(InsertOutcome.LOCK_WAIT_TIMEOUT, outcome)
    }

    private companion object {
        const val LOCK_WAIT_SECONDS = 2
        const val POOL_LOCK_WAIT_SECONDS = 5
        const val WAIT_BOUND_SECONDS = 20L
        const val ER_LOCK_WAIT_TIMEOUT = 1205
    }
}
```

- [ ] **Step 2: 돌린다**

Run: `./gradlew :apps:api:test --tests '*RetentionLockTest*'`
Expected (가설): PASS (2개). 어떤 결과든 끝난다 — 한 테스트 최대 약 22초.

Run: `grep -h '\[RetentionLockTest\] observed' apps/api/build/test-results/test/TEST-*RetentionLockTest.xml`
Expected (가설): `READ COMMITTED = INSERTED`, `REPEATABLE READ = LOCK_WAIT_TIMEOUT` 두 줄. 실패했으면 Gradle 리포트(`apps/api/build/reports/tests/test/`)에도 같은 줄이 있다.

- [ ] **Step 3: 결과가 가설과 다를 때 (같으면 건너뛴다)**

**REPEATABLE READ 가 막히지 않으면 (`REPEATABLE READ = INSERTED`)** — 가설이 틀린 것이다. 잠금 조건을 만들려고 애쓰지 않는다:
1. **READ COMMITTED 테스트와 잡 설정은 그대로 둔다.** 격리 수준을 낮추거나 올리지 않는다. 이 결정을 바꿀지는 사람이 정한다 (CLAUDE.md "설계는 사람이 정한다").
2. REPEATABLE READ 테스트를 **관찰한 대로** 고친다: 이름을 `` `REPEATABLE READ 에서도 이 조건에서는 INSERT 가 막히지 않았다 - 설계 §4 의 가설과 다름 (ADR-0011)` `` 로, 단언을 `assertEquals(InsertOutcome.INSERTED, outcome)` 으로. 관찰을 문서로 남기는 테스트이므로 단언을 지우지 않는다.
3. `purgeTransaction` 주석(`DataRetentionJobConfiguration.kt`)과 `RetentionLockTest` 클래스 주석의 "기다릴 수 있다" 를 관찰에 맞게 고친다. READ COMMITTED 를 유지하는 근거로 남는 것은 "짧은 트랜잭션 · 발행기와 같은 정책" 뿐임을 적는다.
4. 커밋 본문에 관찰을 적고, Task 보고에 **"가설과 다름 — 스펙 §4 근거를 사람이 다시 봐야 한다"** 를 눈에 띄게 적는다. Task 8 의 ADR-0011 에 그대로 쓴다.

**READ COMMITTED 가 막히면 (`READ COMMITTED = LOCK_WAIT_TIMEOUT`)** — 보장이 깨진 것이다. 고치려 들지 말고 멈춰 보고한다. 이후 Task 로 넘어가지 않는다.

**금지 (어느 경우든):** `@Disabled`, 단언 삭제, 두 결과를 다 받아 주는 단언(`assertTrue(outcome in …)`), 락 대기 시간 늘리기, 원하는 결과가 나오도록 시드 데이터 · 삭제 SQL · `LIMIT` 바꾸기, READ COMMITTED 쪽을 잡 설정 대신 직접 적은 격리 수준으로 바꾸기.

- [ ] **Step 4: 커밋 — 본문의 `관찰:` 두 줄은 Step 2 의 실제 값이다. 다르면 고쳐 적는다**

```bash
git add apps/api/src/test/kotlin/io/github/hyeokjinon/banktransfer/api/retention/RetentionLockTest.kt
# Step 3 의 첫 번째 경우에는 DataRetentionJobConfiguration.kt 도 함께 add 한다
git commit -m "test(retention): 삭제 트랜잭션을 열어 둔 채 아웃박스 INSERT — 두 격리 수준을 나란히 재현

READ COMMITTED 쪽은 잡의 실제 단계 속성으로 연다 (보장). REPEATABLE READ 쪽은 설계 §4 의 가설을 잰다.

관찰: READ COMMITTED = INSERTED
관찰: REPEATABLE READ = LOCK_WAIT_TIMEOUT

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 6: 실행 — `DataRetentionLauncher`(건수 합 · 실패 알림 · 재시작 · 동시 실행) · 06:30 스케줄 · 풀 4

**Files:**
- Create: `apps/api/src/main/kotlin/io/github/hyeokjinon/banktransfer/api/retention/DataRetentionLauncher.kt`
- Create: `apps/api/src/main/kotlin/io/github/hyeokjinon/banktransfer/api/retention/DataRetentionScheduler.kt`
- Modify: `apps/api/src/main/kotlin/io/github/hyeokjinon/banktransfer/api/retention/DataRetentionJobConfiguration.kt` (빈 두 개)
- Modify: `apps/api/src/main/resources/application.yml:7` (풀 4)
- Modify: `apps/api/src/main/kotlin/io/github/hyeokjinon/banktransfer/api/dailyrecon/DailyReconScheduler.kt:18` · `.../messaging/MessagingConfiguration.kt:83` (주석의 스레드 수)
- Modify: `apps/api/src/test/kotlin/io/github/hyeokjinon/banktransfer/api/support/ApiIntegrationTest.kt` (`properties` 한 줄)
- Modify: `apps/api/src/test/kotlin/io/github/hyeokjinon/banktransfer/api/retention/RetentionTestSupport.kt` (실행 도우미)
- Test: `apps/api/src/test/kotlin/io/github/hyeokjinon/banktransfer/api/retention/DataRetentionLauncherTest.kt`
- Test: `apps/api/src/test/kotlin/io/github/hyeokjinon/banktransfer/api/retention/DataRetentionSchedulerTest.kt`

**Interfaces:**
- Consumes: Task 1 의 `DatedJobLauncher` · `DatedJobStart` · `failureCauses` · `RETRY_PAUSE_MS`, Task 2 의 `DataRetention.*` · `DataRetentionProperties` · `RetentionRunningException`, Task 4 의 `buildDataRetentionJob` · 빈 `dataRetentionJob`
- Produces:
  - `data class RetentionDeleted(val outboxEvents: Long, val verifications: Long)`
  - `data class RetentionFailure(val code: String, val message: String)`
  - `sealed interface RetentionRun { val date: LocalDate; data class Completed(date, deleted: RetentionDeleted); data class Failed(date, failure: RetentionFailure); data class AlreadyCompleted(date); data class Running(date) }`
  - `class DataRetentionLauncher(jobOperator, job, jobRepository, alerts, pauseBeforeRetry = …) { fun run(date: LocalDate): RetentionRun }`, 빈 `dataRetentionLauncher`
  - `class DataRetentionScheduler(launch: (LocalDate) -> RetentionRun, clock: Clock, zone: String, alerts: AlertNotifier) { fun run() }`, 빈 `dataRetentionScheduler` (`data-retention.schedule.enabled`)
  - 테스트 지원: `RetentionTestSupport` 에 `startedExecution(day): JobExecution`, `abandon(vararg executions: JobExecution)`, `executions(day): List<JobExecution>`

결정(계획): 실패 WARN 은 스케줄 · 수동 모두. `Running` · `AlreadyCompleted` 는 알리지 않는다. 실패 이유 `ERROR`(메시지 = 예외 클래스 이름) · `RESTART_REFUSED`(고정 문장).

- [ ] **Step 1: 테스트 지원에 실행 도우미 추가**

`retention/RetentionTestSupport.kt` — import 추가 (`org.springframework.batch.core.BatchStatus`, `org.springframework.batch.core.job.JobExecution`, `org.springframework.batch.infrastructure.item.ExecutionContext`, `java.time.LocalDateTime`), 함수 추가:
```kotlin
    /** [day] 의 STARTED 실행 — 다른 곳이 돌고 있거나, 도중에 JVM 이 죽어 남은 실행. */
    protected fun startedExecution(day: LocalDate): JobExecution {
        val params = DataRetention.parameters(day)
        val instance = jobRepository.getJobInstance(DataRetention.JOB_NAME, params) ?: jobRepository.createJobInstance(DataRetention.JOB_NAME, params)
        return jobRepository.createJobExecution(instance, params, ExecutionContext()).also {
            it.status = BatchStatus.STARTED
            it.setStartTime(LocalDateTime.now())
            jobRepository.update(it)
        }
    }

    /** 이 날짜는 다시 쓰지 않지만 '돌고 있는 실행' 으로 남기지 않는다. */
    protected fun abandon(vararg executions: JobExecution) = executions.forEach {
        it.status = BatchStatus.FAILED
        it.setEndTime(LocalDateTime.now())   // 게터는 @Nullable, 세터는 non-null 이라 Kotlin 속성 대입이 안 된다
        jobRepository.update(it)
    }

    protected fun executions(day: LocalDate): List<JobExecution> =
        jobRepository.getJobInstance(DataRetention.JOB_NAME, DataRetention.parameters(day))?.let { jobRepository.getJobExecutions(it) } ?: emptyList()
```

- [ ] **Step 2: 실패하는 테스트**

`retention/DataRetentionLauncherTest.kt`:
```kotlin
package io.github.hyeokjinon.banktransfer.api.retention

import io.github.hyeokjinon.banktransfer.api.alert.AlertLevel
import io.github.hyeokjinon.banktransfer.api.batch.failureCauses
import org.springframework.batch.core.BatchStatus
import org.springframework.batch.core.job.Job
import org.springframework.batch.infrastructure.item.ExecutionContext
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.dao.QueryTimeoutException
import java.time.LocalDate
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertTrue

/** 실행 결과 · 건수 합 · 재실행 거부 · 재시작 · 동시 실행 · 실패 알림 (P5-2, 설계 §4 · §6). */
class DataRetentionLauncherTest : RetentionTestSupport() {

    @Autowired private lateinit var launcher: DataRetentionLauncher
    @Autowired @Qualifier(DataRetention.JOB_NAME) private lateinit var job: Job

    private fun launcherOf(target: Job) = DataRetentionLauncher(jobOperator, target, jobRepository, recordedAlerts)

    private fun seed(day: LocalDate, outbox: Int, verifications: Int) {
        outboxRows(outbox, outboxCutoff(day).minusSeconds(1))
        verificationRows(verifications, "SENT", verificationCutoff(day).minusSeconds(1))
    }

    @Test
    fun `완료하면 표별 건수를 돌려주고 텔레그램은 보내지 않는다`() {
        val day = RetentionTestDates.next()
        seed(day, outbox = 3, verifications = 2)

        val run = launcher.run(day)

        assertEquals(RetentionDeleted(outboxEvents = 3, verifications = 2), assertIs<RetentionRun.Completed>(run).deleted)
        assertTrue(recordedAlerts.about(day).isEmpty())
    }

    @Test
    fun `끝난 날짜를 다시 돌리면 AlreadyCompleted - 실행이 늘지 않고 알리지 않는다`() {
        val day = RetentionTestDates.next()
        assertIs<RetentionRun.Completed>(launcher.run(day))

        assertIs<RetentionRun.AlreadyCompleted>(launcher.run(day))

        assertEquals(1, executions(day).size)
        assertTrue(recordedAlerts.about(day).isEmpty())
    }

    @Test
    fun `② 둘째 묶음에서 실패하면 WARN(예외 종류만), 재시작은 ② 부터 - 건수는 모든 실행의 합`() {
        val day = RetentionTestDates.next()
        seed(day, outbox = 5, verifications = 15)
        val calls = AtomicInteger()
        val failingOnce = buildDataRetentionJob(
            jobRepository, transactionManager, props.copy(batchSize = 10), retention::deleteOutboxEvents,
        ) { cutoff, limit ->
            if (calls.incrementAndGet() == 2) throw QueryTimeoutException("내부 메시지")
            retention.deleteVerifications(cutoff, limit)
        }
        val failing = launcherOf(failingOnce)

        val first = assertIs<RetentionRun.Failed>(failing.run(day)).failure

        assertEquals("ERROR" to QueryTimeoutException::class.java.name, first.code to first.message)
        val alert = recordedAlerts.about(day).single()
        assertEquals(AlertLevel.WARN, alert.level)
        assertEquals("보존 배치 $day 실패", alert.title)
        assertFalse(alert.lines.any { "내부 메시지" in it })                     // 내부 메시지는 로그에만

        val second = failing.run(day)

        // 첫 실행 ② 10건 + 재시작 ② 5건. 마지막 단계 실행만 보면 5 다
        assertEquals(RetentionDeleted(outboxEvents = 5, verifications = 15), assertIs<RetentionRun.Completed>(second).deleted)
        val runs = executions(day)
        assertEquals(2, runs.size)
        val restarted = runs.maxBy { it.id }
        assertEquals(listOf(DataRetention.PURGE_VERIFICATIONS), restarted.stepExecutions.map { it.stepName })   // ① 은 다시 돌지 않는다
    }

    @Test
    fun `동시 실행 - 같은 날짜를 두 곳에서 돌리면 하나만 실행되고 다른 하나는 Running, 알리지 않는다`() {
        val day = RetentionTestDates.next()
        val entered = CountDownLatch(1)
        val release = CountDownLatch(1)
        val held = launcherOf(
            buildDataRetentionJob(jobRepository, transactionManager, props, { cutoff, limit ->
                entered.countDown()
                check(release.await(20, TimeUnit.SECONDS)) { "not released" }
                retention.deleteOutboxEvents(cutoff, limit)
            }, retention::deleteVerifications),
        )

        val first = CompletableFuture.supplyAsync { held.run(day) }
        try {
            check(entered.await(20, TimeUnit.SECONDS)) { "the job did not reach its first step" }
            assertIs<RetentionRun.Running>(held.run(day))
        } finally {
            release.countDown()
        }

        assertIs<RetentionRun.Completed>(first.get(20, TimeUnit.SECONDS))
        assertEquals(1, executions(day).size)
        assertTrue(recordedAlerts.about(day).isEmpty())
    }

    @Test
    fun `실행이 동시에 둘 만들어지면(실패한 날짜의 동시 재시작) 나중 실행은 가드가 멈춘다 - 실제 잡 빈에 달려 있다`() {
        val day = RetentionTestDates.next()
        val earlier = startedExecution(day)
        val later = jobRepository.createJobExecution(earlier.jobInstance, DataRetention.parameters(day), ExecutionContext())
        try {
            job.execute(later)

            assertEquals(BatchStatus.FAILED, later.status)
            assertTrue(later.failureCauses().any { it is RetentionRunningException })
            assertTrue(later.stepExecutions.isEmpty())                       // 아무것도 지우지 않았다
        } finally {
            abandon(earlier)
        }
    }

    @Test
    fun `도중에 JVM 이 죽어 STARTED 로 남으면 그 날짜는 Running 이지만 다음 날 인스턴스는 막지 않는다`() {
        val day = RetentionTestDates.next()
        val stuck = startedExecution(day)
        try {
            assertIs<RetentionRun.Running>(launcher.run(day))
            assertTrue(recordedAlerts.about(day).isEmpty())

            assertIs<RetentionRun.Completed>(launcher.run(day.plusDays(1)))
        } finally {
            abandon(stuck)
        }
    }

    @Test
    fun `잡 인스턴스만 남아 있으면(처음 시작하다 죽은 곳) 예외 없이 Failed ERROR + WARN`() {
        val day = RetentionTestDates.next()
        jobRepository.createJobInstance(DataRetention.JOB_NAME, DataRetention.parameters(day))

        val failure = assertIs<RetentionRun.Failed>(launcher.run(day)).failure

        assertEquals("ERROR" to IllegalStateException::class.java.name, failure.code to failure.message)
        assertEquals(AlertLevel.WARN, recordedAlerts.about(day).single().level)
    }
}
```

`retention/DataRetentionSchedulerTest.kt`:
```kotlin
package io.github.hyeokjinon.banktransfer.api.retention

import io.github.hyeokjinon.banktransfer.api.alert.Alert
import io.github.hyeokjinon.banktransfer.api.alert.AlertLevel
import io.github.hyeokjinon.banktransfer.api.alert.AlertNotifier
import org.springframework.boot.context.properties.bind.Binder
import org.springframework.boot.context.properties.source.ConfigurationPropertySources
import org.springframework.boot.env.YamlPropertySourceLoader
import org.springframework.core.io.ClassPathResource
import java.time.Clock
import java.time.Instant
import java.time.LocalDate
import java.time.ZoneOffset
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/** 06:30 스케줄 (P5-2): 시계는 UTC, 날짜는 KST. 실행기는 함수로 바꿔 끼운다. */
class DataRetentionSchedulerTest {

    private val calls = mutableListOf<LocalDate>()
    private val alerts = mutableListOf<Alert>()

    private fun scheduler(nowUtc: String, run: (LocalDate) -> RetentionRun = { date -> calls += date; RetentionRun.AlreadyCompleted(date) }) =
        DataRetentionScheduler(run, Clock.fixed(Instant.parse(nowUtc), ZoneOffset.UTC), "Asia/Seoul", AlertNotifier { alerts += it })

    @Test
    fun `오늘(KST) 날짜로 실행한다 - KST 06시 30분은 UTC 로 아직 전날`() {
        scheduler("2026-11-01T21:30:00Z").run()   // KST 11-02 06:30

        assertEquals(listOf(LocalDate.of(2026, 11, 2)), calls)
        assertTrue(alerts.isEmpty())
    }

    @Test
    fun `실행이 예외를 던져도(시작조차 못 함) 스케줄러는 던지지 않고 WARN - 내부 메시지는 싣지 않는다`() {
        scheduler("2026-11-01T21:30:00Z") { _ -> error("db down") }.run()

        val alert = alerts.single()
        assertEquals(AlertLevel.WARN, alert.level)
        assertEquals("보존 배치 2026-11-02 실패", alert.title)
        assertTrue(alert.lines.any { IllegalStateException::class.java.name in it })
        assertFalse(alert.lines.any { "db down" in it })
    }

    @Test
    fun `실패 결과는 실행기가 이미 알렸으므로 다시 알리지 않는다`() {
        scheduler("2026-11-01T21:30:00Z") { date -> RetentionRun.Failed(date, RetentionFailure("ERROR", "x")) }.run()

        assertTrue(alerts.isEmpty())
    }

    @Test
    fun `스케줄 풀은 4 - 밀린 행이 많은 첫 실행이 길어져도 발행기 · 정리 작업 · 대사를 굶기지 않는다`() {
        val yml = Binder(ConfigurationPropertySources.from(YamlPropertySourceLoader().load("application", ClassPathResource("application.yml"))))

        assertEquals(4, yml.bind("spring.task.scheduling.pool.size", Int::class.javaObjectType).get())
    }
}
```

- [ ] **Step 3: 실패 확인**

Run: `./gradlew :apps:api:test --tests '*DataRetentionLauncherTest*' --tests '*DataRetentionSchedulerTest*'`
Expected: FAIL (컴파일: `Unresolved reference 'DataRetentionLauncher'`, `Unresolved reference 'DataRetentionScheduler'`)

- [ ] **Step 4: 구현**

`retention/DataRetentionLauncher.kt`:
```kotlin
package io.github.hyeokjinon.banktransfer.api.retention

import io.github.hyeokjinon.banktransfer.api.alert.Alert
import io.github.hyeokjinon.banktransfer.api.alert.AlertLevel
import io.github.hyeokjinon.banktransfer.api.alert.AlertNotifier
import io.github.hyeokjinon.banktransfer.api.alert.notifySafely
import io.github.hyeokjinon.banktransfer.api.batch.DatedJobLauncher
import io.github.hyeokjinon.banktransfer.api.batch.DatedJobStart
import io.github.hyeokjinon.banktransfer.api.batch.failureCauses
import org.slf4j.LoggerFactory
import org.springframework.batch.core.BatchStatus
import org.springframework.batch.core.job.Job
import org.springframework.batch.core.job.JobExecution
import org.springframework.batch.core.launch.JobOperator
import org.springframework.batch.core.repository.JobRepository
import java.time.LocalDate

/** 표별 삭제 건수. 이 날짜 인스턴스의 모든 실행을 더한 값이라 재시작해도 맞다. */
data class RetentionDeleted(val outboxEvents: Long, val verifications: Long)

/** 실패 이유. code = ERROR · RESTART_REFUSED. 예외 종류만 싣는다 (내부 메시지는 로그에). */
data class RetentionFailure(val code: String, val message: String)

/** 실행 한 번의 결과. */
sealed interface RetentionRun {
    val date: LocalDate

    data class Completed(override val date: LocalDate, val deleted: RetentionDeleted) : RetentionRun
    data class Failed(override val date: LocalDate, val failure: RetentionFailure) : RetentionRun
    data class AlreadyCompleted(override val date: LocalDate) : RetentionRun
    data class Running(override val date: LocalDate) : RetentionRun
}

/**
 * 날짜 하나의 보존 배치를 이 스레드에서 끝까지 돌린다 (P5-2, 설계 §4). 시작 경합 처리는 [DatedJobLauncher] 가 한다.
 * 실패: 멈춘 단계 · 지운 만큼 그대로 두고 WARN — 삭제가 하루 밀려도 급하지 않다. 자동 재시도는 없다: 다음 날 실행은 새 인스턴스이고 기준 시각이 뒤로 가 밀린 행까지 지운다.
 * 성공은 건수를 로그로만 남긴다 — 대사 요약이 이미 매일 간다.
 */
class DataRetentionLauncher(
    jobOperator: JobOperator,
    job: Job,
    private val jobRepository: JobRepository,
    private val alerts: AlertNotifier,
    pauseBeforeRetry: () -> Unit = { Thread.sleep(DatedJobLauncher.RETRY_PAUSE_MS) },
) {
    private val log = LoggerFactory.getLogger(javaClass)
    private val starter = DatedJobLauncher(jobOperator, job, jobRepository, DataRetention::parameters, pauseBeforeRetry)

    fun run(date: LocalDate): RetentionRun =
        when (val start = starter.start(date)) {
            is DatedJobStart.Ended -> outcomeOf(date, start.execution)
            DatedJobStart.AlreadyCompleted -> {
                log.info("data retention {} is already completed, skipped", date)
                RetentionRun.AlreadyCompleted(date)
            }
            DatedJobStart.Running -> {
                log.info("data retention {} is already running elsewhere, skipped", date)
                RetentionRun.Running(date)
            }
            is DatedJobStart.RestartRefused -> {
                log.error("data retention {} could not be restarted: {}", date, start.message)
                failed(date, RetentionFailure("RESTART_REFUSED", "지난 실행이 UNKNOWN 으로 끝나 재시작을 거부했다 (OPERATION §10)"))
            }
            is DatedJobStart.NotStarted -> {
                log.error("data retention {} could not be started", date, start.cause)
                failed(date, RetentionFailure("ERROR", start.cause.javaClass.name))
            }
        }

    private fun outcomeOf(date: LocalDate, execution: JobExecution): RetentionRun {
        if (execution.status == BatchStatus.COMPLETED) {
            val deleted = deletedOf(execution)
            log.info("data retention {} completed: {} outbox events, {} verifications deleted", date, deleted.outboxEvents, deleted.verifications)
            return RetentionRun.Completed(date, deleted)
        }
        val cause = execution.failureCauses().firstOrNull()
        log.error("data retention {} failed", date, cause)
        return failed(date, RetentionFailure("ERROR", cause?.javaClass?.name ?: execution.exitStatus.exitCode))
    }

    /** 재시작하면 멈춘 단계에 새 StepExecution 이 건수 0 으로 생긴다 → 같은 단계의 모든 실행을 더한다. getLastStepExecution 만 보면 모자란다. */
    private fun deletedOf(execution: JobExecution): RetentionDeleted {
        val steps = jobRepository.getJobExecutions(execution.jobInstance).flatMap { it.stepExecutions }
        fun written(step: String) = steps.filter { it.stepName == step }.sumOf { it.writeCount }
        return RetentionDeleted(written(DataRetention.PURGE_OUTBOX), written(DataRetention.PURGE_VERIFICATIONS))
    }

    private fun failed(date: LocalDate, failure: RetentionFailure): RetentionRun {
        alerts.notifySafely(
            Alert(
                AlertLevel.WARN, "보존 배치 $date 실패",
                listOf("이유: ${failure.code} ${failure.message}", "다음 날 실행이 밀린 행까지 지운다. 바로 돌리려면 POST /admin/retention-runs?date=$date (OPERATION §10)"),
            ),
        )
        log.warn("data retention {} failed: {} {}", date, failure.code, failure.message)
        return RetentionRun.Failed(date, failure)
    }
}
```

`retention/DataRetentionScheduler.kt`:
```kotlin
package io.github.hyeokjinon.banktransfer.api.retention

import io.github.hyeokjinon.banktransfer.api.alert.Alert
import io.github.hyeokjinon.banktransfer.api.alert.AlertLevel
import io.github.hyeokjinon.banktransfer.api.alert.AlertNotifier
import io.github.hyeokjinon.banktransfer.api.alert.notifySafely
import org.slf4j.LoggerFactory
import org.springframework.scheduling.annotation.Scheduled
import java.time.Clock
import java.time.LocalDate
import java.time.ZoneId

/**
 * 매일 06:30(KST)에 오늘 날짜로 보존 배치를 돌린다 (P5-2, 설계 §4). 대사 마지막 회차(06:00) 뒤. 재시도 회차는 없다.
 * 이 스케줄 스레드에서 동기로 돈다 (스케줄 풀 4 — 밀린 행이 많은 첫 실행이 길어져도 발행기 · 정리 작업 · 대사를 굶기지 않게). 테스트는 이 빈을 끄고 실행기를 직접 부른다.
 */
class DataRetentionScheduler(
    /** 실행기 (테스트는 함수로 바꿔 끼운다). */
    private val launch: (LocalDate) -> RetentionRun,
    private val clock: Clock,
    zone: String,
    private val alerts: AlertNotifier,
) {
    private val log = LoggerFactory.getLogger(javaClass)
    private val zone = ZoneId.of(zone)

    @Scheduled(cron = "\${data-retention.schedule.cron:0 30 6 * * *}", zone = "\${data-retention.schedule.zone:Asia/Seoul}")
    fun run() {
        val date = LocalDate.ofInstant(clock.instant(), zone)
        try {
            log.info("data retention round: {}", launch(date))
        } catch (e: Exception) {
            // 실행기는 실패를 결과로 돌려준다(알림도 거기서). 여기 오는 것은 시작조차 못 한 경우(DB 장애 등)다
            log.error("data retention {} could not be started", date, e)
            alerts.notifySafely(Alert(AlertLevel.WARN, "보존 배치 $date 실패", listOf("시작하지 못함: ${e.javaClass.name}")))
        }
    }
}
```

`retention/DataRetentionJobConfiguration.kt` — 클래스 안에 빈 두 개, import 추가 (`AlertNotifier`, `org.springframework.batch.core.launch.JobOperator`, `org.springframework.beans.factory.annotation.Qualifier`, `org.springframework.boot.autoconfigure.condition.ConditionalOnProperty`, `java.time.Clock`):
```kotlin
    @Bean
    fun dataRetentionLauncher(jobOperator: JobOperator, @Qualifier(DataRetention.JOB_NAME) job: Job, jobRepository: JobRepository, alerts: AlertNotifier) =
        DataRetentionLauncher(jobOperator, job, jobRepository, alerts)

    @Bean
    @ConditionalOnProperty(prefix = "data-retention.schedule", name = ["enabled"], havingValue = "true", matchIfMissing = true)
    fun dataRetentionScheduler(launcher: DataRetentionLauncher, clock: Clock, props: DataRetentionProperties, alerts: AlertNotifier) =
        DataRetentionScheduler(launcher::run, clock, props.schedule.zone, alerts)
```

`application.yml:7`:
```yaml
        size: 4   # 발행기(0.5초) · 정리 작업(10초) · 일마감 대사(01 ~ 06시) · 보존 배치(06:30, 밀린 행이 많으면 길다)가 서로 막지 않게 (P5-2 에서 3 → 4)
```

`DailyReconScheduler.kt:18` 의 `(스케줄 풀 3 — 발행기 · 정리 작업과 나눠 쓴다)` → `(스케줄 풀 4 — 발행기 · 정리 작업 · 보존 배치와 나눠 쓴다)`.

`MessagingConfiguration.kt:83` 의 `공용 스케줄러(스레드 2개)를 쓰는데` → `공용 스케줄러를 쓰는데` (이미 낡은 숫자다).

`support/ApiIntegrationTest.kt` — `"daily-recon.schedule.enabled=false",` 다음 줄에 (주석 포함):
```kotlin
        // 보존 배치 스케줄도 끈다 (P5-2): 켜 두면 06:30 에 진짜 날짜로 돌아 다른 테스트의 오래된 행을 지운다
        "data-retention.schedule.enabled=false",
```

- [ ] **Step 5: 통과 확인**

Run: `./gradlew :apps:api:test --tests '*DataRetentionLauncherTest*' --tests '*DataRetentionSchedulerTest*' --tests '*DataRetentionJobTest*' --tests '*DailyReconSchedulerTest*'`
Expected: PASS

"동시 실행" 과 가드 테스트는 Spring Batch 가 `Encountered fatal error executing job` ERROR 스택을 남긴다 (OPERATION §9-6 과 같은 정상 동작).

- [ ] **Step 6: 커밋**

```bash
git add apps/api/src/main/kotlin/io/github/hyeokjinon/banktransfer/api/retention apps/api/src/main/resources/application.yml apps/api/src/main/kotlin/io/github/hyeokjinon/banktransfer/api/dailyrecon/DailyReconScheduler.kt apps/api/src/main/kotlin/io/github/hyeokjinon/banktransfer/api/messaging/MessagingConfiguration.kt apps/api/src/test/kotlin/io/github/hyeokjinon/banktransfer/api/support/ApiIntegrationTest.kt apps/api/src/test/kotlin/io/github/hyeokjinon/banktransfer/api/retention
git commit -m "feat(retention): 실행기 · 06:30 스케줄 — 건수는 모든 실행의 합, 실패는 WARN, 스케줄 풀 4

재시작하면 멈춘 단계에 건수 0 인 새 단계 실행이 생긴다 → 마지막 단계 실행이 아니라 전부 더한다.
자동 재시도는 없다: 다음 날 실행은 새 인스턴스이고 기준 시각이 뒤로 가 밀린 행까지 지운다.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 7: 수동 실행 — `POST /admin/retention-runs`

**Files:**
- Create: `apps/api/src/main/kotlin/io/github/hyeokjinon/banktransfer/api/web/RetentionController.kt`
- Modify: `apps/api/src/main/kotlin/io/github/hyeokjinon/banktransfer/api/web/ApiExceptionHandler.kt`
- Test: `apps/api/src/test/kotlin/io/github/hyeokjinon/banktransfer/api/web/RetentionApiTest.kt`

**Interfaces:**
- Consumes: Task 2 의 `RetentionPolicy.requireNotFuture` · 예외 3종, Task 3 · 6 의 `RetentionTestSupport`(`startedExecution` · `abandon` · `outboxRows` · `verificationRows` · `outboxCutoff` · `verificationCutoff`), Task 6 의 `DataRetentionLauncher.run` · `RetentionRun` · `RetentionDeleted` · `RetentionFailure`
- Produces: `data class RetentionRunResponse(val date: LocalDate, val status: String, val reason: RetentionFailure?, val deleted: RetentionDeleted?)`, `POST /admin/retention-runs?date=yyyy-MM-dd`, 오류 코드 `RETENTION_ALREADY_COMPLETED` · `RETENTION_RUNNING` · `RETENTION_DATE_IN_FUTURE`

- [ ] **Step 1: 실패하는 테스트**

`web/RetentionApiTest.kt`:
```kotlin
package io.github.hyeokjinon.banktransfer.api.web

import io.github.hyeokjinon.banktransfer.api.retention.DataRetention
import io.github.hyeokjinon.banktransfer.api.retention.RetentionTestDates
import io.github.hyeokjinon.banktransfer.api.retention.RetentionTestSupport
import io.github.hyeokjinon.banktransfer.api.support.ApiResponse
import io.github.hyeokjinon.banktransfer.api.support.send
import java.time.LocalDate
import java.time.ZoneId
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/** 수동 실행 API (P5-2, 설계 §4): 200 (실패해도 200 + FAILED + 이유), 409 두 가지, 내일 이후 400. 인증은 범위 밖. */
class RetentionApiTest : RetentionTestSupport() {

    private val seoul = ZoneId.of("Asia/Seoul")

    private fun post(date: String?): ApiResponse =
        send(client.post().uri(if (date == null) "/admin/retention-runs" else "/admin/retention-runs?date=$date"))

    /** 앱 시계를 [today] 정오(KST)에 둔다. [RetentionTestDates] 는 먼 과거라 시계를 두지 않아도 지난 날이지만, 내일 · 오늘을 재려면 시계가 그 날이어야 한다. */
    private fun todayIs(today: LocalDate) = testClock.fix(today.atTime(12, 0).atZone(seoul).toInstant())

    @Suppress("UNCHECKED_CAST")
    private fun ApiResponse.deleted() = body["deleted"] as Map<String, Any?>

    @Suppress("UNCHECKED_CAST")
    private fun ApiResponse.reason() = body["reason"] as Map<String, Any?>

    @Test
    fun `완료하면 200 COMPLETED + 건수 (reason 은 null 로 있다), 다시 보내면 409 RETENTION_ALREADY_COMPLETED`() {
        val day = RetentionTestDates.next()
        outboxRows(2, outboxCutoff(day).minusSeconds(1))
        verificationRows(1, "EXPIRED", verificationCutoff(day).minusSeconds(1))

        val first = post(day.toString())

        assertEquals(200 to "COMPLETED", first.status to first.body["status"])
        assertEquals(day.toString(), first.body["date"])
        assertEquals(mapOf<String, Any?>("outboxEvents" to 2, "verifications" to 1), first.deleted())
        assertTrue("reason" in first.body)
        assertEquals(null, first.body["reason"])

        val again = post(day.toString())

        assertEquals(409 to "RETENTION_ALREADY_COMPLETED", again.status to again.body["code"])
    }

    @Test
    fun `실행 중이면 409 RETENTION_RUNNING - 도중에 JVM 이 죽어 STARTED 로 남은 날짜도 같다`() {
        val day = RetentionTestDates.next()
        val stuck = startedExecution(day)
        try {
            val response = post(day.toString())

            assertEquals(409 to "RETENTION_RUNNING", response.status to response.body["code"])
        } finally {
            abandon(stuck)
        }
    }

    @Test
    fun `실패하면 200 FAILED + 이유, deleted 는 null - 요청은 처리됐고 결과가 실패다`() {
        val day = RetentionTestDates.next()
        jobRepository.createJobInstance(DataRetention.JOB_NAME, DataRetention.parameters(day))   // 처음 시작하다 죽은 곳

        val response = post(day.toString())

        assertEquals(200 to "FAILED", response.status to response.body["status"])
        assertEquals("ERROR" to IllegalStateException::class.java.name, response.reason()["code"] to response.reason()["message"])
        assertTrue("deleted" in response.body)
        assertEquals(null, response.body["deleted"])
    }

    @Test
    fun `내일 이후는 400 RETENTION_DATE_IN_FUTURE, 오늘과 지난 날은 실행한다`() {
        val today = RetentionTestDates.next()
        todayIs(today)

        val tomorrow = post(today.plusDays(1).toString())
        val sameDay = post(today.toString())
        val yesterday = post(today.minusDays(1).toString())

        assertEquals(400 to "RETENTION_DATE_IN_FUTURE", tomorrow.status to tomorrow.body["code"])
        assertEquals(200 to "COMPLETED", sameDay.status to sameDay.body["status"])
        assertEquals(200 to "COMPLETED", yesterday.status to yesterday.body["status"])
    }

    @Test
    fun `오늘은 KST 로 정한다 - KST 00시 30분(UTC 전날 15시 30분)이면 KST 오늘은 실행, KST 내일은 400`() {
        val today = RetentionTestDates.next()
        testClock.fix(today.atTime(0, 30).atZone(seoul).toInstant())   // UTC 로는 아직 어제

        val sameDay = post(today.toString())
        val tomorrow = post(today.plusDays(1).toString())

        assertEquals(200 to "COMPLETED", sameDay.status to sameDay.body["status"])
        assertEquals(400 to "RETENTION_DATE_IN_FUTURE", tomorrow.status to tomorrow.body["code"])
    }

    @Test
    fun `날짜가 없거나 형식이 틀리면 400 INVALID_REQUEST`() {
        assertEquals(400 to "INVALID_REQUEST", post(null).let { it.status to it.body["code"] })
        assertEquals(400 to "INVALID_REQUEST", post("2026-13-01").let { it.status to it.body["code"] })
        assertEquals(400 to "INVALID_REQUEST", post("20261102").let { it.status to it.body["code"] })
    }
}
```

- [ ] **Step 2: 실패 확인**

Run: `./gradlew :apps:api:test --tests '*RetentionApiTest*'`
Expected: FAIL (`assertEquals` 에서 첫 응답이 404 — 컨트롤러가 없다. 예: `expected: <(200, COMPLETED)> but was: <(404, null)>`)

- [ ] **Step 3: 구현**

`web/RetentionController.kt`:
```kotlin
package io.github.hyeokjinon.banktransfer.api.web

import io.github.hyeokjinon.banktransfer.api.retention.DataRetentionLauncher
import io.github.hyeokjinon.banktransfer.api.retention.RetentionAlreadyCompletedException
import io.github.hyeokjinon.banktransfer.api.retention.RetentionDeleted
import io.github.hyeokjinon.banktransfer.api.retention.RetentionFailure
import io.github.hyeokjinon.banktransfer.api.retention.RetentionPolicy
import io.github.hyeokjinon.banktransfer.api.retention.RetentionRun
import io.github.hyeokjinon.banktransfer.api.retention.RetentionRunningException
import org.springframework.format.annotation.DateTimeFormat
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController
import java.time.Clock
import java.time.LocalDate

/** 보존 배치 실행 결과. status = COMPLETED 면 deleted(이 날짜 인스턴스의 모든 실행의 합), FAILED 면 reason. */
data class RetentionRunResponse(val date: LocalDate, val status: String, val reason: RetentionFailure?, val deleted: RetentionDeleted?)

/**
 * 보존 배치 수동 실행 (P5-2, 관리용 — 인증은 다른 관리 API 와 같이 범위 밖). 이 요청 스레드에서 끝까지 돌린다.
 * 실패해도 200 (status = FAILED + 이유) — 요청은 처리됐고 결과가 실패다. 이미 끝난 날짜 · 실행 중이면 409,
 * 내일 이후(KST)면 400 — 기준 시각이 앞당겨져 정책보다 일찍 지우게 된다 ([RetentionPolicy.requireNotFuture]).
 */
@RestController
@RequestMapping("/admin/retention-runs")
class RetentionController(
    private val launcher: DataRetentionLauncher,
    private val clock: Clock,
) {

    @PostMapping
    fun run(@RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) date: LocalDate): RetentionRunResponse {
        RetentionPolicy.requireNotFuture(date, clock.instant())
        return when (val run = launcher.run(date)) {
            is RetentionRun.Completed -> RetentionRunResponse(date, "COMPLETED", null, run.deleted)
            is RetentionRun.Failed -> RetentionRunResponse(date, "FAILED", run.failure, null)
            is RetentionRun.AlreadyCompleted -> throw RetentionAlreadyCompletedException(date)
            is RetentionRun.Running -> throw RetentionRunningException(date)
        }
    }
}
```

`web/ApiExceptionHandler.kt` — import 세 개 (`…api.retention.RetentionAlreadyCompletedException`, `…api.retention.RetentionDateInFutureException`, `…api.retention.RetentionRunningException`), `@ExceptionHandler(…)` 목록의 `ReconDateNotClosedException::class,` 뒤에:
```kotlin
        RetentionAlreadyCompletedException::class,
        RetentionRunningException::class,
        RetentionDateInFutureException::class,
```
`when` 의 `is ReconDateNotClosedException -> …` 줄 뒤에:
```kotlin
            // P5-2 보존 배치 관리 API. 대사와 같이 날짜마다 잡 인스턴스 하나
            is RetentionAlreadyCompletedException -> HttpStatus.CONFLICT to "RETENTION_ALREADY_COMPLETED"
            is RetentionRunningException -> HttpStatus.CONFLICT to "RETENTION_RUNNING"
            is RetentionDateInFutureException -> HttpStatus.BAD_REQUEST to "RETENTION_DATE_IN_FUTURE"
```

- [ ] **Step 4: 통과 확인**

Run: `./gradlew :apps:api:test --tests '*RetentionApiTest*' --tests '*ApiExceptionHandlerTest*'`
Expected: PASS

`"reason" in first.body` 가 실패하면(응답에 null 키가 빠짐) `RetentionRunResponse` 에 `@JsonInclude(JsonInclude.Include.ALWAYS)` 를 붙인다 — 스펙 §4 의 응답 모양이 `"reason":null` 이다. 단언을 지우지 않는다.

- [ ] **Step 5: 커밋**

```bash
git add apps/api/src/main/kotlin/io/github/hyeokjinon/banktransfer/api/web/RetentionController.kt apps/api/src/main/kotlin/io/github/hyeokjinon/banktransfer/api/web/ApiExceptionHandler.kt apps/api/src/test/kotlin/io/github/hyeokjinon/banktransfer/api/web/RetentionApiTest.kt
git commit -m "feat(retention): POST /admin/retention-runs — 200 · 409 RETENTION_ALREADY_COMPLETED · RETENTION_RUNNING · 400 RETENTION_DATE_IN_FUTURE

내일 이후를 받으면 기준 시각이 앞당겨져 정책보다 일찍 지운다. 오늘과 지난 날은 받는다.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 8: 변이 검사 · 결정 기록 · 문서 · 손 확인 자료

코드를 고치지 않는다 (변이는 되돌린다). 손 확인 자료는 만들기만 하고 **실행하지 않는다** — PR 뒤 사용자 몫이다.

**Files:**
- Create: `docs/adr/0011-data-retention.md`, `http/data-retention.http`
- Create: `postman/collections/bank-transfer/7-data-retention/.resources/definition.yaml` 와 요청 4개 (`01-run-retention`, `02-run-again`, `03-tomorrow`, `04-verified-transfer` `.request.yaml`)
- Modify: `docs/database.md`, `docs/adr/0010-daily-reconciliation.md`(두 줄), `OPERATION.md`, `ARCHITECTURE.md`, `README.md`, `SETUP.md`(테스트 수), `HANDOVER.md`, `CHANGELOG.md`, `postman/collections/bank-transfer/.resources/definition.yaml`, `.run/5. API (부하 측정).run.xml`

**Interfaces:**
- Consumes: Task 1 ~ 7 의 이름 · 설정 · 오류 코드, Task 5 커밋 본문의 `관찰:` 두 줄 (`git log --grep '두 격리 수준을 나란히' --format=%B -1`), Step 1 의 변이 결과, Step 9 의 테스트 수
- Produces: ADR-0011 (상태: 제안 — 머지 때 채택)

- [ ] **Step 1: 변이 검사 (일회성, 커밋하지 않는다)**

시작 전 `git status --short` 가 깨끗한지 본다. 변이마다: 파일을 고치고 → 테스트를 돌리고 → 실패한 테스트 이름을 적고 → `git restore <파일>` 로 되돌리고 → `git status --short` 가 다시 깨끗한지 본다. 실패 이름은 `grep -l '<failure' apps/api/build/test-results/test/TEST-*Retention*.xml` 과 Gradle 출력에서 읽는다. 모두 Testcontainers 안에서만 지운다.

| # | 파일 | 바꾸기 | 명령 | 기대 |
|---|---|---|---|---|
| 1 | `retention/RetentionRepository.kt` | `WHERE status IN ('SENT', 'EXPIRED', 'LOCKED') AND expires_at < :cutoff` → `WHERE expires_at < :cutoff` | `./gradlew :apps:api:test --tests '*DataRetentionJobTest*' --tests '*RetentionRepositoryTest*'` | `핵심 - expires_at 이 지난 VERIFIED …` 실패 (`findById` 가 null 이거나 출금이 422) + 저장소의 VERIFIED 테스트 실패 |
| 2 | `retention/RetentionRepository.kt` | `WHERE published_at < :cutoff` → `WHERE published_at IS NULL OR published_at < :cutoff` | `./gradlew :apps:api:test --tests '*RetentionRepositoryTest*' --tests '*DataRetentionJobTest*'` | `아웃박스 - 미발행은 아무리 오래돼도 남는다 …` · `대상만 지운다 …` 실패 |
| 3 | `retention/DataRetentionJobConfiguration.kt` | `.transactionAttribute(purgeTransaction())` 줄 삭제 (쓰지 않게 된 함수 경고는 무시) | `./gradlew :apps:api:test --tests '*DataRetentionJobTest*' --tests '*RetentionLockTest*'` | `단계 트랜잭션 - …` 실패 + `READ COMMITTED (잡의 실제 설정) …` 실패 |

Task 5 에서 REPEATABLE READ 가 막히지 않았다면 변이 3 의 잠금 테스트는 실패하지 않는다 — 그대로 적는다 (리플렉션 테스트만 실패). 기대와 다른 결과는 어느 변이든 고치지 말고 그대로 적고 Task 보고에 남긴다. 기대한 테스트가 실패하지 않으면 그것은 테스트 구멍이다: 멈춰 보고한다.

- [ ] **Step 2: `docs/adr/0011-data-retention.md`** — 기존 ADR 형식(제목 · 상태 · 단계 · 맥락 · 결정 · 결과 · 검토한 다른 선택지):
  - 제목: `# ADR-0011 보존 배치는 일회성 데이터 두 종류만 READ COMMITTED 로 나눠 지운다 — 금융 기록은 지우지 않는다`
  - 상태: `제안 (2026-11-02)`. 단계: `P5-2 · 설계: [P5-2 설계](../superpowers/specs/2026-11-02-p5-2-data-retention-design.md) · [ADR-0005](0005-kafka-delivery-and-consumer-idempotency.md) (발행기 READ COMMITTED) · [ADR-0010](0010-daily-reconciliation.md) (Spring Batch · 실행기) · [database.md](../database.md) §4-9`
  - 맥락: 설계 §1 — `outbox_event` 는 출금마다 한 행씩 늘고 발행이 끝나면 읽는 코드가 없다, `account_verification` 은 끝나지 않은 인증도 예금주명 · 계좌번호를 계속 들고 있다. 삭제는 되돌릴 수 없으므로 조건이 이 단계의 전부다.
  - 결정: (1) 지울 대상과 조건 — 설계 §3 표 그대로, 두 함정(`VERIFIED` 도 `expires_at` 이 지나 있다 / 코드를 입력하지 않은 건은 `SENT` 그대로다)을 조건으로 막는다. 상태는 목록으로 적어 새 상태를 묻지 않고 지우지 않는다 (2) 지우지 않는 표 — `transfer` · `wallet_ledger` · `wallet` · `bank_statement_line` · `recon_issue` · `BATCH_*`, 이유는 database.md §4-9 (3) Spring Batch 잡 `dataRetentionJob`, 표마다 tasklet 한 단계, 호출마다 `DELETE … LIMIT n` = 트랜잭션 하나, `ORDER BY` 없음 (4) 기준 시각 = 날짜 00:00(KST) − 보관 기간, 시계를 보지 않아 재시작해도 같다 (5) `READ COMMITTED` + 모든 예외 롤백 — 이유는 재현 결과, 그리고 `TaskletStep` 기본 속성의 `rollbackOn` 을 지키는 이유 (6) 시작 경합 처리를 `DatedJobLauncher` · `DatedJobSingleRunGuard` 로 뽑아 대사와 같이 쓴다 (대사 동작 변화 없음, `DailyReconLauncherTest` 무수정) (7) 스케줄 06:30 KST, 재시도 회차 없음, 풀 4 (8) 알림은 실패만 WARN (스케줄 · 수동 모두), 예외 종류만 (9) 수동 `POST /admin/retention-runs` 200 · 409 · 400, 건수는 인스턴스의 모든 실행의 합 — 재시작하면 멈춘 단계에 건수 0 인 새 단계 실행이 생기기 때문 (10) 설정 범위를 벗어나면 기동 실패.
  - 결과 — 격리 재현: Task 5 커밋 본문의 `관찰:` 두 줄을 그대로 옮기고, 설계 §4 의 가설(`REPEATABLE READ` 의 범위 삭제가 첫 발행 행의 next-key 락으로 새 미발행 행의 자리를 잠근다)이 맞았는지 한 문장으로 적는다. 다르면 "가설과 다름" 을 제목처럼 적고, READ COMMITTED 를 유지한 근거로 남는 것만 적는다.
  - 결과 — 변이 검사: Step 1 표의 세 줄을 실제 실패한 테스트 이름으로 적는다.
  - 결과 — 증명 테스트 이름: `RetentionRepositoryTest`(조건 · 경계 · `NOW(6)` KST), `DataRetentionJobTest`(핵심 202 · 대상만 · 3번 커밋 · 배수 · 단계 설정 리플렉션), `RetentionLockTest`, `DataRetentionLauncherTest`(건수 합 · 재실행 거부 · 재시작 · 동시 실행 · 가드 · STARTED · 인스턴스만), `DataRetentionSchedulerTest`, `RetentionPolicyTest`, `RetentionApiTest`.
  - 결과 — 한계: 설계 §7 그대로 (되돌릴 수 없음 — 첫 운영 실행 전 같은 조건의 `SELECT COUNT(*)`, 만료 후 30일이 지난 인증 확인은 404 `VERIFICATION_NOT_FOUND`, 쓰지 않는 `VERIFIED` 는 영원히 남음, 묶음 사이에 쉬지 않음, InnoDB 표 파일은 바로 줄지 않음) + 실행 계획은 자동 테스트로 두지 않는다 (행이 적으면 옵티마이저가 전체 훑기를 고를 수 있다 — 손 확인 때 `EXPLAIN`) + 법정 보존 기간 주의 (설계 §3 그대로: 전자금융거래법 시행령 제12조 기준 5년(1만 원 이하 거래 등은 1년)으로 2차 자료에서 확인했고 조문 원문은 확인하지 않았다. "이 프로젝트의 가정, 실제 적용은 원문 · 금융위 고시 확인 필요". 7일 · 30일은 법이 아니라 운영 판단).
  - 다른 선택지 표: `READER`/`WRITER` 청크(읽었다가 지우기) — 표마다 읽기가 낭비, 트랜잭션도 길어진다 / `@Scheduled` + SQL 만 — 날짜별 이력 · 삭제 건수 · 재시작 · 동시 실행 방지를 직접 만들어야 한다 / `expires_at` 만으로 삭제 — 인증한 사용자 전부가 422 / `EXPIRED` 만 삭제 — 코드를 입력하지 않은 `SENT` 가 영원히 남는다 / 묶음마다 `ORDER BY` — 정렬 비용만 든다, ROW 복제라 필요 없다 / 일 단위 파티션 DROP — 미발행 행이 섞여 있고 스키마 변경이 크다 / `Job` 에 `@Primary` — 앞으로 `Job` 을 주입하는 코드가 조용히 대사 잡을 받는다 / 삭제 전 건수 미리보기 API — 범위 밖, OPERATION 의 `SELECT COUNT(*)` 로 충분.

- [ ] **Step 3: `docs/database.md`**:
  - 머리 인용문: `기준: P5-2 (Flyway V1 ~ V5 적용)`, 설계 배경에 `[P5-2 설계](superpowers/specs/2026-11-02-p5-2-data-retention-design.md)` 추가.
  - §4-1 인덱스 목록에: `- idx_verification_purge (status, expires_at) — 보존 배치의 삭제 조건 (V5, P5-2). 없으면 묶음마다 표 전체를 훑는다`
  - §4-1 표 아래에 한 줄: `` `VERIFIED` 가 아닌 만료 행은 보존 배치가 지운다 (§4-9). `expires_at` 은 코드 입력 시한이라 `VERIFIED` 도 지나 있다 — 출금은 `status = VERIFIED` 만 보므로 이 행을 지우면 안 된다. ``
  - §4-3 인덱스 줄 뒤에: `발행 뒤 7일이 지난 행은 보존 배치가 지운다 (§4-9). 범위 삭제가 이 인덱스를 탄다. 미발행은 NULL 이라 비교에 걸리지 않는다.`
  - `## 5. 상태 값` 앞에 새 절:
    ```markdown
    ### 4-9. 보존 정책 (P5-2)
    | 표 | 정책 | 조건 · 이유 |
    |---|---|---|
    | `outbox_event` | 발행 뒤 7일 지나면 삭제 | `published_at < 기준`. 미발행은 아무리 오래돼도 남는다. 7일은 브로커 기본 보존과 같아 그동안 Kafka 메시지와 대조할 수 있다 |
    | `account_verification` | 만료 뒤 30일 지나면 삭제 (`VERIFIED` 제외) | `status IN ('SENT','EXPIRED','LOCKED') AND expires_at < 기준`. 끝나지 않은 인증이 예금주명 · 계좌번호를 들고 있을 이유가 없다 |
    | `account_verification` 의 `VERIFIED` | 삭제하지 않음 | 출금 인증의 근거. `expires_at` 이 지나 있어도 지우면 그 사용자의 출금이 422 `ACCOUNT_NOT_VERIFIED` 다. 한 번도 쓰지 않는 `VERIFIED` 도 남는다 — 인증에 만료 정책이 생길 때 다룬다 |
    | `transfer` · `wallet_ledger` · `wallet` | 삭제하지 않음 | 전자금융거래 기록 · 잔고의 근거 |
    | `bank_statement_line` · `recon_issue` | 삭제하지 않음 | 대사 근거 · 감사 기록. 대사 ③ 이 모든 날짜의 줄을 본다 |
    | `BATCH_*` | 삭제하지 않음 | 하루 몇 행, 실행 이력 |

    기준 시각 = 실행 날짜 00:00(KST) − 보관 기간 (`data-retention.*`, OPERATION §10). 7일 · 30일은 법이 아니라 이 프로젝트의 운영 판단이다.

    **법정 보존 기간 (확인되지 않은 가정)**: 전자금융거래법 시행령 제12조 기준 5년(1만 원 이하 거래 등은 1년)으로 2차 자료에서 확인했고, 조문 원문은 확인하지 않았다. 이 프로젝트의 가정이며 실제 적용은 원문 · 금융위 고시 확인이 필요하다.
    ```
  - §6 동시성 표에 행 두 개: `| 보존 배치 삭제 (P5-2) | READ COMMITTED + 트랜잭션마다 DELETE … LIMIT n 한 문장 | 짧은 트랜잭션, gap 락 없음. REPEATABLE READ 와 비교한 재현은 RetentionLockTest · ADR-0011 |` · `| 같은 날짜 보존 배치 동시 실행 (P5-2) | 대사와 같은 코드 (DatedJobLauncher · DatedJobSingleRunGuard) | 진 쪽은 409 RETENTION_RUNNING |`
  - §8 파일명 예시에 `V5__data_retention.sql` 추가. "큰 테이블 변경" 규칙에 따라 PR 본문에 `idx_verification_purge` 의 운영 반영(온라인 DDL)을 적는다고 Step 10 의 체크에 남긴다.
  - §9 에 소절:
    ````markdown
    ### 보존 배치 (P5-2)
    ```sql
    -- 첫 운영 실행 전 대상 확인 (건수 미리보기 기능은 없다). 기준은 실행 날짜 00:00 KST − 보관 기간
    SELECT COUNT(*) FROM outbox_event WHERE published_at < '2026-10-26 00:00:00';
    SELECT status, COUNT(*) FROM account_verification
    WHERE status IN ('SENT','EXPIRED','LOCKED') AND expires_at < '2026-10-03 00:00:00' GROUP BY status;

    -- 실행 이력 · 표별 삭제 건수 (실행 · 단계마다 한 줄, 재시작하면 여러 줄 — 더하면 응답의 deleted 와 같다)
    SELECT p.PARAMETER_VALUE AS retention_date, e.JOB_EXECUTION_ID, e.STATUS, s.STEP_NAME, s.WRITE_COUNT, s.COMMIT_COUNT, e.START_TIME, e.END_TIME
    FROM BATCH_JOB_INSTANCE i
    JOIN BATCH_JOB_EXECUTION e ON e.JOB_INSTANCE_ID = i.JOB_INSTANCE_ID
    JOIN BATCH_JOB_EXECUTION_PARAMS p ON p.JOB_EXECUTION_ID = e.JOB_EXECUTION_ID AND p.PARAMETER_NAME = 'retentionDate'
    LEFT JOIN BATCH_STEP_EXECUTION s ON s.JOB_EXECUTION_ID = e.JOB_EXECUTION_ID
    WHERE i.JOB_NAME = 'dataRetentionJob' ORDER BY e.JOB_EXECUTION_ID DESC, s.STEP_EXECUTION_ID LIMIT 40;

    -- 실행 계획 (행이 적으면 옵티마이저가 전체 훑기를 고를 수 있다 — 시드 데이터로 본다)
    EXPLAIN DELETE FROM outbox_event WHERE published_at < '2026-10-26 00:00:00' LIMIT 1000;
    EXPLAIN DELETE FROM account_verification WHERE status IN ('SENT','EXPIRED','LOCKED') AND expires_at < '2026-10-03 00:00:00' LIMIT 1000;
    ```
    위 예시의 날짜는 2026-11-02 실행 기준이다.
    ````
  - §10: `- **데이터 보존**: …P5-2 보존 배치.` 줄을 `- **데이터 보존**: 발행된 outbox_event · 만료된 account_verification 은 보존 배치가 지운다 (§4-9, P5-2). 한 번도 쓰지 않는 VERIFIED 는 남는다.` 로. `원장 보존(파티셔닝 · 보관)은 P5-2.` 문장을 `원장은 지우지 않는다 (§4-9). 아카이빙 · 파티셔닝은 범위 밖.` 로. `- **대사 보존 · 파티셔닝**: …(P5-2 보존 배치).` 줄을 `- **대사 기록 파티셔닝**: bank_statement_line · recon_issue 는 매일 늘지만 지우지 않는다 (§4-9). 커지면 파티셔닝을 검토한다.` 로.
  - 변경 이력 행: `| 2026-11-02 | V5: idx_verification_purge, 보존 정책표(§4-9), 보존 배치 동시성 · 운영 쿼리 | P5-2 (ADR-0011) |`

- [ ] **Step 4: `docs/adr/0010-daily-reconciliation.md` 두 줄** — 결과 한계의 `` `bank_statement_line` · `recon_issue` 는 매일 늘어난다(보존 · 파티셔닝은 P5-2). `` → `` `bank_statement_line` · `recon_issue` 는 매일 늘어난다(지우지 않는다 — [ADR-0011](0011-data-retention.md)). ``, 선택지 표의 `범위 밖(P5-2). 기록의 원본은 recon_issue 다` → `범위 밖. 기록의 원본은 recon_issue 다`.

- [ ] **Step 5: `OPERATION.md`**:
  - §2 표: `spring.task.scheduling.pool.size` 행을 `` `4` `` · `스케줄러 스레드. 발행기(0.5초) · 정리 작업(10초) · 일마감 대사(01 ~ 06시) · 보존 배치(06:30)가 서로 막지 않게 한다 (P5-2 에서 3 → 4)` 로. `daily-recon.schedule…` 행 뒤에 행 세 개:
    ```markdown
    | `data-retention.outbox-retention` / `verification-retention` | `7d` / `30d` | 발행 · 만료 뒤 이만큼 지난 행을 지운다 (§10). 1일 미만이면 기동 실패 |
    | `data-retention.batch-size` | `1000` | 트랜잭션 하나에 지우는 최대 행 수. 1 ~ 10000 밖이면 기동 실패 |
    | `data-retention.schedule.enabled` / `cron` / `zone` | `true` / `0 30 6 * * *` / `Asia/Seoul` | 매일 06:30 오늘 날짜로. 부하 측정 · 테스트는 `false` |
    ```
  - §4 표: `대사: 아직 마감되지 않은 날짜` 행 뒤에:
    ```markdown
    | 보존 배치: 이미 완료한 날짜 | 409 | `RETENTION_ALREADY_COMPLETED` |
    | 보존 배치: 같은 날짜가 실행 중 | 409 | `RETENTION_RUNNING` |
    | 보존 배치: 내일 이후 날짜 (KST) | 400 | `RETENTION_DATE_IN_FUTURE` |
    ```
  - §8 의 부하 측정 명령줄(249줄 부근)에 `--daily-recon.schedule.enabled=false --data-retention.schedule.enabled=false` 를 더한다 (실행 구성 5 와 같게. 대사 옵션은 지금 빠져 있다).
  - 끝에 새 절 (§9 와 같은 밀도):
    ````markdown
    ## 10. 보존 배치 (P5-2)
    근거: [ADR-0011](docs/adr/0011-data-retention.md). 정책표: [database.md §4-9](docs/database.md).

    ### 10-1. 무엇을 지우나
    | 표 | 조건 | 기본 보관 |
    |---|---|---|
    | `outbox_event` | `published_at < 기준` — 미발행은 남는다 | 7일 |
    | `account_verification` | `status IN ('SENT','EXPIRED','LOCKED') AND expires_at < 기준` — **`VERIFIED` 는 지우지 않는다** | 30일 |

    기준 = 실행 날짜 00:00(KST) − 보관 기간. 금융 기록(`transfer` · `wallet_ledger` · `wallet` · `bank_statement_line` · `recon_issue`)은 지우지 않는다.

    **삭제는 되돌릴 수 없다. 첫 운영 실행 전에는 같은 조건의 `SELECT COUNT(*)` 로 대상을 확인한다** ([database.md §9](docs/database.md)). 건수 미리보기 기능은 없다.

    ### 10-2. 실행
    - **스케줄**: 매일 06:30 (KST), 오늘 날짜로. 재시도 회차는 없다 — 다음 날 실행은 새 인스턴스이고 기준이 뒤로 가 밀린 행까지 지운다. 끝난 날짜는 조용히(INFO) 넘어간다.
    - **수동**: `POST /admin/retention-runs?date=yyyy-MM-dd` — 요청 스레드에서 끝까지 돈다.

    | 응답 | 뜻 |
    |---|---|
    | 200 `{"date","status":"COMPLETED","reason":null,"deleted":{"outboxEvents":n,"verifications":m}}` | 끝났다. 건수는 이 날짜 인스턴스의 모든 실행을 더한 값 (재시작해도 맞다) |
    | 200 `{"date","status":"FAILED","reason":{"code","message"},"deleted":null}` | 요청은 처리됐고 결과가 실패다 |
    | 409 `RETENTION_ALREADY_COMPLETED` · `RETENTION_RUNNING` | 끝난 날짜 · 돌고 있음 |
    | 400 `RETENTION_DATE_IN_FUTURE` | 내일 이후(KST). 기준 시각이 앞당겨져 정책보다 일찍 지우게 되므로 거부. 오늘과 지난 날은 받는다 (지난 날은 기준이 더 과거라 범위가 좁아질 뿐이다) |

    묶음 사이에 쉬지 않는다. 지연 복제가 있는 운영이면 쉬는 시간이 필요하다 (이 프로젝트에는 복제가 없다). 삭제가 늘 돌아도 InnoDB 표 파일은 바로 줄지 않는다 — 공간은 재사용된다.

    ### 10-3. 알림과 결과 확인
    | 언제 | 수준 | 내용 |
    |---|---|---|
    | 실패 (스케줄 · 수동) | WARN | `보존 배치 {date} 실패` + 이유(예외 종류만, 내부 메시지는 로그) |
    | 시작하지 못함 (DB 장애 등) | WARN | 같은 제목 + `시작하지 못함: 예외 클래스` |
    | 성공 | 없음 | 표별 건수를 INFO 로그로 (`data retention … completed`) — 대사 요약이 이미 매일 간다 |

    실행 이력 · 표별 건수는 [database.md §9](docs/database.md) "보존 배치" 쿼리.

    ### 10-4. 복구
    **실패**: 멈춘 단계 · 지운 만큼은 그대로다. 삭제가 하루 밀려도 급하지 않으니 다음 날 실행에 맡겨도 된다. 그날 날짜를 바로 돌리면 멈춘 단계부터 잇는다.

    **실행이 STARTED 로 남음** — 도중에 JVM 이 죽으면 그 실행은 "돌고 있음" 으로 남는다. **다음 날 인스턴스는 막지 않는다** — 그날 날짜를 수동으로 다시 돌릴 때만 409 `RETENTION_RUNNING` 이다. 그 날짜를 꼭 다시 돌려야 할 때만 §9-6 과 같은 SQL 로 푼다:
    1. 실행 이력 쿼리로 `STATUS = STARTED` · `END_TIME` NULL 인 `JOB_EXECUTION_ID` 를 찾는다.
    2. 다른 API 인스턴스가 정말 돌고 있지 않은지 확인한다.
    3. FAILED 로 바꾼다:
       ```sql
       UPDATE BATCH_STEP_EXECUTION SET STATUS='FAILED', EXIT_CODE='FAILED', END_TIME=NOW(6) WHERE JOB_EXECUTION_ID=? AND STATUS IN ('STARTING','STARTED');
       UPDATE BATCH_JOB_EXECUTION SET STATUS='FAILED', EXIT_CODE='FAILED', END_TIME=NOW(6) WHERE JOB_EXECUTION_ID=?;
       ```
    4. 같은 날짜를 다시 실행하면 멈춘 곳부터 잇는다.

    **`reason.code = ERROR`(`IllegalStateException`)** — 처음 시작하다 죽어 잡 인스턴스만 남았다. §9-6 의 "잡 인스턴스만 있고 실행이 없음" 과 같은 SQL 로 지운다 (`JOB_NAME = 'dataRetentionJob'`).

    **`RESTART_REFUSED`** — 지난 실행이 `UNKNOWN` 으로 끝났다. `BATCH_JOB_EXECUTION` 의 `STATUS` 를 확인하고 위와 같이 `FAILED` 로 바꿔 다시 실행한다.

    **동시 시작 때 ERROR 스택은 정상이다** — §9-6 과 같다 (`RetentionRunningException`).

    ### 10-5. 함께 알아 둘 것
    - 만료 후 30일이 지난 인증을 확인하면 "만료"(410) 대신 404 `VERIFICATION_NOT_FOUND` 다.
    - 한 번도 쓰지 않는 `VERIFIED` 는 영원히 남는다 — 인증에 만료 정책이 생길 때 다룬다.
    - 손 확인은 [`http/data-retention.http`](http/data-retention.http), Postman 7 폴더.
    ````

- [ ] **Step 6: `ARCHITECTURE.md`**:
  - §2 소스 지도: `dailyrecon/` 위에 `  batch/DatedJobLauncher.kt                # 날짜별 잡 시작 · 시작 경합 처리 · 동시 실행 가드 (대사 · 보존 배치 공용, P5-2)`. `alert/` 위에:
    ```
      retention/                              # 보존 배치 (P5-2, Spring Batch 6)
        DataRetention.kt                      #   이름 · 기준 시각(RetentionPolicy) · 설정(data-retention.*)
        RetentionRepository.kt                #   삭제 SQL 두 문장 (이 단계의 핵심 조건)
        PurgeTasklet.kt                       #   한 번 불릴 때 DELETE … LIMIT n 한 문장
        DataRetentionJobConfiguration.kt      #   잡 · 두 단계 · READ COMMITTED
        DataRetentionLauncher.kt              #   실행 · 건수 합 · 실패 알림
        DataRetentionScheduler.kt             #   매일 06:30
    ```
    `web/` 쪽에 `  web/RetentionController.kt              # POST /admin/retention-runs (P5-2)`.
  - §6 첫 줄 끝에 ` P5-2 보존 배치 ✅ (ADR-0011).` 를 더한다.
  - 245줄: `- 스케줄러 스레드 4개: 발행기(0.5초) · 정리 작업(10초) · 일마감 대사(01 ~ 06시) · 보존 배치(06:30)가 서로 막지 않는다 (P5-2 에서 3 → 4).`
  - `## 7. 관련 문서` 앞에 소절:
    ```markdown
    ### P5-2 보존 배치
    - 매일 06:30(KST) `dataRetentionJob`: ① 발행 뒤 7일 지난 `outbox_event` ② `VERIFIED` 가 아니고 만료 뒤 30일 지난 `account_verification`. 금융 기록은 지우지 않는다 ([database.md §4-9](docs/database.md)).
    - 호출마다 `DELETE … LIMIT 1000` 한 문장 = READ COMMITTED 트랜잭션 하나. 접수의 아웃박스 INSERT 를 막지 않는다 (재현: `RetentionLockTest`).
    - 대사와 같은 잡 저장소 · 시작 경합 처리(`DatedJobLauncher`) · 알림. 건수는 인스턴스의 모든 실행의 합. 근거: [ADR-0011](docs/adr/0011-data-retention.md)
    ```
  - §8 색인 행: `| [0011](docs/adr/0011-data-retention.md) | 보존 배치는 일회성 데이터 두 종류만 READ COMMITTED 로 나눠 지우고, 금융 기록은 지우지 않는다 |`

- [ ] **Step 7: `http/data-retention.http`** (SQL 은 사용자가 손 확인 때 직접 실행한다 — 구현자는 실행하지 않는다):
  ```http
  ### IntelliJ HTTP Client 로 보존 배치 확인 (P5-2). 머지 전 손 확인 — 로컬 infra MySQL 에 행을 심고 지운다.
  ### 준비: infra up, 모의 은행 "1. Mock Bank (9090)". API 는 미발행 행이 발행되지 않게 발행기를 끄고, 06:30 스케줄도 끈 채로:
  ###   ./gradlew :apps:api:bootRun --args="--transfer.outbox.relay.enabled=false --data-retention.schedule.enabled=false"

  ### 날짜 — 1) · 2) 에서 쓴다. 오늘(KST)로 직접 고친다 (Git Bash: TZ=Asia/Seoul date +%F).
  @retentionDate = 2026-11-02

  ### ============================================================
  ### 0) 시드 — MySQL 클라이언트로 직접 (접속은 infra/README.md "직접 만져 보기"). 모두 'retention-hand' 로 시작해 끝에 지운다.
  ###    -- 아웃박스: 8일 전 발행(지움) · 6일 전 발행(남음) · 미발행(남음)
  ###    INSERT INTO outbox_event (aggregate_id, event_type, payload, created_at, published_at) VALUES
  ###      ('retention-hand-old', 'TransferRequested', '{}', NOW(6), NOW(6) - INTERVAL 8 DAY),
  ###      ('retention-hand-new', 'TransferRequested', '{}', NOW(6), NOW(6) - INTERVAL 6 DAY),
  ###      ('retention-hand-pending', 'TransferRequested', '{}', NOW(6) - INTERVAL 40 DAY, NULL);
  ###    -- 인증: SENT · EXPIRED · LOCKED 31일 전 만료(지움), SENT 29일 전 만료(남음), VERIFIED 31일 전 만료(남음)
  ###    INSERT INTO account_verification (id, user_id, bank_code, account_no, holder_name, code_hash, status, failed_attempts, expires_at, created_at) VALUES
  ###      ('retention-hand-sent', 'user-retention', '090', '5500000000099', '홍길동', REPEAT('0', 64), 'SENT', 0, NOW(6) - INTERVAL 31 DAY, NOW(6)),
  ###      ('retention-hand-expired', 'user-retention', '090', '5500000000099', '홍길동', REPEAT('0', 64), 'EXPIRED', 0, NOW(6) - INTERVAL 31 DAY, NOW(6)),
  ###      ('retention-hand-locked', 'user-retention', '090', '5500000000099', '홍길동', REPEAT('0', 64), 'LOCKED', 5, NOW(6) - INTERVAL 31 DAY, NOW(6)),
  ###      ('retention-hand-fresh', 'user-retention', '090', '5500000000099', '홍길동', REPEAT('0', 64), 'SENT', 0, NOW(6) - INTERVAL 29 DAY, NOW(6)),
  ###      ('retention-hand-verified', 'user-retention', '090', '1002123456789', '홍길동', REPEAT('0', 64), 'VERIFIED', 0, NOW(6) - INTERVAL 31 DAY, NOW(6));
  ###    -- 실행 계획 (database.md §9 의 EXPLAIN 두 줄, 기준 날짜를 고쳐서). 결과를 HANDOVER 손 확인에 적는다

  ### 0-1) 시드 사용자 충전 → 200
  POST http://localhost:8080/api/v1/wallet/deposits
  Content-Type: application/json
  X-User-Id: user-retention

  {
    "amount": 100000
  }

  ### ============================================================
  ### 1) 실행 → 200 {"status":"COMPLETED","reason":null,"deleted":{"outboxEvents":1 이상,"verifications":3 이상}} (로컬 DB 의 다른 오래된 행도 조건에 맞으면 함께 지운다)
  ###    SQL 확인: SELECT aggregate_id, published_at FROM outbox_event WHERE aggregate_id LIKE 'retention-hand%';  → new · pending 만
  ###             SELECT id, status FROM account_verification WHERE id LIKE 'retention-hand%';                    → fresh · verified 만
  POST http://localhost:8080/admin/retention-runs?date={{retentionDate}}

  ### 1-1) 다시 보내면 409 RETENTION_ALREADY_COMPLETED
  POST http://localhost:8080/admin/retention-runs?date={{retentionDate}}

  ### 1-2) 내일 이후는 400 RETENTION_DATE_IN_FUTURE (고치지 않아도 되게 먼 미래 날짜)
  POST http://localhost:8080/admin/retention-runs?date=2099-12-31

  ### ============================================================
  ### 2) 핵심: expires_at 이 지난 VERIFIED 사용자의 출금 → 202. 422 ACCOUNT_NOT_VERIFIED 면 VERIFIED 를 지운 것이다
  POST http://localhost:8080/api/v1/transfers
  Content-Type: application/json
  X-User-Id: user-retention
  Idempotency-Key: {{$random.uuid}}

  {
    "bankCode": "090",
    "accountNo": "1002123456789",
    "amount": 1000,
    "memo": "보존"
  }

  ### ============================================================
  ### 3) 정리 (MySQL): DELETE FROM outbox_event WHERE aggregate_id LIKE 'retention-hand%';
  ###                 DELETE FROM account_verification WHERE id LIKE 'retention-hand%';
  ###    2) 의 출금은 RECEIVED + 미발행으로 남는다 — 발행기를 켠 구성으로 다시 띄우면 평소처럼 처리된다
  ```
  (바깥 펜스의 `http` 내용을 들여쓰기 없이 그대로 파일에 쓴다.)

- [ ] **Step 8: Postman 7 폴더 · 실행 구성**

  `postman/collections/bank-transfer/7-data-retention/.resources/definition.yaml`:
  ```yaml
  $kind: collection
  name: 7. 보존 배치 (P5-2)
  order: 7000
  ```

  `7-data-retention/01-run-retention.request.yaml`:
  ```yaml
  $kind: http-request
  name: 7-1 (수동) 보존 배치 실행 → 200 COMPLETED + 건수
  method: POST
  url: '{{baseUrl}}/admin/retention-runs?date={{retentionDate}}'
  order: 1000
  description: |-
    수동 확인 전용 — manualRetention 이 true 일 때만 나간다 (실제로 지운다). 준비: http/data-retention.http 0) 의 시드와 실행 명령.
    retentionDate 가 비어 있으면 오늘(KST)을 넣는다. 지난 날을 넣으면 기준이 더 과거라 지우는 범위가 좁아진다.
  scripts:
    - type: beforeRequest
      language: text/javascript
      code: |-
        if (pm.variables.get("manualRetention") !== "true") {
          pm.execution.skipRequest();
        }
        if (!pm.collectionVariables.get("retentionDate")) {
          pm.collectionVariables.set("retentionDate", new Date(Date.now() + 9 * 3600 * 1000).toISOString().slice(0, 10));
        }
    - type: afterResponse
      language: text/javascript
      code: |-
        const body = pm.response.json();
        pm.test("200 COMPLETED", () => {
          pm.response.to.have.status(200);
          pm.expect(body.status).to.eql("COMPLETED");
        });
        console.log("deleted = " + JSON.stringify(body.deleted));
  ```

  `7-data-retention/02-run-again.request.yaml`:
  ```yaml
  $kind: http-request
  name: 7-2 (수동) 같은 날짜 다시 → 409 RETENTION_ALREADY_COMPLETED
  method: POST
  url: '{{baseUrl}}/admin/retention-runs?date={{retentionDate}}'
  order: 2000
  scripts:
    - type: beforeRequest
      language: text/javascript
      code: |-
        if (pm.variables.get("manualRetention") !== "true") {
          pm.execution.skipRequest();
        }
        if (!pm.collectionVariables.get("retentionDate")) {
          pm.collectionVariables.set("retentionDate", new Date(Date.now() + 9 * 3600 * 1000).toISOString().slice(0, 10));
        }
    - type: afterResponse
      language: text/javascript
      code: |-
        pm.test("409 RETENTION_ALREADY_COMPLETED", () => {
          pm.response.to.have.status(409);
          pm.expect(pm.response.json().code).to.eql("RETENTION_ALREADY_COMPLETED");
        });
  ```

  `7-data-retention/03-tomorrow.request.yaml`:
  ```yaml
  $kind: http-request
  name: 7-3 내일 날짜 → 400 RETENTION_DATE_IN_FUTURE
  method: POST
  url: '{{baseUrl}}/admin/retention-runs?date={{tomorrow}}'
  order: 3000
  description: |-
    내일 이후(KST)는 기준 시각이 앞당겨져 정책보다 일찍 지우게 되므로 거부한다. 아무것도 지우지 않아 언제나 안전하다.
  scripts:
    - type: beforeRequest
      language: text/javascript
      code: |-
        pm.collectionVariables.set("tomorrow", new Date(Date.now() + 33 * 3600 * 1000).toISOString().slice(0, 10));   // KST 내일
    - type: afterResponse
      language: text/javascript
      code: |-
        pm.test("400 RETENTION_DATE_IN_FUTURE", () => {
          pm.response.to.have.status(400);
          pm.expect(pm.response.json().code).to.eql("RETENTION_DATE_IN_FUTURE");
        });
  ```

  `7-data-retention/04-verified-transfer.request.yaml`:
  ```yaml
  $kind: http-request
  name: 7-4 (수동) 만료 시각이 지난 VERIFIED 사용자의 출금 → 202
  method: POST
  url: '{{baseUrl}}/api/v1/transfers'
  order: 4000
  description: |-
    이 단계의 핵심 확인. http/data-retention.http 0) 의 시드(user-retention 의 VERIFIED)와 충전 뒤, 7-1 다음에 보낸다. 422 ACCOUNT_NOT_VERIFIED 면 VERIFIED 를 지운 것이다.
  headers:
    - key: Content-Type
      value: application/json
    - key: X-User-Id
      value: user-retention
    - key: Idempotency-Key
      value: '{{$guid}}'
  body:
    type: json
    content: |-
      {
        "bankCode": "090",
        "accountNo": "1002123456789",
        "amount": 1000,
        "memo": "보존"
      }
  scripts:
    - type: beforeRequest
      language: text/javascript
      code: |-
        if (pm.variables.get("manualRetention") !== "true") {
          pm.execution.skipRequest();
        }
    - type: afterResponse
      language: text/javascript
      code: |-
        pm.test("202 RECEIVED", () => {
          pm.response.to.have.status(202);
          pm.expect(pm.response.json().status).to.eql("RECEIVED");
        });
  ```

  `postman/collections/bank-transfer/.resources/definition.yaml` — description 의 `8) (P5-1) …` 줄 뒤에:
  ```
    9) (P5-2) "7. 보존 배치" 폴더: 7-3 은 언제나 안전. 7-1 · 7-2 · 7-4 는 실제로 지우는 손 확인이라 http/data-retention.http 0) 의 시드 · 실행 명령 뒤 manualRetention=true 로 (끝나면 비운다). 응답 칸이 비어 있으면 그 변수가 true 가 아니라 건너뛴 것이다. retentionDate 를 비워 두면 오늘(KST). 대응: http/data-retention.http.
  ```
  `대응 파일:` 줄에 `http/data-retention.http` 를 더하고, `variables:` 의 `reconDate: ""` 뒤에 `manualRetention: ""` · `retentionDate: ""`.

  `.run/5. API (부하 측정).run.xml` — `PROGRAM_PARAMETERS` 의 값 끝 `--daily-recon.schedule.enabled=false` 뒤에 ` --data-retention.schedule.enabled=false`.

- [ ] **Step 9: 전체 빌드 · 테스트 수**

  Run: `./gradlew build --rerun-tasks` (5분 안팎 추정. 시간 제한을 넉넉히 — 도구 기본 2분을 넘는다)
  Expected: BUILD SUCCESSFUL

  ```bash
  grep -ho 'testsuite name="[^"]*" tests="[0-9]*"' apps/*/build/test-results/test/*.xml modules/*/build/test-results/test/*.xml \
    | awk -F'tests="' '{ split($2, a, "\""); s += a[1] } END { print s }'
  ```
  이 수(이전 370 + 이 계획의 새 테스트)를 Step 10 의 네 곳에 쓴다.

- [ ] **Step 10: `README.md` · `SETUP.md` · `HANDOVER.md` · `CHANGELOG.md`**:
  - `README.md`: `- **일마감 대사 …` 줄 뒤에 `- **보존 배치 (P5-2)**: 발행 뒤 7일 지난 아웃박스와 `VERIFIED` 가 아닌 만료 뒤 30일 지난 1원인증만 매일 06:30 에 나눠 지운다 (READ COMMITTED, 트랜잭션마다 `DELETE … LIMIT 1000`). 금융 기록은 지우지 않고 이유를 정책표로 남겼다 ([ADR-0011](docs/adr/0011-data-retention.md))`. 테스트 수 세 곳(34 · 97 · 123줄의 `370`)을 Step 9 값으로. 로드맵: `- [x] **P5** …`, `- [x] P5-2 보존 배치 (발행된 아웃박스 · 만료된 1원인증 정리) → [ADR-0011](docs/adr/0011-data-retention.md)`.
  - `SETUP.md:26`: `테스트 370개` → Step 9 값 (그 밖에는 바꾸지 않는다).
  - `HANDOVER.md`: `마지막 갱신: 2026-11-02`. 표의 `| P5-2 ~ P9 | ⬜ 예정 | …` 를 두 행으로: `| P5-2 보존 배치 | 🔧 구현 완료 · PR 전 (브랜치 feat/p5-2-data-retention) | ADR-0011 제안, 테스트 Step 9 값 개. 잠금 재현 · 변이 검사 결과는 ADR. 손 확인(http/data-retention.http, Postman 7)과 EXPLAIN 결과는 PR 뒤 |` · `| P6 ~ P9 | ⬜ 예정 | README 로드맵 |`. §3 "다음 할 일" 2번을 `2. P5-2 PR → 손 확인(EXPLAIN 포함, 결과를 이 표에) → 머지 → ADR-0011 채택 → velog #10.` 로. `### P5-1 범위 밖으로 남긴 것` 의 `보존 배치 … → P5-2.` 줄을 지우고, `원장 파티셔닝 · 보관, bank_statement_line · recon_issue 보존.` 을 `원장 · 대사 기록의 아카이빙 · 파티셔닝 (지우지 않기로 함, ADR-0011).` 로. 109줄의 `원장 보존 정책(파티셔닝 · 보관)은 P5-2 이후.` 를 `원장은 지우지 않는다 (ADR-0011). 파티셔닝 · 보관은 범위 밖.` 으로.
  - `CHANGELOG.md`: `## [P5-1] …` 앞에:
    ```markdown
    ## [P5-2] 보존 배치 — 진행 중 (브랜치 `feat/p5-2-data-retention`)
    ### Added
    - Spring Batch 6 잡 `dataRetentionJob`(① `purgeOutbox` ② `purgeVerifications`), `DELETE … LIMIT n` 을 READ COMMITTED 트랜잭션마다 한 번. 발행된 `outbox_event`(7일) · `VERIFIED` 가 아닌 만료 `account_verification`(30일)만 지운다.
    - Flyway V5 `idx_verification_purge (status, expires_at)`, 설정 `data-retention.*`(범위를 벗어나면 기동 실패).
    - 스케줄 매일 06:30 KST · `POST /admin/retention-runs`(200 / 409 `RETENTION_ALREADY_COMPLETED` · `RETENTION_RUNNING` / 400 `RETENTION_DATE_IN_FUTURE`), 실패 WARN 알림.
    - `http/data-retention.http`, Postman 7 폴더, ADR-0011, database.md 보존 정책표(§4-9), OPERATION §10.
    ### Changed
    - 시작 경합 처리를 `DatedJobLauncher` · `DatedJobSingleRunGuard` 로 뽑아 대사와 같이 쓴다 (대사 동작 변화 없음).
    - 스케줄 풀 3 → 4. `Job` 빈이 둘이 되어 대사 쪽 주입에 `@Qualifier`.
    - 부하 측정 실행 구성 · 명령줄에 `--data-retention.schedule.enabled=false` (명령줄에는 빠져 있던 `--daily-recon.schedule.enabled=false` 도).
    ```

- [ ] **Step 11: 낡은 문장 · 비밀 확인, 커밋**

  ```bash
  grep -rn "P5-2" --include=*.md . --exclude-dir=build --exclude-dir=superpowers | grep -v "0011\|보존 배치\|§4-9"
  git status --short
  ```
  첫 명령에 "P5-2 에서 한다" 고 남은 문장이 없어야 한다. `git status` 에서 `postman/` 아래 **이 Task 가 만든 · 고친 파일 말고 다른 파일이 바뀌어 있으면**(Postman 앱이 다른 형식으로 다시 쓴 것) 커밋에 넣지 않는다 — `git add` 는 아래 경로만 적는다. 토큰 · 비밀번호가 새 파일에 없는지 본다 (`http/data-retention.http` 의 값은 시드용 가짜 값뿐이다).

  ```bash
  git add docs/adr/0011-data-retention.md docs/adr/0010-daily-reconciliation.md docs/database.md http/data-retention.http OPERATION.md ARCHITECTURE.md README.md SETUP.md HANDOVER.md CHANGELOG.md postman/collections/bank-transfer/7-data-retention postman/collections/bank-transfer/.resources/definition.yaml ".run/5. API (부하 측정).run.xml"
  git commit -m "docs(p5-2): ADR-0011 보존 배치 · database.md 정책표 · OPERATION §10 · 손 확인 자료

왜 일회성 데이터 두 종류만 지우는지, 왜 READ COMMITTED 로 나눠 지우는지, 잠금 재현과 변이 검사 결과를 남긴다.
법정 보존 기간은 2차 자료로만 확인한 가정임을 정책표에 적는다.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```
(push · PR 없음. PR 본문에는 `idx_verification_purge` 운영 반영(온라인 DDL) 한 줄과, 손 확인이 PR 뒤 사용자 몫이라는 것을 적는다 — database.md §8 규칙.)

---

## 스펙 대응표 (계획 자체 점검)

| 스펙 | 내용 | Task |
|---|---|---|
| §1 · §2 | 지울 대상 · 잡 · 삭제 방식 · 기준 시각 · 스케줄 · 알림 | 2 ~ 7, 범위 밖은 8 (정책표) |
| §3 | 삭제 조건 표 · 두 함정 · 미발행 유지 | 3 (조건 · 테스트), 4 (핵심 202) |
| §3 지우지 않는 표 · 법정 기간 문구 | database.md 정책표 + 가정 표시 | 8 (Step 2 · 3) |
| §4 잡 | `dataRetentionJob` · 두 단계 · `retentionDate` · `incrementWriteCount` · `CONTINUABLE`/`FINISHED` · 트랜잭션 관리자 · READ COMMITTED · 리플렉션 · `ORDER BY` 없음 | 4 |
| §4 기준 시각 | `RetentionPolicy` 순수 함수, `NOW(6)` KST 테스트 | 2, 3 |
| §4 인덱스 | V5, `EXPLAIN` 은 손 확인 | 3, 8 (database.md §9 · http) |
| §4 READ COMMITTED 가설 | 나란히 재현, 결과를 ADR 에, 다르면 그 결과를 적는다 | 5, 8 (Step 2) |
| §4 실행기 공용화 | 공용 부분 추출 · 가드 일반화 · `DailyReconLauncherTest` 통과 | 1 (+ 4 의 `@Qualifier`) |
| §4 실행과 실패 | 스케줄 · 풀 4 · 끝난 날짜 INFO · 실패 WARN · 자동 재시도 없음 · JVM 사망 · 성공 INFO | 6, 8 (OPERATION) |
| §4 수동 실행 | 200 · 200 FAILED · 409 두 가지 · 400 · 건수 합 | 6 (합), 7 |
| §5 | 설정 키 · 기본값 · 기동 실패 | 2 (풀은 6) |
| §6 핵심 | `expires_at` 지난 `VERIFIED` + 잡 뒤 202 | 4 |
| §6 삭제 조건 | `SENT` · `EXPIRED` · `LOCKED` · 미발행 · 경계 −1µs · `NOW(6)` | 3 |
| §6 순수 함수 | 기준 시각 · KST 경계 · 내일 거부 · 오늘 허용 · 설정 하한 | 2 |
| §6 잡 | 2,500행 3번 커밋 · ① 뒤 ② 실패 재시작 합계 · 리플렉션 · 끝난 날짜 거부 · 동시 실행 | 4, 6 |
| §6 잠금 | READ COMMITTED · REPEATABLE READ 나란히, 한도 있는 락 대기 | 5 |
| §6 API · 스케줄러 · 기본 설정 | 200 · 409 · 400 · KST 경계, 오늘 날짜 · 실패 WARN · 시작 실패, yml 바인딩 | 7, 6, 2 |
| §6 변이 검사 | 세 가지, 결과를 ADR 에 | 8 (Step 1 · 2) |
| §6 손 확인 | `http/data-retention.http` · Postman 7 (자료만 — 실행은 PR 뒤 사용자) | 8 (Step 7 · 8) |
| §7 | 위험과 한계 | 8 (ADR · OPERATION §10) |
| §8 | ADR-0011 · database.md · OPERATION §10 · §4 · ARCHITECTURE · README · HANDOVER · CHANGELOG · http · Postman 7 · `.run/5`. velog #10 은 머지 뒤라 이 계획에 없다 (HANDOVER 다음 할 일에만) | 8 |
