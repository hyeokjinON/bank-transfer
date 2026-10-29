# P5-1 일마감 대사 · 은행 점검 시간 · 알림 — 구현 계획

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 하루치 출금을 모의 은행이 만든 일자별 대사 파일과 맞춰 보는 Spring Batch 잡(`dailyReconciliationJob`)을 API 앱 안에 두고, 결과를 모르는 출금(`UNKNOWN` · `SENDING`)만 자동 확정(`COMPLETED` / `FAILED E002` + 환불)하며 나머지 어긋남은 `recon_issue` 에 기록하고 알린다. 날짜 경계를 맞추려고 자정 전후 은행 점검 시간(23:50 ~ 00:10 KST)에는 은행을 건드리는 웹 요청을 503 으로 거절하고 게이트웨이 송신을 막는다. 알림은 텔레그램(토큰 · chat id 가 있을 때) 또는 로그.

**Architecture:** 파일 형식(쓰기 · 읽기 · 검증)은 `modules/bank-protocol` 의 `ReconFileFormat` 하나를 모의 은행(일자별 거래 기록 · 00:05 일마감 · `--close-day`)과 API 가 같이 쓴다. 점검 시간은 순수 객체 `BankMaintenanceWindow`(`modules/bank-gateway`)와 앱 전체가 보는 `Clock` 빈 하나로 판단하고, 웹은 `BankMaintenanceGuard`(503 `BANK_MAINTENANCE` + `Retry-After`), 은행 호출은 `BankGatewayClient.send()` 의 보내기 문(`SendGate` — 닫히면 "연결 없음" 과 같은 `BankNotSentException`) 한 곳에서 막는다. 판정은 순수 함수 `StatementMatcher` 와 미확정만 확정하는 `StatementSettler`(`modules/transfer`, 기존 `TransferStore.transition` / `fail` 그대로)가 하고, 배치(`apps/api/dailyrecon`)는 6단계(① 파일 검증 ② 적재 500 ③ 출금 대조 100 ④ 은행 줄 대조 100 ⑤ 잔고 · 원장 ⑥ 보고)로 읽기 · 쓰기 · 연결만 한다. 잡 저장소는 Spring Batch 6 JDBC(Flyway V4 가 메타 표를 만든다), 실행은 `DailyReconLauncher` 하나를 스케줄(01 ~ 06시 매시)과 관리 API(`POST /admin/reconciliations`)가 같이 쓴다. 알림은 `AlertNotifier` 포트 + `TelegramNotifier` / `LogNotifier`, 실패는 삼킨다.

**Tech Stack:** Kotlin 2.3.21 · Java 21 · Spring Boot 4.1.1 (Spring 7.0.9, `spring-boot-starter-batch-jdbc`) · Spring Batch 6.0.5 (`JobOperator`, `ChunkOrientedStepBuilder`, `JdbcPagingItemReader`, `@StepScope`) · Hibernate 7.4.5 · Flyway 12.4.0 · MySQL 8.4 · Spring Kafka · Netty 4.2 · Jackson 3.1.5 (`tools.jackson`) · JDK `HttpClient` + `RestClient`(`JdkClientHttpRequestFactory`) · Testcontainers 2.0.5 · JUnit 5 · Gradle 9.7 Kotlin DSL

**Spec:** [docs/superpowers/specs/2026-10-29-p5-1-daily-reconciliation-design.md](../specs/2026-10-29-p5-1-daily-reconciliation-design.md) (승인 2026-10-29. 앞 단계: [P3 설계](../specs/2026-10-19-p3-transfer-design.md) · [P4-1 설계](../specs/2026-10-26-p4-1-wallet-balance-design.md), 결정 기록은 ADR-0010 — Task 12 에서 작성)

## 스펙과 다른 점 / 확인 필요

승인된 결정은 바꾸지 않는다. 아래는 실제 jar 를 확인하다 드러난 스펙의 빈틈과 이 계획의 해결책이다 — 리뷰에서 확인해 달라.

1. **"동시 실행 하나만 — Spring Batch 잡 저장소가 보장한다"(§2 · §5-2) 는 Spring Batch 6.0.5 에서 절반만 맞다.** `TaskExecutorJobLauncher.createJobExecution` 은 ① 인스턴스 조회 ② 실행 목록에서 "돌고 있는 실행" 확인 ③ 실행 생성(`createJobExecution`, 별도 트랜잭션)을 서로 다른 트랜잭션으로 한다(소스 확인). 처음 시작하는 날짜는 `BATCH_JOB_INSTANCE` 의 `UNIQUE (JOB_NAME, JOB_KEY)` 가 막지만 진 쪽은 `JobExecutionAlreadyRunningException` 이 아니라 **`DuplicateKeyException`**(또는 직렬화 격리에서 데드락 → `PessimisticLockingFailureException`)을 받는다. **실패한 날짜를 두 곳이 동시에 재시작하면 둘 다 ②를 통과해 실행이 둘 생길 수 있다.** 해결: (a) 런처가 `DuplicateKeyException` · `PessimisticLockingFailureException` 도 "실행 중"(409 `RECON_RUNNING`)으로 바꾼다 (b) 잡 리스너 `SingleRunGuard.beforeJob` 이 같은 인스턴스에 먼저 만든(작은 id) 실행이 아직 돌고 있으면 이 실행을 멈춘다(예외 → 이 실행만 FAILED, 런처는 "실행 중"으로 답함). 결과가 같다는 보장(조건부 전이 · 유니크)은 그대로이므로 남는 위험은 알림 중복뿐이었지만, 스펙 문장대로 "하나만" 을 지킨다 (Task 11).
2. **Spring Batch 메타 표의 시각은 KST 가 아니다.** Batch 6 은 `CREATE_TIME` · `START_TIME` 등을 `LocalDateTime.now()`(JVM 기본 시간대)로 쓴다(`AbstractJob.execute` 소스). database.md §2 는 `TimeZone.setDefault` 를 금지하므로 바꾸지 않고, **`BATCH_*` 표만 예외(JVM 기본 시간대 벽시계)** 라고 database.md 에 적는다 (Task 12). 업무 표(`bank_statement_line` · `recon_issue`)는 KST 정책 그대로다.
3. **§9 의 `bank.maintenance.start=23:50` 을 application.yml 에 따옴표 없이 쓰면 기동이 실패한다.** SnakeYAML(YAML 1.1)은 `23:50` 을 60진수 정수 1430 으로 읽는다(`00:10` 은 0 으로 시작해 문자열). 값은 그대로 두고 yml 에서 `"23:50"` 처럼 따옴표로 쓴다. 기본 설정 확인 테스트가 이 함정을 잡는다 (Task 4).
4. **§8 가림 예시 `1002****6789`** 는 13자리 계좌의 가운데 5자리를 별 4개로 쓴다. 예시를 규칙으로 읽어 "앞 4 · 뒤 4 만 남기고 가운데는 길이와 무관하게 `****`"(자릿수도 숨김)로 정한다.

## Global Constraints

스펙에서 그대로 옮긴 프로젝트 전체 요구 (모든 Task 의 요구사항에 암묵적으로 포함된다):

- **은행 점검 시간**: `bank.maintenance.start` / `end` (기본 `23:50` / `00:10`, `Asia/Seoul`), `bank.maintenance.enabled` (기본 `true`). **시작 포함 · 끝 제외**, 자정을 넘는 구간을 지원한다. 순수 객체 `BankMaintenanceWindow(start, end, zone)`: `isActive(now)`, `remaining(now)`. 시계는 새 `Clock` 빈 하나를 주입받는다.
- **웹**: 출금 접수, 1원인증 요청 · 확인 → 503 `BANK_MAINTENANCE` + `Retry-After: <점검 종료까지 남은 초, 올림>`. 조회 · 지갑 충전 · 관리 API 는 그대로.
- **은행 호출 어댑터**: 점검 중에는 이체 · 결과조회 전문을 보내지 않고 **"연결 없음"** 으로 답한다 → 이체는 `RECEIVED` 로 돌아가 재시도 토픽 · 재발행, 결과조회는 시도 횟수를 세지 않고 다음 주기. 1원인증의 은행 호출도 막는다. 점검 길이는 재발행 포기(`E001`, 약 26분)보다 짧게 둔다(설정 주석 · OPERATION 에 명시).
- **응답코드 `E002`** = "일마감 대사로 은행 미처리 확정" (`E001` 재발행 포기와 구분). 조회 응답 · OPERATION 표에 추가.
- **오류 코드**: 점검 503 `BANK_MAINTENANCE`, 이미 완료 409 `RECON_ALREADY_COMPLETED`, 실행 중 409 `RECON_RUNNING`, 내일 이후 날짜 400 (오늘은 손 확인용으로 허용).
- **파일 이름**: 대사 파일 `{dir}/recon/RECON_yyyyMMdd.dat`, 모의 은행 일자별 거래 기록 `{dir}/journal/yyyyMMdd.log` (날짜 = 처리 시각의 KST 날짜). 디렉터리: 모의 은행 `--bank-files=…` 인자 / API `daily-recon.file-dir`, 기본 둘 다 `./bank-files` (작업 디렉터리 = 저장소 루트, `.gitignore`). 모의 은행 `--close-day=yyyyMMdd`: 서버를 띄우지 않고 그 날짜 파일만 만들고 끝난다.
- **레코드 레이아웃** (필드 규칙은 전문과 같다: AN 왼쪽 정렬 · 공백 채움, N 오른쪽 정렬 · 0 채움, EUC-KR. 한 줄 = 한 레코드, 줄바꿈 `\n`. 레코드 길이는 구분자 1자리를 포함해 모두 같게 공백으로 채운다):

  | 레코드 | 필드 (길이) |
  |---|---|
  | 헤더 `H` | 구분 `H`(1) · 처리일 `yyyyMMdd`(8) · 은행코드(3) |
  | 상세 `D` | 구분 `D`(1) · 이용기관 거래번호 AN(20) · 은행코드 AN(3) · 계좌번호 AN(16) · 금액 N(13) · 처리시각 `HHmmss`(6) |
  | 트레일러 `T` | 구분 `T`(1) · 상세 건수 N(9) · 금액 합계 N(18) |

  상세에는 **은행이 실제로 처리(입금)한 이체만** 넣는다. 1원인증 송금(`V…`)도 들어가고, 대조하지 않고 건수만 보고한다. 일마감은 매일 00:05 (KST) 에 전날 기록으로, 기록이 없는 날도 헤더 + 0건 트레일러, 임시 파일에 쓰고 같은 디렉터리 안 원자적 이동, 이미 있으면 덮어쓰지 않는다.
- **잡**: `dailyReconciliationJob`, 파라미터 `reconDate`. 단계 ① `validateFile`(태스클릿, 없으면 `FILE_NOT_READY`, 틀리면 `FILE_INVALID`) ② `loadStatement`(**청크 500**) ③ `matchTransfers`(**청크 100**) ④ `matchBankLines`(**청크 100**) ⑤ `checkLedger`(태스클릿) ⑥ `report`(태스클릿). 대사일 D 의 우리 쪽 대상은 `sending_at` 이 D일(KST)인 출금.
- **자동 보정 범위**: **결과를 모르는 출금(`UNKNOWN` · `SENDING`)만** 파일로 확정한다 (있으면 `COMPLETED` 전이 + 이슈 `AUTO_COMPLETED`, 없으면 `fail(…, "E002")` + 환불 + 이슈 `AUTO_FAILED`). 이미 확정된 건은 바꾸지 않고 기록 + 알림. 0건이면 다시 읽어 바뀐 상태로 판정한다.
- **실행**: `daily-recon.schedule.enabled` (기본 `true`), **01:00 ~ 06:00 매시 정각**(Asia/Seoul)에 전날 날짜로. `FILE_NOT_READY` 면 다음 정각에 재시작, 이미 완료면 조용히(INFO), 06:00 회차까지 완료하지 못하면 "대사 미완료" 긴급 알림. 수동 `POST /admin/reconciliations?date=yyyy-MM-dd` → 200 `{status, summary}` (실패해도 200 + `status=FAILED` + 이유). 부하 측정 실행 구성 · 다른 테스트는 `daily-recon.schedule.enabled=false`.
- **설정 키 · 기본값 (§9 그대로)**: `bank.maintenance.{enabled=true,start=23:50,end=00:10}` · `daily-recon.{file-dir=./bank-files, schedule.enabled=true, schedule.cron=0 0 1-6 * * *, schedule.zone=Asia/Seoul, chunk-size=100}` · `alert.telegram.{bot-token=, chat-id=, base-url=https://api.telegram.org}` · `spring.batch.job.enabled=false` · `spring.batch.jdbc.initialize-schema=never`. 환경변수 `ALERT_TELEGRAM_BOT_TOKEN` · `ALERT_TELEGRAM_CHAT_ID`.
- **알림**: `AlertNotifier.notify(Alert(level = INFO | WARN | CRITICAL, title, lines))`. `TelegramNotifier`(토큰 · chat id 가 **둘 다** 있을 때) / `LogNotifier`(그 밖). `POST {base-url}/bot{token}/sendMessage`, 일반 텍스트(파싱 모드 없음), 연결 3초 · 응답 5초. **알림 실패가 대사를 실패시키거나 되돌리지 않는다** — 실패하면 ERROR 로그만 남기고 삼킨다. 기록의 원본은 `recon_issue`. 대사 완료: 불일치 0 이면 INFO, 있으면 CRITICAL. 대사 실패: CRITICAL (`FILE_NOT_READY` 는 06:00 마지막 회차에서만). 기존 경고 3곳 → WARN.
- **가림**: 계좌번호 가운데 가림(`1002****6789`), 사용자 id · 예금주명 넣지 않음, 불일치 목록 최대 **20건** + "외 n건", **4,096자** 이하로 자른다. **토큰 · 비밀은 저장소 · 로그에 남기지 않는다.**
- **데이터**: Flyway `V4__daily_reconciliation.sql` — Spring Batch 6 MySQL 스키마 그대로 + `bank_statement_line`(PK `(bank_date, line_no)`, 인덱스 `reference`) + `recon_issue`(유니크 `(recon_date, type, ref_key)`, `ref_key` = 거래번호 또는 지갑이면 `user_id`). 스키마는 Flyway 로만 만든다. 둘 다 append-only (`resolved_at` · `resolution_note` 갱신만).
- **문서는 한국어**, 코드 주석은 "왜" 를 쓴다 (CLAUDE.md). 도메인 모듈(`modules/*`)은 Spring · JPA · Kafka 의존 없음. 버전 정확히 고정. TDD. 설정 파일은 먼저 읽고 합친다(덮어쓰기 금지). 커밋 `type(scope): 한국어 요약`, 마지막 줄 `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`. push 하지 않는다.
- **테스트는 Testcontainers 만** (MySQL `mysql:8.4` · Kafka · Redis 컨테이너). 테스트 · 도구를 개발자 PC 의 로컬 인프라 MySQL(`infra/docker-compose.yml`, 3306)로 향하게 하지 않는다. Docker Desktop 필요, Git Bash 에서 `export PATH="/c/Users/user/AppData/Local/Programs/DockerDesktop/resources/bin:$PATH"`. 통합 테스트는 `ApiIntegrationTest` 상속(컨텍스트 하나 공유). **공유 DB 규칙**: 테스트마다 새 사용자(`newUserId()` / `fundedUser()`), `RECEIVED` 를 남기는 테스트는 `unregisteredAccount()`, **대사 테스트는 테스트마다 다른 대사일**(잡 인스턴스가 날짜로 갈린다).

**버전 · API 확인 결과 (계획 작성 때 실제 jar 로 확인함 — 구현자는 다시 찾지 않아도 된다)**

확인 방법: 저장소 빌드 파일은 건드리지 않고, 임시 Gradle 프로젝트(스크래치)에서 `platform("org.springframework.boot:spring-boot-dependencies:4.1.1")` + `spring-boot-starter-batch-jdbc` · `spring-boot-starter-batch` · `spring-batch-test` 를 해석해 jar 와 `-sources` jar 를 받아 `javap` 와 소스로 읽었다. 버전은 `~/.gradle/caches/.../spring-boot-dependencies-4.1.1.pom` 의 속성으로 확인했다.

1. **버전 (Boot 4.1.1 BOM)**: `spring-batch.version` **6.0.5**, Spring Framework 7.0.9, Hibernate 7.4.5.Final, Flyway 12.4.0, Jackson 3.1.5(`tools.jackson`), Testcontainers 2.0.5, HikariCP 7.0.2. 의존성은 BOM 이 정하므로 버전을 적지 않는다.
2. **스타터 (Boot 3 습관과 다름)**: Boot 4 는 배치 자동 설정을 둘로 나눴다. `spring-boot-starter-batch` 만 넣으면 `BatchAutoConfiguration` → `DefaultBatchConfiguration` → **`ResourcelessJobRepository`(메모리 — 재시작 · 중복 거부가 프로세스를 넘지 못한다)**. JDBC 잡 저장소는 **`spring-boot-starter-batch-jdbc`**(→ `spring-boot-batch-jdbc` 의 `BatchJdbcAutoConfiguration extends JdbcDefaultBatchConfiguration`)가 있어야 한다. `dataSource` · `PlatformTransactionManager`(여기서는 JPA 의 `transactionManager`) 빈을 그대로 쓰고, `@EnableBatchProcessing` 이 있으면 물러난다(붙이지 않는다). 생성 격리 수준 기본 SERIALIZABLE, `validateTransactionState=true`(잡 시작을 트랜잭션 안에서 부르면 거부).
3. **설정 키**: `spring.batch.job.enabled`(기본 true → `JobLauncherApplicationRunner` 가 기동 때 잡을 돌린다 — false 로), `spring.batch.jdbc.initialize-schema`(`BatchJdbcProperties`, prefix `spring.batch.jdbc`). `spring.batch.initialize-schema` 같은 옛 키는 오류 수준으로 폐기.
4. **패키지가 옮겨졌다**: `Job`·`JobExecution`·`JobInstance` → `org.springframework.batch.core.job`, `JobParameters`·`JobParametersBuilder`·`DefaultJobParametersValidator` → `core.job.parameters`, `JobBuilder` → `core.job.builder`, `Step`·`StepExecution`·`StepContribution` → `core.step`, `StepBuilder` → `core.step.builder`, `Tasklet` → `core.step.tasklet`, `ChunkContext` → `core.scope.context`, `JobExecutionListener` → `core.listener`, `JobOperator`·예외 → `core.launch`, `JobRepository` → `core.repository`, `StepScope` → `core.configuration.annotation`. 아이템 쪽은 **`org.springframework.batch.infrastructure.item`**(`ItemStreamReader`·`ItemStreamWriter`·`Chunk`·`ExecutionContext`), `JdbcPagingItemReaderBuilder` → `infrastructure.item.database.builder`, `Order` → `infrastructure.item.database`, `AbstractItemCountingItemStreamItemReader` → `infrastructure.item.support`, `RepeatStatus` → `infrastructure.repeat`.
5. **실행 = `JobOperator`**: `JobOperator extends JobLauncher`, `start(Job, JobParameters): JobExecution`(기본 메서드, `TaskExecutorJobOperator` 가 구현). 기본 실행기는 `SyncTaskExecutor` → **부른 스레드에서 끝까지 돈다**. `SimpleJobOperator` 는 6.0 에서 폐기 예정. 같은 식별 파라미터면: 마지막이 COMPLETED/ABANDONED → `JobInstanceAlreadyCompleteException`, 돌고 있는 실행 → `JobExecutionAlreadyRunningException`, FAILED/STOPPED → **새 실행으로 재시작**(끝난 단계는 건너뛰고 멈춘 단계는 마지막 커밋 지점부터), UNKNOWN → `JobRestartException`. 처음 시작 경합은 `DuplicateKeyException`(위 "스펙과 다른 점" 1). Kotlin 에서는 모두 비검사 예외로 잡는다.
6. **청크 API 가 바뀌었다**: `StepBuilder.chunk(int, PlatformTransactionManager)` 와 `SimpleStepBuilder` 는 **6.0 에서 폐기 예정(forRemoval)**. 새 길은 `StepBuilder(name, jobRepository).chunk<I, O>(size)` → `ChunkOrientedStepBuilder` → `.reader() .writer() .transactionManager(tm) .transactionAttribute(attr) .build()` → `ChunkOrientedStep`. **`.transactionManager()` 를 빼면 기본이 `ResourcelessTransactionManager`(트랜잭션 없음)** 이다. 태스클릿도 `tasklet(tasklet, tm)` 으로 넘긴다(`tasklet(tasklet)` 만 쓰면 역시 Resourceless). 읽기는 청크 트랜잭션 **안**에서 한다. 청크가 실패하면 스트림의 `update` 는 불리지 않고, 단계의 ExecutionContext 는 마지막으로 성공한 청크 값으로 남는다(`AbstractStep` finally 의 `updateExecutionContext`).
7. **파라미터**: `JobParametersBuilder().addLocalDate("reconDate", date)`, `JobParameters.getLocalDate(name): LocalDate?`(jspecify `@Nullable`). `StepContext.getJobParameters()` 는 타입 그대로의 값 맵이라 `@Value("#{jobParameters['reconDate']}") reconDate: LocalDate` 가 된다.
8. **`@StepScope`** = `@Scope("step", proxyMode = TARGET_CLASS)`. Kotlin 클래스는 final 이라 CGLIB 프록시가 안 되므로 **스텝 범위 빈의 반환형을 인터페이스(`ItemStreamReader<T>` · `ItemStreamWriter<T>`)로** 선언한다 → JDK 프록시. 인터페이스가 `ItemStream` 이라 빌더가 스트림으로도 등록한다(`addAsStreamAndListener`). 스코프는 `DefaultBatchConfiguration` 의 `@Import(ScopeConfiguration)` 가 등록한다.
9. **리더 · 기타**: `JdbcPagingItemReaderBuilder` 는 where 절의 `:이름` 을 보고 이름 있는 파라미터로 쿼리한다(`?` 와 섞으면 거부), 정렬 키 값은 별칭을 뗀 열 이름으로 읽는다. `AbstractItemCountingItemStreamItemReader` 는 `doOpen/doRead/doClose` 를 구현하고 `setName(…)` 을 불러야 하며 `getCurrentItemCount()` 는 public, 재시작하면 읽은 건수만큼 다시 읽어 건너뛴다. `Tasklet.execute` 는 `@Nullable RepeatStatus` 를 돌려준다(아이템 패키지 `@NullMarked`). `JobRepository.findRunningJobExecutions(jobName)` 는 `STATUS IN ('STARTING','STARTED','STOPPING')`, `getLastStepExecution(instance, stepName)` 은 단계의 ExecutionContext 까지 채워 돌려준다. `createJobInstance` · `createJobExecution(instance, params, ExecutionContext)` 는 공개 메서드다.
10. **메타 스키마**: `spring-batch-core-6.0.5.jar!/org/springframework/batch/core/schema-mysql.sql` — 표 6개 + 시퀀스 표 3개(`BATCH_*_SEQ` + 초기 행 INSERT). Task 9 의 V4 에 **한 글자도 바꾸지 않고** 옮겼다. 시각은 `LocalDateTime.now()` (위 "스펙과 다른 점" 2).
11. **Spring 7 · Jackson 3**: `org.springframework.http.client.JdkClientHttpRequestFactory(HttpClient)` + `setReadTimeout(Duration)`, `RestClientResponseException.statusCode`. `tools.jackson.databind.json.JsonMapper.shared()`, `JsonNode.asString()`(3.x 이름; `asText()` 는 final 별칭). `org.springframework.scheduling.support.CronExpression.parse(…).next(temporal)`.
12. **JPA 트랜잭션 격리**: `HibernateJpaDialect.beginTransaction` 은 연결 해제 모드가 ON_CLOSE 일 때 격리 수준을 커넥션에 건다(아니면 `InvalidIsolationLevelException`). 이 저장소는 이미 `MessagingConfiguration.outboxRelay` 의 READ COMMITTED 템플릿으로 같은 길을 쓴다 → ③ ④ 의 `transactionAttribute(READ_COMMITTED)` 도 된다 (Task 10 의 경합 테스트가 증명).
13. **코드 사실**: `JpaTransferStore.transition` · `fail` 은 공용 `TransactionTemplate`(REQUIRED)이라 청크 트랜잭션에 합류한다(실패하면 전이 · 환불도 함께 롤백 → 재시작해도 이중 환불 없음). 조건부 UPDATE 들은 `@Modifying(clearAutomatically = true)` 라 0건 뒤 `findById` 는 영속성 컨텍스트가 아니라 DB 를 다시 읽는다. `Clock.systemUTC()` 배선 위치: `AppConfiguration`(인증 저장소 · 원장 · 출금 서비스 · 1원인증 서비스), `MessagingConfiguration.transferProcessor`, `ReconcileConfiguration.transferReconciler`. 테스트 JVM 은 `-Duser.timezone=UTC`(apps/api). `TransferRequestedListener { … }` 꼴의 후행 람다 생성이 테스트에 여러 곳 있다 → 알림 인자는 **앞**에 기본값과 함께 둔다. Gradle `run` · `bootRun` 의 작업 디렉터리는 기본이 각 모듈 폴더라 `./bank-files` 가 서로 다른 곳이 된다 → 둘 다 저장소 루트로 바꾼다(Task 2).

## 결정(계획) 모음

스펙이 말하지 않거나 모호한 곳의 판단이다. 각 Task 안에도 같은 표시로 다시 적는다.

- 결정(계획): 레코드 길이 = 가장 긴 상세 레코드 **59바이트**, 모든 레코드 뒤에 `\n`, **마지막 레코드 뒤 줄바꿈이 없으면 거부** — 반쯤 쓴 파일을 받아들이지 않게.
- 결정(계획): `line_no` = **파일의 물리 줄 번호(헤더 = 1)** — 운영자가 파일에서 바로 찾을 수 있게.
- 결정(계획): 헤더 은행코드 = 모의 은행 코드 **`090`**(모든 예시 · 테스트가 쓰는 값). API 는 읽어 두기만 하고 검증하지 않는다 — 스펙의 검증 항목(날짜 · 건수 · 합계)만.
- 결정(계획): 모의 은행 일마감이 만든 파일이 이미 있으면 그대로 둔다(`--close-day` 도 같음). 다시 만들려면 파일을 지우고 `--close-day` — 은행이 한 번 보낸 파일은 바뀌지 않는다.
- 결정(계획): 점검 차단은 **`BankGatewayClient.send()` 의 `SendGate` 한 곳** — 이체 · 결과조회 · 1원인증(성명조회 · 1원 이체)이 모두 이 길을 지나 빠뜨릴 수 없다. 폴링(링크 체크)은 `send()` 를 거치지 않아 점검 중에도 연결을 유지한다. `BankMaintenanceWindow` 는 `modules/bank-gateway` 에 둔다(은행의 성질, 순수 `java.time`).
- 결정(계획): `start == end` 는 기동 실패(구간이 없다). 시간대는 `Asia/Seoul` 고정(새 설정 키를 만들지 않는다).
- 결정(계획): `Clock.systemUTC()` 배선 **전부**를 `Clock` 빈으로 바꾼다 — 상태 시각(`sending_at` · `updated_at`)과 점검 판단이 같은 "지금" 을 봐야 시계를 옮기는 흐름 테스트가 맞는다. 도메인 클래스의 기본 인자(`Clock.systemUTC()`)는 그대로.
- 결정(계획): 통합 테스트의 시계 `TestClock` 은 진짜 시각을 따르되 **진짜 시각이 점검 시간에 걸리면 점검 직전으로 비켜 간다** — 밤 23:50 ~ 00:10 에 CI · 개발 PC 에서 도는 다른 테스트가 503 으로 흔들리지 않게.
- 결정(계획): `bank.maintenance.start` · `end` 는 문자열 `"HH:mm"` 로 받아 `LocalTime.parse` — yml 은 따옴표 필수(위 "스펙과 다른 점" 3).
- 결정(계획): 가림 = 앞 4 · 뒤 4 + `****`. 알림 본문 전체에서 **10~16자리 숫자 덩어리(낱말 경계)** 를 한 번 더 가린다(안전망) — 거래번호(`T…` 20자)는 영문으로 시작하는 한 낱말이라 걸리지 않는다.
- 결정(계획): 텔레그램을 켜도 **로그에 먼저 남긴다**(채널이 죽어도 기록), 로그는 한 줄(줄바꿈 → ` | `). 텔레그램 실패 로그에는 예외 종류 · HTTP 상태만 — 예외 메시지에는 URL(토큰)이 들어 있다.
- 결정(계획): 알림 `items`(목록)는 **불일치만, 긴급 먼저** — 자동 확정은 요약 줄의 건수로만.
- 결정(계획): 자동 완료의 `bank_response_code` = `0000`(결과조회 확정과 같다). 감사 기록은 이슈 `AUTO_COMPLETED`.
- 결정(계획): 판정 순서 — 줄 2개 이상(`PAID_TWICE`)이 금액 · 계좌 비교보다 먼저. 우리 출금이 없으면 줄 수와 무관하게 `UNKNOWN_REFERENCE`.
- 결정(계획): 0건 뒤 다시 읽어 판정은 **최대 3회**, 넘으면 예외 → 단계 실패(재시작으로 이어서).
- 결정(계획): ③ ④ 청크 트랜잭션은 **READ COMMITTED** — MySQL 기본 REPEATABLE READ 에서는 청크 첫 읽기의 스냅숏 때문에 "다시 읽기" 가 바뀐 상태를 보지 못한다.
- 결정(계획): ④ 는 거래번호마다 **파일 D 의 첫 줄 하나만** 읽고, 판정은 그 거래번호의 적재된 모든 줄로 한 번 — 이중 지급을 두 번 세지 않게.
- 결정(계획): 판정별 건수(일치 · 재시도 중 · 대상 수)는 단계 ExecutionContext(`verdict.<이름>`)에, 자동 확정 · 불일치 건수는 `recon_issue` 에서 센다 — 재시작해도 늘지 않는다.
- 결정(계획): 실패 이유 코드 = `FILE_NOT_READY` · `FILE_INVALID` · `RESTART_REFUSED` · `ERROR`(예외 클래스 이름만 — 내부 메시지를 응답에 싣지 않는다). 관리 API 의 `reason` = `{code, message}`.
- 결정(계획): 내일 이후 날짜의 400 코드 = **`RECON_DATE_IN_FUTURE`**. 날짜 형식 오류 · 누락은 기존 규칙대로 400 `INVALID_REQUEST`.
- 결정(계획): 실패 알림 — 스케줄 마지막 회차(06:00)는 이유와 무관하게 "대사 미완료" CRITICAL 하나, 그 전 회차 · 수동의 `FILE_NOT_READY` 는 알리지 않음(INFO 로그), 그 밖 실패는 회차 · 수동마다 CRITICAL. "실행 중" · "이미 완료" 는 알리지 않는다.
- 결정(계획): "마지막 회차" = **같은 cron 의 다음 실행이 다음 날인 회차** — 설정 키를 늘리지 않고 cron 을 바꿔도 맞게.
- 결정(계획): `spring.task.scheduling.pool.size` 2 → **3** — 대사가 스케줄 스레드에서 동기로 도는 동안 발행기 · 정리 작업이 한 스레드를 나눠 쓰지 않게.
- 결정(계획): `recon_issue.ref_key` = `COALESCE(reference, user_id)` **STORED 생성 컬럼 + NOT NULL**(둘 다 없으면 INSERT 가 실패), `resolution_note VARCHAR(500)`, 열린 이슈 조회용 인덱스 `(resolved_at, recon_date)`. 중복 무시는 `ON DUPLICATE KEY UPDATE` no-op.
- 결정(계획): `transfer.sending_at` 단독 인덱스는 **추가하지 않는다** — 하루 한 번 훑기, database.md §10 에 검토 항목으로.
- 결정(계획): Gradle `:apps:mock-bank:run` · `:apps:api:bootRun` 의 작업 디렉터리 = 저장소 루트 — IntelliJ 실행 구성(Gradle 형)도 이 값을 따른다.
- 결정(계획): 손 확인용 실행 구성 `6. API (대사 손 확인)` (`--transfer.reconcile.enabled=false`) 를 더한다 — 스펙 §10 손 확인 2 의 기동 조건.

## 파일 구조

| 파일 | 책임 |
|---|---|
| `modules/bank-protocol/.../protocol/ReconFile.kt` (신규) | `ReconDetail` · `ReconLine` · `ReconFile`, `ReconFileFormat`(쓰기 · 읽기 · 검증 · 거래 기록 줄) |
| `apps/mock-bank/.../mockbank/BankJournal.kt` (신규) | 일자별 거래 기록, 처리 거래번호 복원, 일마감 파일(원자적), 00:05 까지 남은 시간 |
| `apps/mock-bank/.../mockbank/MockBankOptions.kt` (신규) | `[포트] --bank-files= --close-day=` 인자 |
| `apps/mock-bank/.../mockbank/MockBankServer.kt` (수정) | 기록 · 복원 · 00:05 일마감 예약, `main` |
| `apps/mock-bank/build.gradle.kts` · `apps/api/build.gradle.kts` · `.gitignore` (수정) | 작업 디렉터리 = 루트, `bank-files/` 제외, (Task 9) `spring-boot-starter-batch-jdbc` |
| `modules/bank-gateway/.../gateway/BankMaintenanceWindow.kt` (신규) | 점검 시간 순수 객체 |
| `modules/bank-gateway/.../gateway/BankGatewayClient.kt` (수정) | `SendGate` — 닫히면 `BankNotSentException` |
| `apps/api/.../config/AppConfiguration.kt` (수정) | `Clock` 빈, `BankMaintenanceProperties` · 창 빈 · 게이트 연결, 웹 가드 빈 |
| `apps/api/.../messaging/{MessagingConfiguration, TransferRequestedListener}.kt` (수정) | 시계 빈, CONFLICT · DLT 알림 |
| `apps/api/.../reconcile/ReconcileConfiguration.kt` (수정) | 시계 빈, 결과조회 한도 알림 |
| `apps/api/.../web/BankMaintenanceGuard.kt` (신규), `TransferController` · `AccountVerificationController` · `ApiExceptionHandler` (수정) | 503 `BANK_MAINTENANCE` + `Retry-After`, (Task 11) 409 · 400 |
| `apps/api/.../alert/{Alert, LogNotifier, TelegramNotifier, AlertConfiguration}.kt` (신규) | 알림 포트 · 본문(가림 · 20건 · 4,096자) · 구현 선택 |
| `modules/transfer/.../transfer/StatementMatching.kt` (신규) | `BankStatementLine` · `ReconIssueType` · `ReconVerdict` · `StatementMatcher` · `Settlement` · `StatementSettler` |
| `apps/api/src/main/resources/db/migration/V4__daily_reconciliation.sql` (신규) | Batch 메타 표 + `bank_statement_line` + `recon_issue` |
| `apps/api/.../dailyrecon/DailyRecon.kt` (신규) | 이름 · 상수 · `DailyReconProperties` |
| `apps/api/.../dailyrecon/ReconRepository.kt` (신규) | 대사 SQL (줄 · 이슈 · 잔고 검증) |
| `apps/api/.../dailyrecon/DailyReconSteps.kt` (신규) | 태스클릿 ①⑤⑥, 파일 리더 ②, 판정 쓰기 ③④, 건수, 파일 예외 |
| `apps/api/.../dailyrecon/ReconSummary.kt` (신규) | 요약 · 완료 알림 |
| `apps/api/.../dailyrecon/DailyReconJobConfiguration.kt` (신규) | 잡 · 단계 · 스텝 범위 빈 · 런처 · 스케줄러 빈 |
| `apps/api/.../dailyrecon/{DailyReconLauncher, DailyReconScheduler}.kt` (신규) | 실행 결과 · 실패 알림 · 동시 실행 가드, 매시 회차 |
| `apps/api/.../web/ReconciliationController.kt` (신규) | `POST /admin/reconciliations` |
| `apps/api/src/main/resources/application.yml` (수정) | §9 설정, 스케줄 풀 3 |
| 테스트 (각 Task) | 아래 Task 마다 |
| 문서 (Task 12) | ADR-0010, protocol v0.3, database.md, OPERATION, SETUP, ARCHITECTURE, README, HANDOVER, CHANGELOG, `http/daily-recon.http`, Postman `6-daily-recon/`, `.run/` |

---

### Task 1: 대사 파일 형식 v0.3 — `ReconFileFormat` (쓰기 · 읽기 · 검증, `modules/bank-protocol`)

**Files:**
- Create: `modules/bank-protocol/src/main/kotlin/io/github/hyeokjinon/banktransfer/protocol/ReconFile.kt`
- Test: `modules/bank-protocol/src/test/kotlin/io/github/hyeokjinon/banktransfer/protocol/ReconFileFormatTest.kt`

**Interfaces:**
- Consumes: 기존 `FixedLengthWriter` · `FixedLengthReader` · `BankCharsets.EUC_KR` · `ProtocolException` (`FixedLength.kt` · `BankMessage.kt`)
- Produces:
  - `data class ReconDetail(reference: String, bankCode: String, accountNo: String, amount: Long, processedAt: LocalTime)` (toString 은 계좌 뒤 4자리만)
  - `data class ReconLine(lineNo: Int, detail: ReconDetail)` — `lineNo` = 파일의 물리 줄 번호(헤더 = 1)
  - `data class ReconFile(date: LocalDate, bankCode: String, lines: List<ReconLine>)`
  - `object ReconFileFormat { const val RECORD_LENGTH = 59; fun fileName(date: LocalDate): String /* RECON_yyyyMMdd.dat */; fun journalFileName(date: LocalDate): String /* yyyyMMdd.log */; fun detailRecord(detail: ReconDetail): ByteArray; fun journalLine(detail: ReconDetail): ByteArray; fun parseDetail(record: ByteArray): ReconDetail; fun parseJournal(bytes: ByteArray): List<ReconDetail>; fun write(date: LocalDate, bankCode: String, details: List<ReconDetail>): ByteArray; fun read(bytes: ByteArray, expectedDate: LocalDate): ReconFile }` — 틀리면 모두 `ProtocolException` (줄 번호 접두 `line N: `)

결정(계획): 레코드 59바이트 + `\n`, 마지막 줄바꿈 필수, `line_no` = 물리 줄 번호 (위 "결정(계획) 모음").

- [ ] **Step 1: 실패하는 테스트**

`ReconFileFormatTest.kt`:
```kotlin
package io.github.hyeokjinon.banktransfer.protocol

import java.time.LocalDate
import java.time.LocalTime
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/** 대사 파일 형식 v0.3 (P5-1). 모의 은행이 쓰고 API 의 일마감 대사가 읽는다 — 같은 코드라 왕복이 맞아야 한다. */
class ReconFileFormatTest {

    private val day = LocalDate.of(2026, 10, 28)
    private val paid = ReconDetail("T261028ABCDEFGHIJKLM", "090", "1002123456789", 30_000, LocalTime.of(10, 15, 30))
    private val verification = ReconDetail("V261028NOPQRSTUVWXYZ", "090", "3333012345678", 1, LocalTime.of(23, 49, 59))

    private fun text(bytes: ByteArray) = String(bytes, BankCharsets.EUC_KR)

    /** ASCII 레코드들을 59자로 채우고 줄바꿈을 붙여 파일 바이트로 (잘못된 파일을 손으로 만들 때). */
    private fun fileOf(vararg records: String): ByteArray =
        records.joinToString("") { it.padEnd(ReconFileFormat.RECORD_LENGTH) + "\n" }.toByteArray(BankCharsets.EUC_KR)

    private fun detailText(detail: ReconDetail) = text(ReconFileFormat.detailRecord(detail))

    private fun readFails(bytes: ByteArray): String =
        assertFailsWith<ProtocolException> { ReconFileFormat.read(bytes, day) }.message!!

    @Test
    fun `쓰기 → 읽기 왕복 - 레코드는 모두 59바이트 + 줄바꿈, 줄 번호는 헤더가 1`() {
        val bytes = ReconFileFormat.write(day, "090", listOf(paid, verification))

        val lines = text(bytes).split("\n")
        assertEquals(listOf(59, 59, 59, 59, 0), lines.map { it.toByteArray(BankCharsets.EUC_KR).size })   // 마지막 레코드 뒤에도 줄바꿈
        assertEquals("H20261028090", lines[0].trimEnd())
        assertEquals("DT261028ABCDEFGHIJKLM0901002123456789   0000000030000101530", lines[1])
        assertEquals("T000000002000000000000030001", lines[3].trimEnd())

        val file = ReconFileFormat.read(bytes, day)
        assertEquals(day, file.date)
        assertEquals("090", file.bankCode)
        assertEquals(listOf(ReconLine(2, paid), ReconLine(3, verification)), file.lines)
    }

    @Test
    fun `처리한 이체가 없는 날도 헤더 + 0건 트레일러`() {
        val bytes = ReconFileFormat.write(day, "090", emptyList())

        assertEquals(2, text(bytes).count { it == '\n' })
        assertEquals("T000000000000000000000000000", text(bytes).split("\n")[1].trimEnd())
        assertTrue(ReconFileFormat.read(bytes, day).lines.isEmpty())
    }

    @Test
    fun `트레일러 건수가 상세와 다르면 거부`() {
        val message = readFails(fileOf("H20261028090", detailText(paid), "T000000002000000000000030000"))

        assertTrue("trailer count 2" in message, message)
    }

    @Test
    fun `트레일러 합계가 상세와 다르면 거부`() {
        val message = readFails(fileOf("H20261028090", detailText(paid), "T000000001000000000000029999"))

        assertTrue("trailer sum 29999" in message, message)
    }

    @Test
    fun `헤더 날짜가 대사일과 다르면 거부`() {
        val message = readFails(ReconFileFormat.write(day.minusDays(1), "090", listOf(paid)))

        assertTrue("header date 2026-10-27" in message, message)
    }

    @Test
    fun `길이가 틀린 줄은 줄 번호와 함께 거부 - CRLF 도 길이가 60 이라 거부`() {
        val short = text(ReconFileFormat.write(day, "090", listOf(paid))).replaceFirst("101530\n", "10153\n")
        assertTrue("line 2: record length 58" in readFails(short.toByteArray(BankCharsets.EUC_KR)))

        val crlf = text(ReconFileFormat.write(day, "090", listOf(paid))).replace("\n", "\r\n")
        assertTrue("line 1: record length 60" in readFails(crlf.toByteArray(BankCharsets.EUC_KR)))
    }

    @Test
    fun `마지막 줄바꿈이 없거나(반쯤 쓴 파일) 비었거나 트레일러가 없으면 거부`() {
        val full = ReconFileFormat.write(day, "090", listOf(paid))
        assertTrue("must end with a newline" in readFails(full.copyOf(full.size - 1)))
        assertTrue("empty" in readFails(ByteArray(0)))
        assertTrue("line 2: last record must be the trailer" in readFails(fileOf("H20261028090", detailText(paid))))
        assertTrue("line 2: not a detail record" in readFails(fileOf("H20261028090", "T000000000000000000000000000", "T000000000000000000000000000")))
    }

    @Test
    fun `숫자 칸에 숫자가 아니면 거부`() {
        val broken = detailText(paid).replace("0000000030000", "00000000300X0")

        assertTrue("line 2: not a number" in readFails(fileOf("H20261028090", broken, "T000000001000000000000030000")))
    }

    @Test
    fun `거래 기록 줄 왕복 - 상세 레코드 + 줄바꿈`() {
        val journal = ReconFileFormat.journalLine(paid) + ReconFileFormat.journalLine(verification)

        assertEquals(listOf(paid, verification), ReconFileFormat.parseJournal(journal))
        assertEquals(emptyList(), ReconFileFormat.parseJournal(ByteArray(0)))
    }

    @Test
    fun `파일 이름 - 대사 파일 RECON_yyyyMMdd_dat, 거래 기록 yyyyMMdd_log`() {
        assertEquals("RECON_20261028.dat", ReconFileFormat.fileName(day))
        assertEquals("20261028.log", ReconFileFormat.journalFileName(day))
    }
}
```

- [ ] **Step 2: 실패 확인**

Run: `./gradlew :modules:bank-protocol:test --tests '*ReconFileFormatTest*'`
Expected: FAIL (컴파일: `Unresolved reference 'ReconDetail'`, `Unresolved reference 'ReconFileFormat'`)

- [ ] **Step 3: 구현**

`ReconFile.kt`:
```kotlin
package io.github.hyeokjinon.banktransfer.protocol

import java.io.ByteArrayOutputStream
import java.time.LocalDate
import java.time.LocalTime
import java.time.format.DateTimeFormatter
import java.time.format.DateTimeParseException
import java.time.temporal.TemporalAccessor

/**
 * 대사 파일 상세 한 건 = 은행이 실제로 처리(입금)한 이체 하나 (규격 v0.3 "대사 파일", P5-1).
 * 계좌가 없어 거절한 건은 돈이 움직이지 않았으므로 상세가 없다. 1원인증 송금(V…)도 처리했으면 들어간다.
 */
data class ReconDetail(
    val reference: String,
    val bankCode: String,
    val accountNo: String,
    val amount: Long,
    /** 은행 처리 시각 (KST). 처리일은 파일 헤더 · 거래 기록 파일 이름의 날짜다. */
    val processedAt: LocalTime,
) {
    /** 로그에 계좌번호가 그대로 남지 않게 한다 (data class 기본 toString 은 모든 필드를 찍는다). */
    override fun toString(): String =
        "ReconDetail(reference=$reference, bankCode=$bankCode, accountNo=***${accountNo.takeLast(4)}, amount=$amount, processedAt=$processedAt)"
}

/** 읽은 상세와 파일 안의 물리 줄 번호 (헤더 = 1, 첫 상세 = 2). 운영자가 파일에서 바로 찾을 수 있게 그대로 쓴다. */
data class ReconLine(val lineNo: Int, val detail: ReconDetail)

/** 검증을 통과한 대사 파일 하나. */
data class ReconFile(val date: LocalDate, val bankCode: String, val lines: List<ReconLine>)

/**
 * 대사 파일 형식 (규격 v0.3). 필드 규칙은 전문과 같다 — AN 왼쪽 정렬 · 공백, N 오른쪽 정렬 · 0, EUC-KR, 길이는 바이트.
 * 한 줄 = 한 레코드 + `\n`. 레코드 길이는 구분자 1자리를 포함해 모두 [RECORD_LENGTH] 바이트로 공백을 채운다.
 *
 * - 헤더 `H`: 구분(1) · 처리일 yyyyMMdd(8) · 은행코드(3)
 * - 상세 `D`: 구분(1) · 이용기관 거래번호 AN(20) · 은행코드 AN(3) · 계좌번호 AN(16) · 금액 N(13) · 처리시각 HHmmss(6)
 * - 트레일러 `T`: 구분(1) · 상세 건수 N(9) · 금액 합계 N(18)
 *
 * 모의 은행(일자별 거래 기록 · 일마감 쓰기)과 API 의 일마감 대사(읽기 · 검증)가 이 코드 하나를 쓴다.
 * 마지막 레코드 뒤 줄바꿈이 없으면 거부한다 — 쓰다 만 파일을 받아들이지 않게 (모의 은행은 임시 파일 + 이름 바꾸기로 쓴다).
 */
object ReconFileFormat {
    /** 가장 긴 상세 레코드 = 1 + 20 + 3 + 16 + 13 + 6. 헤더 · 트레일러도 이 길이로 채운다. */
    const val RECORD_LENGTH = 59

    private const val NEWLINE: Byte = 0x0A
    private const val SPACE: Byte = 0x20
    private val DATE: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMdd")
    private val TIME: DateTimeFormatter = DateTimeFormatter.ofPattern("HHmmss")

    /** 은행이 날짜마다 만드는 대사 파일 이름. */
    fun fileName(date: LocalDate): String = "RECON_${DATE.format(date)}.dat"

    /** 모의 은행의 일자별 거래 기록 파일 이름 (처리할 때마다 상세 레코드 한 줄을 덧붙인다). */
    fun journalFileName(date: LocalDate): String = "${DATE.format(date)}.log"

    /** 상세 레코드 한 줄 (줄바꿈 제외, 정확히 [RECORD_LENGTH] 바이트). */
    fun detailRecord(detail: ReconDetail): ByteArray =
        FixedLengthWriter()
            .text("D", 1)
            .text(detail.reference, 20)
            .text(detail.bankCode, 3)
            .text(detail.accountNo, 16)
            .number(detail.amount, 13)
            .text(TIME.format(detail.processedAt), 6)
            .toByteArray()

    /** 거래 기록 파일에 덧붙일 한 줄 (상세 레코드 + 줄바꿈). */
    fun journalLine(detail: ReconDetail): ByteArray = detailRecord(detail) + NEWLINE

    /** 거래 기록 파일 전체를 읽는다. 빈 파일은 0건. 줄이 깨졌으면 [ProtocolException] (줄 번호 포함). */
    fun parseJournal(bytes: ByteArray): List<ReconDetail> =
        if (bytes.isEmpty()) emptyList() else splitRecords(bytes).mapIndexed { i, record -> withLine(i + 1) { parseDetail(record) } }

    /** 상세 레코드 한 줄(줄바꿈 제외)을 읽는다. 길이 · 구분 · 숫자 · 시각이 틀리면 [ProtocolException]. */
    fun parseDetail(record: ByteArray): ReconDetail {
        requireLength(record)
        val reader = FixedLengthReader(record)
        val type = reader.text(1)
        if (type != "D") throw ProtocolException("not a detail record: '$type'")
        return ReconDetail(
            reference = reader.text(20),
            bankCode = reader.text(3),
            accountNo = reader.text(16),
            amount = reader.number(13),
            processedAt = parse(reader.text(6), TIME, LocalTime::from),
        )
    }

    /** 헤더 + 상세 + 트레일러. 처리한 이체가 없는 날도 헤더 + 0건 트레일러인 파일이다. */
    fun write(date: LocalDate, bankCode: String, details: List<ReconDetail>): ByteArray {
        val out = ByteArrayOutputStream()
        fun line(record: ByteArray) {
            out.write(record)
            out.write(NEWLINE.toInt())
        }
        line(padded(FixedLengthWriter().text("H", 1).text(DATE.format(date), 8).text(bankCode, 3)))
        details.forEach { line(detailRecord(it)) }
        line(padded(FixedLengthWriter().text("T", 1).number(details.size.toLong(), 9).number(details.sumOf { it.amount }, 18)))
        return out.toByteArray()
    }

    /**
     * 파일을 읽고 검증한다: 모든 줄 [RECORD_LENGTH] 바이트, 첫 줄 헤더 · 마지막 줄 트레일러, 헤더 날짜 = [expectedDate],
     * 트레일러 건수 · 합계 = 상세. 하나라도 틀리면 [ProtocolException] — 대사는 이 파일을 믿지 않는다.
     */
    fun read(bytes: ByteArray, expectedDate: LocalDate): ReconFile {
        val records = splitRecords(bytes)
        if (records.size < 2) throw ProtocolException("recon file needs a header and a trailer, got ${records.size} record(s)")
        val (date, bankCode) = withLine(1) { header(records.first()) }
        if (date != expectedDate) throw ProtocolException("header date $date is not the expected $expectedDate")
        val lines = records.subList(1, records.size - 1).mapIndexed { i, record ->
            ReconLine(i + 2, withLine(i + 2) { parseDetail(record) })
        }
        val (count, sum) = withLine(records.size) { trailer(records.last()) }
        if (count != lines.size.toLong()) throw ProtocolException("trailer count $count does not match ${lines.size} detail record(s)")
        val actual = lines.sumOf { it.detail.amount }
        if (sum != actual) throw ProtocolException("trailer sum $sum does not match the details sum $actual")
        return ReconFile(date, bankCode, lines)
    }

    private fun header(record: ByteArray): Pair<LocalDate, String> {
        requireLength(record)
        val reader = FixedLengthReader(record)
        if (reader.text(1) != "H") throw ProtocolException("first record must be the header (H)")
        return parse(reader.text(8), DATE, LocalDate::from) to reader.text(3)
    }

    private fun trailer(record: ByteArray): Pair<Long, Long> {
        requireLength(record)
        val reader = FixedLengthReader(record)
        if (reader.text(1) != "T") throw ProtocolException("last record must be the trailer (T)")
        return reader.number(9) to reader.number(18)
    }

    /** `\n` 으로 끝나는 레코드들로 나눈다. EUC-KR 의 2바이트 글자는 0x81 이상이라 0x0A 와 겹치지 않는다. */
    private fun splitRecords(bytes: ByteArray): List<ByteArray> {
        if (bytes.isEmpty()) throw ProtocolException("recon file is empty")
        if (bytes.last() != NEWLINE) throw ProtocolException("file must end with a newline (partially written?)")
        val records = mutableListOf<ByteArray>()
        var start = 0
        for (i in bytes.indices) {
            if (bytes[i] == NEWLINE) {
                records += bytes.copyOfRange(start, i)
                start = i + 1
            }
        }
        return records
    }

    private fun requireLength(record: ByteArray) {
        if (record.size != RECORD_LENGTH) throw ProtocolException("record length ${record.size}, expected $RECORD_LENGTH")
    }

    private fun padded(writer: FixedLengthWriter): ByteArray {
        val bytes = writer.toByteArray()
        return bytes + ByteArray(RECORD_LENGTH - bytes.size) { SPACE }
    }

    private fun <T> parse(text: String, format: DateTimeFormatter, query: (TemporalAccessor) -> T): T =
        try {
            query(format.parse(text))
        } catch (e: DateTimeParseException) {
            throw ProtocolException("not a valid date/time: '$text'", e)
        }

    private inline fun <T> withLine(lineNo: Int, block: () -> T): T =
        try {
            block()
        } catch (e: ProtocolException) {
            throw ProtocolException("line $lineNo: ${e.message}", e)
        }
}
```
(`FixedLengthReader.number` 는 숫자가 아니면 `not a number: '…' at offset …` 을 던진다 → `withLine` 이 `line 2: ` 를 붙인다.)

- [ ] **Step 4: 통과 확인**

Run: `./gradlew :modules:bank-protocol:test`
Expected: PASS (새 테스트 10개 + 기존 `BankMessageSerializerTest` · `BankCodecTest`)

- [ ] **Step 5: 커밋**

```bash
git add modules/bank-protocol
git commit -m "feat(protocol): 대사 파일 형식 v0.3 — 헤더 · 상세 · 트레일러 쓰기 · 읽기 · 검증 (ReconFileFormat)

모의 은행과 일마감 대사가 같은 코드로 쓰고 읽는다. 줄마다 59바이트 + 줄바꿈, 마지막 줄바꿈이 없으면
쓰다 만 파일로 보고 거부한다. 헤더 날짜 · 트레일러 건수 · 합계가 상세와 다르면 대사는 그 파일을 믿지 않는다.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 2: 모의 은행 — 일자별 거래 기록 · 재기동 복원 · 00:05 일마감 · `--close-day` · `--bank-files`

**Files:**
- Create: `apps/mock-bank/src/main/kotlin/io/github/hyeokjinon/banktransfer/mockbank/BankJournal.kt`
- Create: `apps/mock-bank/src/main/kotlin/io/github/hyeokjinon/banktransfer/mockbank/MockBankOptions.kt`
- Modify: `apps/mock-bank/src/main/kotlin/io/github/hyeokjinon/banktransfer/mockbank/MockBankServer.kt`
- Modify: `apps/mock-bank/build.gradle.kts`, `apps/api/build.gradle.kts` (작업 디렉터리), `.gitignore`
- Test: `apps/mock-bank/src/test/kotlin/io/github/hyeokjinon/banktransfer/mockbank/{BankJournalTest, MockBankRestartTest, MockBankOptionsTest}.kt` (모듈의 첫 테스트 — 공통 테스트 의존성은 루트 `build.gradle.kts` 가 준다)

**Interfaces:**
- Consumes: Task 1 `ReconFileFormat.journalLine / parseJournal / write / fileName / journalFileName`, `ReconDetail`
- Produces:
  - `class BankJournal(dir: Path, clock: Clock = Clock.systemUTC(), bankCode: String = MockBankServer.BANK_CODE) { fun record(request: TransferRequest); fun processedReferences(): Set<String>; fun closeDay(date: LocalDate): Path }`
  - `internal fun delayUntilNextClose(now: Instant, at: LocalTime = DAILY_CLOSE_AT): Duration`, `internal val KST: ZoneId`
  - `data class MockBankOptions(port: Int = 9090, bankFiles: Path = Path.of("./bank-files"), closeDay: LocalDate? = null) { companion object { fun parse(args: Array<String>): MockBankOptions } }`
  - `MockBankServer(port, accounts, silentAccounts, silentAfterProcessingAccounts, responseDelay, bankFiles: Path? = null, clock: Clock = Clock.systemUTC())`, `MockBankServer.BANK_CODE = "090"` — `bankFiles = null`(기존 테스트의 기본값)이면 지금처럼 메모리만

결정(계획): 헤더 은행코드 `090`, 이미 있는 대사 파일은 덮어쓰지 않음, Gradle `run` · `bootRun` 작업 디렉터리 = 저장소 루트 (위 "결정(계획) 모음").

- [ ] **Step 1: 실패하는 테스트**

`BankJournalTest.kt`:
```kotlin
package io.github.hyeokjinon.banktransfer.mockbank

import io.github.hyeokjinon.banktransfer.protocol.BizType
import io.github.hyeokjinon.banktransfer.protocol.Header
import io.github.hyeokjinon.banktransfer.protocol.MessageClass
import io.github.hyeokjinon.banktransfer.protocol.ReconFileFormat
import io.github.hyeokjinon.banktransfer.protocol.TransferRequest
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Files
import java.nio.file.Path
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.ZoneOffset
import kotlin.io.path.listDirectoryEntries
import kotlin.io.path.name
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/** 모의 은행의 일자별 거래 기록 · 일마감 (P5-1). 시계는 UTC 로 주고 날짜는 KST 로 정해지는지 본다. */
class BankJournalTest {

    @TempDir
    lateinit var dir: Path

    private val day = LocalDate.of(2026, 10, 28)

    private fun transfer(reference: String, amount: Long = 30_000) =
        TransferRequest(Header(MessageClass.REQUEST, BizType.TRANSFER, 1, LocalDateTime.of(2026, 10, 28, 10, 0)), "090", "1002123456789", amount, "memo", reference)

    private fun journalAt(utc: String) = BankJournal(dir, Clock.fixed(Instant.parse(utc), ZoneOffset.UTC))

    @Test
    fun `처리할 때마다 그날(KST) 기록에 한 줄, 마감하면 헤더 + 상세 + 트레일러`() {
        val journal = journalAt("2026-10-28T14:59:59Z")   // KST 23:59:59
        journal.record(transfer("T261028AAAAAAAAAAAAA"))
        journal.record(transfer("V261028BBBBBBBBBBBBB", amount = 1))

        val file = journal.closeDay(day)

        assertEquals(dir.resolve("recon").resolve("RECON_20261028.dat"), file)
        val read = ReconFileFormat.read(Files.readAllBytes(file), day)
        assertEquals("090", read.bankCode)
        assertEquals(listOf("T261028AAAAAAAAAAAAA", "V261028BBBBBBBBBBBBB"), read.lines.map { it.detail.reference })
        assertEquals(LocalTime.of(23, 59, 59), read.lines.first().detail.processedAt)
    }

    @Test
    fun `날짜는 처리 시각의 KST 날짜 - UTC 15시는 다음 날 기록`() {
        journalAt("2026-10-28T15:00:00Z").record(transfer("T261029AAAAAAAAAAAAA"))

        assertTrue(Files.exists(dir.resolve("journal").resolve("20261029.log")))
    }

    @Test
    fun `이미 만든 대사 파일은 덮어쓰지 않는다 - 그 뒤 기록이 늘어도`() {
        val journal = journalAt("2026-10-28T01:00:00Z")
        journal.record(transfer("T261028AAAAAAAAAAAAA"))
        val first = Files.readAllBytes(journal.closeDay(day))

        journal.record(transfer("T261028CCCCCCCCCCCCC"))
        val again = Files.readAllBytes(journal.closeDay(day))

        assertTrue(first.contentEquals(again))
        assertEquals(1, ReconFileFormat.read(again, day).lines.size)
    }

    @Test
    fun `기록이 없는 날도 헤더 + 0건 트레일러 파일을 만들고 임시 파일은 남지 않는다`() {
        val file = journalAt("2026-10-28T01:00:00Z").closeDay(day)

        assertTrue(ReconFileFormat.read(Files.readAllBytes(file), day).lines.isEmpty())
        assertEquals(listOf("RECON_20261028.dat"), dir.resolve("recon").listDirectoryEntries().map { it.name })
    }

    @Test
    fun `처리한 거래번호는 모든 날짜의 기록에서 되살린다`() {
        journalAt("2026-10-27T01:00:00Z").record(transfer("T261027AAAAAAAAAAAAA"))
        journalAt("2026-10-28T01:00:00Z").record(transfer("T261028AAAAAAAAAAAAA"))

        assertEquals(setOf("T261027AAAAAAAAAAAAA", "T261028AAAAAAAAAAAAA"), journalAt("2026-10-29T01:00:00Z").processedReferences())
        assertEquals(emptySet(), BankJournal(dir.resolve("empty")).processedReferences())
    }

    @Test
    fun `일마감은 매일 00시 05분 (KST) - 정각이면 다음 날`() {
        assertEquals(Duration.ofSeconds(1), delayUntilNextClose(Instant.parse("2026-10-28T15:04:59Z")))   // KST 00:04:59
        assertEquals(Duration.ofDays(1), delayUntilNextClose(Instant.parse("2026-10-28T15:05:00Z")))       // KST 00:05:00
        assertEquals(Duration.ofMinutes(65), delayUntilNextClose(Instant.parse("2026-10-28T14:00:00Z")))  // KST 23:00
    }
}
```

`MockBankRestartTest.kt`:
```kotlin
package io.github.hyeokjinon.banktransfer.mockbank

import io.github.hyeokjinon.banktransfer.protocol.BizType
import io.github.hyeokjinon.banktransfer.protocol.Header
import io.github.hyeokjinon.banktransfer.protocol.InquiryRequest
import io.github.hyeokjinon.banktransfer.protocol.InquiryResponse
import io.github.hyeokjinon.banktransfer.protocol.MessageClass
import io.github.hyeokjinon.banktransfer.protocol.ResponseCodes
import io.github.hyeokjinon.banktransfer.protocol.TransferRequest
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Files
import java.nio.file.Path
import java.time.LocalDateTime
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/** 재기동해도 결과조회가 처리 기록대로 답한다 (P5-1). 기록 디렉터리를 주지 않으면(기존 테스트의 기본값) 아무것도 쓰지 않는다. */
class MockBankRestartTest {

    @TempDir
    lateinit var dir: Path

    private val header = Header(MessageClass.REQUEST, BizType.TRANSFER, 1, LocalDateTime.of(2026, 10, 28, 10, 0))

    private fun inquire(bank: MockBankServer, reference: String) =
        bank.respond(InquiryRequest(header.copy(bizType = BizType.INQUIRY), reference)) as InquiryResponse

    @Test
    fun `재기동해도 결과조회가 처리 기록대로 답한다`() {
        val reference = "T261028RESTART000001"
        MockBankServer(bankFiles = dir).start().use { it.respond(TransferRequest(header, "090", "1002123456789", 30_000, "m", reference)) }

        MockBankServer(bankFiles = dir).start().use { restarted ->
            assertTrue(restarted.hasProcessed(reference))
            assertEquals(ResponseCodes.OK, inquire(restarted, reference).result)
            assertEquals(ResponseCodes.ORIGINAL_NOT_FOUND, inquire(restarted, "T261028NEVERSENT0001").result)
        }
    }

    @Test
    fun `없는 계좌로 거절한 이체는 기록하지 않는다 - 돈이 움직이지 않았다`() {
        MockBankServer(bankFiles = dir).start().use { it.respond(TransferRequest(header, "090", "5500000000001", 1_000, "m", "T261028REJECTED00001")) }

        assertFalse(Files.exists(dir.resolve("journal")))
    }

    @Test
    fun `기록 디렉터리를 주지 않으면 작업 디렉터리에 아무것도 만들지 않는다 (기존 테스트의 기본값)`() {
        MockBankServer().start().use { it.respond(TransferRequest(header, "090", "1002123456789", 1_000, "m", "T261028MEMORYONLY001")) }

        assertFalse(Files.exists(Path.of("bank-files")))   // Gradle 테스트의 작업 디렉터리 = apps/mock-bank
    }
}
```

`MockBankOptionsTest.kt`:
```kotlin
package io.github.hyeokjinon.banktransfer.mockbank

import java.nio.file.Path
import java.time.LocalDate
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class MockBankOptionsTest {

    @Test
    fun `인자가 없으면 9090 · 저장소 루트의 bank-files · 서버 실행`() {
        assertEquals(MockBankOptions(9090, Path.of("./bank-files"), null), MockBankOptions.parse(emptyArray()))
    }

    @Test
    fun `포트 · 디렉터리 · 마감일 (순서 무관)`() {
        assertEquals(
            MockBankOptions(9191, Path.of("tmp/bf"), LocalDate.of(2026, 10, 28)),
            MockBankOptions.parse(arrayOf("--close-day=20261028", "9191", "--bank-files=tmp/bf")),
        )
    }

    @Test
    fun `모르는 옵션 · 숫자가 아닌 포트 · yyyyMMdd 가 아닌 날짜는 거부`() {
        assertFailsWith<IllegalArgumentException> { MockBankOptions.parse(arrayOf("--verbose")) }
        assertFailsWith<IllegalArgumentException> { MockBankOptions.parse(arrayOf("90x0")) }
        assertFailsWith<IllegalArgumentException> { MockBankOptions.parse(arrayOf("--close-day=2026-10-28")) }
    }
}
```

- [ ] **Step 2: 실패 확인**

Run: `./gradlew :apps:mock-bank:test`
Expected: FAIL (컴파일: `Unresolved reference 'BankJournal'`, `'delayUntilNextClose'`, `'MockBankOptions'`, `No parameter with name 'bankFiles'`)

- [ ] **Step 3: 구현**

`BankJournal.kt`:
```kotlin
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
```

`MockBankOptions.kt`:
```kotlin
package io.github.hyeokjinon.banktransfer.mockbank

import java.nio.file.Path
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.time.format.DateTimeParseException

/**
 * 모의 은행 실행 인자 (P5-1): `[포트] [--bank-files=디렉터리] [--close-day=yyyyMMdd]`.
 * `--close-day` 가 있으면 서버를 띄우지 않고 그 날짜의 대사 파일만 만들고 끝난다 (손 확인 · 재생성용).
 * 디렉터리 기본값 `./bank-files` 는 작업 디렉터리 = 저장소 루트 기준이다 (Gradle run 이 루트에서 돈다 — API 의 daily-recon.file-dir 과 같은 곳).
 */
data class MockBankOptions(
    val port: Int = 9090,
    val bankFiles: Path = Path.of("./bank-files"),
    val closeDay: LocalDate? = null,
) {
    companion object {
        private const val USAGE = "usage: [port] [--bank-files=dir] [--close-day=yyyyMMdd]"

        fun parse(args: Array<String>): MockBankOptions =
            args.fold(MockBankOptions()) { options, arg ->
                when {
                    arg.startsWith("--bank-files=") -> options.copy(bankFiles = Path.of(arg.substringAfter('=')))
                    arg.startsWith("--close-day=") -> options.copy(closeDay = closeDayOf(arg.substringAfter('=')))
                    arg.startsWith("--") -> throw IllegalArgumentException("unknown option '$arg' — $USAGE")
                    else -> options.copy(port = arg.toIntOrNull() ?: throw IllegalArgumentException("port must be a number: '$arg' — $USAGE"))
                }
            }

        private fun closeDayOf(text: String): LocalDate =
            try {
                LocalDate.parse(text, DateTimeFormatter.BASIC_ISO_DATE)
            } catch (e: DateTimeParseException) {
                throw IllegalArgumentException("close-day must be yyyyMMdd: '$text'", e)
            }
    }
}
```

`MockBankServer.kt`:
1) import 추가: `java.nio.file.Path`, `java.time.Clock`, `java.time.LocalDate`, `java.util.concurrent.Executors`, `java.util.concurrent.ScheduledExecutorService`.
2) 클래스 KDoc 의 `pause/resume` 줄 아래에 추가:
```kotlin
 * - bankFiles (P5-1): 처리한 이체를 일자별 거래 기록에 남기고, 기동할 때 처리한 거래번호를 되살리고, 매일 00:05(KST)에 전날 대사 파일을 만든다 ([BankJournal]).
 *   null(테스트 기본값)이면 지금처럼 메모리만 쓴다
```
3) 생성자의 `private val responseDelay: Duration = Duration.ZERO,` 줄 아래에 추가:
```kotlin
    /** 일자별 거래 기록 · 대사 파일 디렉터리 (P5-1). null 이면 기록하지 않는다. */
    bankFiles: Path? = null,
    private val clock: Clock = Clock.systemUTC(),
```
4) `private val clients = DefaultChannelGroup(GlobalEventExecutor.INSTANCE)` 줄 아래에 추가:
```kotlin
    private val journal: BankJournal? = bankFiles?.let { BankJournal(it, clock) }

    /** 00:05 일마감 예약 (기록할 때만). 데몬 스레드라 프로세스 종료를 막지 않는다. */
    private val closer: ScheduledExecutorService? = journal?.let {
        Executors.newSingleThreadScheduledExecutor { task -> Thread(task, "mock-bank-daily-close").apply { isDaemon = true } }
    }
```
5) `fun start()` 를 바꾼다:
```kotlin
    fun start(): MockBankServer {
        journal?.let {
            // 재기동해도 결과조회가 처리 기록대로 답한다 — 메모리의 처리 목록만 쓰면 재기동 뒤 모든 조회가 2001 이 된다
            processedReferences += it.processedReferences()
            log.info("mock bank restored {} processed references from the journal", processedReferences.size)
        }
        serverChannel = bootstrap.bind(listenPort).sync().channel()
        listenPort = (serverChannel.localAddress() as InetSocketAddress).port
        log.info("mock bank listening on {}", listenPort)
        scheduleDailyClose()
        return this
    }
```
6) `fun hasProcessed(…)` 아래에 추가:
```kotlin

    private fun scheduleDailyClose() {
        val executor = closer ?: return
        executor.schedule({
            closeYesterday()
            scheduleDailyClose()
        }, delayUntilNextClose(clock.instant()).toMillis(), TimeUnit.MILLISECONDS)
    }

    /** 일마감: 전날(KST) 기록으로 대사 파일을 만든다. 실패해도 서버는 계속 돈다 — 손으로 --close-day 로 다시 만든다. */
    private fun closeYesterday() {
        val yesterday = LocalDate.ofInstant(clock.instant(), KST).minusDays(1)
        try {
            log.info("[일마감] {} 대사 파일: {}", yesterday, journal!!.closeDay(yesterday))
        } catch (e: Exception) {
            log.error("[일마감] {} 대사 파일을 만들지 못함 — --close-day 로 다시 만든다", yesterday, e)
        }
    }
```
7) `override fun close()` 의 첫 줄로 `closer?.shutdownNow()` 를 추가한다.
8) `private fun deposit(message: TransferRequest)` 본문 첫 줄에 추가 (메모리보다 먼저 — "처리함" 의 기준이 기록이다. 쓰기가 실패하면 예외 → 채널이 닫혀 응답이 없다 = 결과를 모름):
```kotlin
        journal?.record(message)
```
9) `companion object` 의 `val DEFAULT_SILENT_AFTER_PROCESSING = …` 줄 아래에 추가:
```kotlin

        /** 모의 은행의 은행코드. 대사 파일 헤더에 쓴다 (예시 · 테스트가 모두 이 코드로 보낸다). */
        const val BANK_CODE = "090"
```
10) 파일 끝의 `fun main(…)` 을 바꾼다:
```kotlin
fun main(args: Array<String>) {
    val options = MockBankOptions.parse(args)
    options.closeDay?.let { day ->
        // 서버를 띄우지 않고 그 날짜 파일만 만든다 (손 확인 · 재생성용). 이미 있으면 그대로 둔다
        println("Recon file for $day: ${BankJournal(options.bankFiles).closeDay(day).toAbsolutePath()}")
        return
    }
    MockBankServer(options.port, bankFiles = options.bankFiles).start()
    println("Mock bank FEP started on port ${options.port}. Accounts: ${MockBankServer.DEFAULT_ACCOUNTS}. Bank files: ${options.bankFiles.toAbsolutePath()}")
    Thread.currentThread().join()
}
```

`apps/mock-bank/build.gradle.kts` 의 `tasks.named<JavaExec>("run") { … }` 블록 안 `jvmArgs(…)` 줄 아래에 추가:
```kotlin
    // ./bank-files 를 API 와 같은 곳(저장소 루트)으로 읽게 (P5-1). 기본값은 모듈 폴더(apps/mock-bank)다
    workingDir = rootProject.projectDir
```
`apps/api/build.gradle.kts` 의 `tasks.named<org.springframework.boot.gradle.tasks.run.BootRun>("bootRun") { … }` 블록 안 `jvmArgs(…)` 줄 아래에 추가:
```kotlin
    // daily-recon.file-dir 기본값 ./bank-files 를 모의 은행과 같은 곳(저장소 루트)으로 (P5-1)
    workingDir = rootProject.projectDir
```
`.gitignore` 의 `### OS / local ###` 절 끝(`.env` 줄 아래)에 추가:
```
bank-files/
```

- [ ] **Step 4: 통과 확인**

Run: `./gradlew :apps:mock-bank:test :modules:bank-gateway:test`
Expected: PASS (새 테스트 12개. 게이트웨이 테스트는 `MockBankServer()` 기본값 — 기록하지 않음 — 으로 그대로 통과)

손으로 한 번: `./gradlew :apps:mock-bank:run --args="--close-day=20261028"` → 콘솔 `Recon file for 2026-10-28: C:\Users\user\bank-transfer\.\bank-files\recon\RECON_20261028.dat` (저장소 루트 아래), 서버는 뜨지 않고 끝난다. `git status --short` 에 `bank-files/` 가 보이지 않아야 한다. 확인 뒤 `rm -r bank-files`.

- [ ] **Step 5: 커밋**

```bash
git add apps/mock-bank apps/api/build.gradle.kts .gitignore
git commit -m "feat(mock-bank): 일자별 거래 기록 · 재기동 복원 · 00:05 일마감 대사 파일 · --close-day / --bank-files

처리할 때마다 기록에 한 줄 남기고 기동할 때 되살려, 재기동해도 결과조회가 처리 기록대로 답한다.
대사 파일은 임시 파일 + 원자적 이름 바꾸기로 만들어 API 가 반쯤 쓴 파일을 읽지 않는다.
모의 은행 run 과 API bootRun 의 작업 디렉터리를 저장소 루트로 맞춰 ./bank-files 가 같은 곳을 가리키게 한다.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 3: 은행 점검 시간 `BankMaintenanceWindow` · 게이트웨이 보내기 문 `SendGate`

**Files:**
- Create: `modules/bank-gateway/src/main/kotlin/io/github/hyeokjinon/banktransfer/gateway/BankMaintenanceWindow.kt`
- Modify: `modules/bank-gateway/src/main/kotlin/io/github/hyeokjinon/banktransfer/gateway/BankGatewayClient.kt`
- Test: `modules/bank-gateway/src/test/kotlin/io/github/hyeokjinon/banktransfer/gateway/BankMaintenanceWindowTest.kt` (신규), `BankGatewayClientTest.kt` (테스트 1개 추가)

**Interfaces:**
- Produces:
  - `class BankMaintenanceWindow(start: LocalTime, end: LocalTime, zone: ZoneId, enabled: Boolean = true) { val length: Duration; fun isActive(now: Instant): Boolean; fun remaining(now: Instant): Duration; fun remainingSeconds(now: Instant): Long }` — `start == end` 면 `IllegalArgumentException`
  - `fun interface SendGate { fun closedReason(): String?; companion object { val OPEN: SendGate } }`
  - `BankGatewayClient(config: BankGatewayConfig, clock: Clock = Clock.systemDefaultZone(), sendGate: SendGate = SendGate.OPEN)` — 닫혀 있으면 `send()` 가 `BankNotSentException(이유)` 로 실패한 future

결정(계획): 점검 차단은 `send()` 한 곳(폴링 제외), 창은 `modules/bank-gateway`, `start == end` 거부 (위 "결정(계획) 모음").

- [ ] **Step 1: 실패하는 테스트**

`BankMaintenanceWindowTest.kt`:
```kotlin
package io.github.hyeokjinon.banktransfer.gateway

import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.LocalDate
import java.time.LocalTime
import java.time.ZoneId
import java.time.ZoneOffset
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/** 은행 점검 시간 (P5-1). 기본 23:50 ~ 00:10 (KST), 시작 포함 · 끝 제외, 자정을 넘는다. */
class BankMaintenanceWindowTest {

    private val seoul = ZoneId.of("Asia/Seoul")
    private val window = BankMaintenanceWindow(LocalTime.of(23, 50), LocalTime.of(0, 10), seoul)

    private fun kst(h: Int, m: Int, s: Int = 0, nanos: Int = 0): Instant =
        LocalDate.of(2026, 10, 28).atTime(h, m, s, nanos).atZone(seoul).toInstant()

    @Test
    fun `시작 포함 · 끝 제외`() {
        assertFalse(window.isActive(kst(23, 49, 59, 999_999_999)))
        assertTrue(window.isActive(kst(23, 50)))
        assertTrue(window.isActive(kst(0, 9, 59, 999_999_999)))
        assertFalse(window.isActive(kst(0, 10)))
    }

    @Test
    fun `자정을 넘는 구간 - 자정 앞뒤 모두 점검 중, 낮은 아님`() {
        assertTrue(window.isActive(kst(23, 59, 59)))
        assertTrue(window.isActive(kst(0, 0)))
        assertFalse(window.isActive(kst(12, 0)))
        assertEquals(Duration.ofMinutes(20), window.length)
    }

    @Test
    fun `자정을 넘지 않는 구간도 된다`() {
        val night = BankMaintenanceWindow(LocalTime.of(3, 0), LocalTime.of(3, 20), seoul)

        assertTrue(night.isActive(kst(3, 10)))
        assertFalse(night.isActive(kst(3, 20)))
        assertFalse(night.isActive(kst(2, 59)))
        assertEquals(Duration.ofMinutes(20), night.length)
    }

    @Test
    fun `남은 시간 - 자정 전이면 다음 날 끝까지, Retry-After 초는 올림, 점검이 아니면 0`() {
        assertEquals(Duration.ofSeconds(899, 500_000_000), window.remaining(kst(23, 55, 0, 500_000_000)))
        assertEquals(900, window.remainingSeconds(kst(23, 55, 0, 500_000_000)))
        assertEquals(Duration.ofMinutes(5), window.remaining(kst(0, 5)))
        assertEquals(300, window.remainingSeconds(kst(0, 5)))
        assertEquals(Duration.ZERO, window.remaining(kst(12, 0)))
        assertEquals(0, window.remainingSeconds(kst(12, 0)))
    }

    @Test
    fun `꺼져 있으면 언제나 점검 아님`() {
        val off = BankMaintenanceWindow(LocalTime.of(23, 50), LocalTime.of(0, 10), seoul, enabled = false)

        assertFalse(off.isActive(kst(23, 55)))
        assertEquals(Duration.ZERO, off.remaining(kst(23, 55)))
    }

    @Test
    fun `시계는 UTC 로 주고 판정은 KST - LocalTime_now(clock) 을 그대로 쓰는 실수를 잡는다`() {
        val utc1450 = Clock.fixed(Instant.parse("2026-10-28T14:50:00Z"), ZoneOffset.UTC)   // = KST 23:50
        val utc2350 = Clock.fixed(Instant.parse("2026-10-28T23:50:00Z"), ZoneOffset.UTC)   // = KST 다음 날 08:50

        assertTrue(window.isActive(utc1450.instant()))
        assertFalse(window.isActive(utc2350.instant()))
        assertEquals(LocalTime.of(14, 50), LocalTime.now(utc1450))   // 시계의 시간대(UTC)로 읽으면 점검이 아닌 것처럼 보인다
    }

    @Test
    fun `시작과 끝이 같으면 거부`() {
        assertFailsWith<IllegalArgumentException> { BankMaintenanceWindow(LocalTime.of(1, 0), LocalTime.of(1, 0), seoul) }
    }
}
```

`BankGatewayClientTest.kt` — 클래스 끝(마지막 `}` 앞)에 추가 (필요한 import 는 이미 있다):
```kotlin

    @Test
    fun `보내기 문이 닫혀 있으면(점검 시간) 연결이 있어도 쓰지 않고 BankNotSentException - 폴링은 계속된다`() {
        val bank = MockBankServer().start().closeLater()
        var closed = true
        val client = BankGatewayClient(
            BankGatewayConfig(host = "127.0.0.1", port = bank.boundPort, pollInterval = Duration.ofMillis(200)),
            sendGate = { if (closed) "bank maintenance window" else null },
        ).closeLater().start()
        waitUntil { client.isConnected }

        val error = client.transfer("090", "1002123456789", 1_000, "점검", "T261028MAINTENANCE01").awaitFailure()

        assertIs<BankNotSentException>(error)
        assertEquals("bank maintenance window", error.message)
        assertFalse(bank.hasProcessed("T261028MAINTENANCE01"))
        val polls = bank.pollCount
        waitUntil { bank.pollCount > polls }   // 링크 체크는 막지 않는다 — 점검이 끝나면 바로 보낼 수 있다
        closed = false
        assertEquals(ResponseCodes.ORIGINAL_NOT_FOUND, client.inquire("T261028MAINTENANCE01").get(3, TimeUnit.SECONDS).result)
    }
```

- [ ] **Step 2: 실패 확인**

Run: `./gradlew :modules:bank-gateway:test`
Expected: FAIL (컴파일: `Unresolved reference 'BankMaintenanceWindow'`, `No parameter with name 'sendGate'`)

- [ ] **Step 3: 구현**

`BankMaintenanceWindow.kt`:
```kotlin
package io.github.hyeokjinon.banktransfer.gateway

import java.time.Duration
import java.time.Instant
import java.time.LocalTime
import java.time.ZoneId

/**
 * 은행 점검 시간 (P5-1). 시작 포함 · 끝 제외, [start] > [end] 면 자정을 넘는 구간이다 (기본 23:50 ~ 00:10 KST).
 * 점검 중에는 아무것도 은행에 보내지 않는다 → "D일에 보낸 출금" 과 "D일 대사 파일" 이 같은 범위가 된다.
 * 판단은 늘 [zone] 의 벽시계로 한다: Instant 는 시간대가 없으므로 `LocalTime.now(clock)` 처럼 시계의 시간대(서버는 UTC)로 읽으면 9시간 어긋난다.
 * 순수 객체다 — 지금 시각은 부르는 쪽이 주입한 Clock 에서 넘긴다 (테스트가 시각을 고정할 수 있게).
 * 점검 길이는 재발행 포기(E001)까지 걸리는 약 26분보다 짧게 둔다 — 길면 점검 때문에 포기하는 출금이 생긴다.
 */
class BankMaintenanceWindow(
    val start: LocalTime,
    val end: LocalTime,
    val zone: ZoneId,
    val enabled: Boolean = true,
) {
    init {
        require(start != end) { "maintenance start and end must differ: $start" }
    }

    private val crossesMidnight = start > end

    /** 점검 길이 (자정을 넘으면 다음 날 끝까지). */
    val length: Duration = Duration.between(start, end).let { if (it.isNegative) it.plusDays(1) else it }

    fun isActive(now: Instant): Boolean {
        if (!enabled) return false
        val time = LocalTime.ofInstant(now, zone)
        return if (crossesMidnight) time >= start || time < end else time >= start && time < end
    }

    /** 점검이 끝날 때까지 남은 시간. 점검 중이 아니면 0. */
    fun remaining(now: Instant): Duration {
        if (!isActive(now)) return Duration.ZERO
        val local = now.atZone(zone)
        // 자정 전(시작 쪽)이면 끝은 다음 날이다
        val endDate = if (crossesMidnight && local.toLocalTime() >= start) local.toLocalDate().plusDays(1) else local.toLocalDate()
        return Duration.between(now, endDate.atTime(end).atZone(zone).toInstant())
    }

    /** `Retry-After` 초 — 남은 시간을 올림한다 (899.5초 → 900). 점검 중이 아니면 0. */
    fun remainingSeconds(now: Instant): Long {
        val remaining = remaining(now)
        return remaining.seconds + if (remaining.nano > 0) 1 else 0
    }
}
```

`BankGatewayClient.kt`:
1) `class BankNotSentException …` 줄 아래에 추가:
```kotlin

/**
 * 지금 은행에 보내도 되는지 (P5-1, 점검 시간). 닫혀 있으면 이유를 돌려준다 — 그 요청은 "연결 없음" 과 같이 [BankNotSentException] 으로 끝난다:
 * 이체는 RECEIVED 로 돌아가 재시도 · 재발행, 결과조회는 시도로 세지 않는다 (어댑터는 두 이유를 구분하지 않는다).
 */
fun interface SendGate {
    fun closedReason(): String?

    companion object {
        val OPEN = SendGate { null }
    }
}
```
2) 생성자를 바꾼다:
```kotlin
class BankGatewayClient(
    private val config: BankGatewayConfig,
    private val clock: Clock = Clock.systemDefaultZone(),
    /** 점검 시간 등으로 보내지 않을 때 (P5-1). 기본은 늘 열림. */
    private val sendGate: SendGate = SendGate.OPEN,
) : AutoCloseable {
```
3) `fun send(request: BankMessage)` 의 KDoc 마지막 줄(` * 쓰기 실패 · 보낸 뒤 연결 끊김 -> …`) 아래에 ` * 보내기 문이 닫혀 있으면(점검 시간, P5-1) 연결이 있어도 쓰지 않고 BankNotSentException 이다. 폴링은 send 를 거치지 않아 점검 중에도 회선은 유지된다.` 를 더하고, 본문 첫 줄(`val ch = channel`) 앞에 추가:
```kotlin
        sendGate.closedReason()?.let { return CompletableFuture.failedFuture(BankNotSentException(it)) }
```

- [ ] **Step 4: 통과 확인**

Run: `./gradlew :modules:bank-gateway:test`
Expected: PASS (새 테스트 8개 + 기존 게이트웨이 테스트)

- [ ] **Step 5: 커밋**

```bash
git add modules/bank-gateway
git commit -m "feat(gateway): 은행 점검 시간 BankMaintenanceWindow (KST 판정 · 자정 넘김 · 올림) · 보내기 문 SendGate

점검 중에는 연결이 있어도 전문을 쓰지 않고 '연결 없음' 과 같은 BankNotSentException 으로 끝낸다 — 이체 · 결과조회 · 1원인증이
모두 send() 를 지나므로 한 곳에서 막으면 빠뜨릴 수 없다. 폴링은 막지 않아 점검이 끝나면 바로 보낸다.
시계는 UTC 여도 판정은 KST 벽시계로 한다 (LocalTime.now(clock) 은 9시간 어긋난다).

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 4: API — 앱 시계 빈 하나 · 점검 설정 · 점검 중 게이트웨이 송신 보류 · 테스트 시계

**Files:**
- Modify: `apps/api/src/main/kotlin/io/github/hyeokjinon/banktransfer/api/config/AppConfiguration.kt` (전체 교체 — 아래)
- Modify: `apps/api/src/main/kotlin/io/github/hyeokjinon/banktransfer/api/messaging/MessagingConfiguration.kt`
- Modify: `apps/api/src/main/kotlin/io/github/hyeokjinon/banktransfer/api/reconcile/ReconcileConfiguration.kt`
- Modify: `apps/api/src/main/resources/application.yml`
- Create (test): `apps/api/src/test/kotlin/io/github/hyeokjinon/banktransfer/api/support/TestClock.kt`
- Modify (test): `apps/api/src/test/kotlin/io/github/hyeokjinon/banktransfer/api/support/ApiIntegrationTest.kt`
- Test: `apps/api/src/test/kotlin/io/github/hyeokjinon/banktransfer/api/messaging/BankMaintenanceFlowTest.kt` (신규)

**Interfaces:**
- Consumes: Task 3 `BankMaintenanceWindow`, `BankGatewayClient(config, sendGate = …)`
- Produces:
  - 빈 `clock: Clock` (= `Clock.systemUTC()`), 빈 `bankMaintenanceWindow: BankMaintenanceWindow`
  - `@ConfigurationProperties("bank.maintenance") data class BankMaintenanceProperties(enabled: Boolean = true, start: String = "23:50", end: String = "00:10") { fun toWindow(): BankMaintenanceWindow }`
  - 테스트: `class TestClock(window: BankMaintenanceWindow) : Clock { fun fix(at: Instant); fun release() }`, `@TestConfiguration class TestClockConfiguration`(`@Primary` `TestClock` 빈), `ApiIntegrationTest.testClock: TestClock` (테스트마다 `release()`)

결정(계획): `Clock.systemUTC()` 배선 전부를 빈으로, `TestClock` 은 진짜 시각이 점검 시간이면 비켜 감, 시각은 문자열 `"HH:mm"` + yml 따옴표 (위 "결정(계획) 모음").

- [ ] **Step 1: 실패하는 테스트**

`support/TestClock.kt`:
```kotlin
package io.github.hyeokjinon.banktransfer.api.support

import io.github.hyeokjinon.banktransfer.gateway.BankMaintenanceWindow
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Primary
import java.time.Clock
import java.time.Instant
import java.time.ZoneId
import java.time.ZoneOffset

/**
 * 통합 테스트의 앱 시계 (P5-1). 평소에는 진짜 시각이다.
 * - [fix]: 점검 시간 · 대사 테스트가 시각을 정한다. [ApiIntegrationTest] 가 테스트마다 [release] 로 되돌린다.
 * - 진짜 시각이 점검 시간(기본 23:50 ~ 00:10 KST)에 걸리면 점검 직전으로 비켜 간다 — 밤에 CI · 개발 PC 에서 도는 다른 테스트가 503 으로 흔들리지 않게.
 *   비켜 가는 동안 시각은 (점검 길이 + 1분) 만큼 앞의 같은 간격을 지나므로, 점검 시작 · 끝 순간에 걸친 테스트만 시각이 그만큼 뒤로 · 앞으로 뛴다.
 */
class TestClock(private val window: BankMaintenanceWindow) : Clock() {
    @Volatile
    private var fixed: Instant? = null

    fun fix(at: Instant) {
        fixed = at
    }

    fun release() {
        fixed = null
    }

    override fun instant(): Instant =
        fixed ?: Instant.now().let { now -> if (window.isActive(now)) now.minus(window.length).minusSeconds(60) else now }

    override fun getZone(): ZoneId = ZoneOffset.UTC

    override fun withZone(zone: ZoneId): Clock = throw UnsupportedOperationException("TestClock is UTC only, like Clock.systemUTC()")
}

/** 앱의 Clock 빈(Clock.systemUTC())보다 먼저 주입된다. 진짜 시각을 따르되 테스트가 고정할 수 있다. */
@TestConfiguration(proxyBeanMethods = false)
class TestClockConfiguration {

    @Bean
    @Primary
    fun testClock(window: BankMaintenanceWindow) = TestClock(window)
}
```

`ApiIntegrationTest.kt`:
1) import 추가: `org.junit.jupiter.api.AfterEach`.
2) `@Import(MySqlTestContainer::class, KafkaTestContainer::class, RedisTestContainer::class)` 를 `@Import(MySqlTestContainer::class, KafkaTestContainer::class, RedisTestContainer::class, TestClockConfiguration::class)` 로.
3) KDoc 마지막 줄(`Redis 컨테이너는 …`) 아래에 ` * 앱 시계는 [testClock] 이다 (P5-1): 점검 시간 · 대사 테스트는 fix 로 시각을 정하고, 테스트가 끝나면 진짜 시각으로 돌아간다.` 를 더한다.
4) `@Autowired protected lateinit var wallets: WalletLedger` 아래에 추가:
```kotlin

    @Autowired
    protected lateinit var testClock: TestClock
```
5) `@BeforeEach fun waitForBankConnection() …` 아래에 추가:
```kotlin

    /** 시각을 고정한 테스트가 다음 테스트 · 백그라운드 컨슈머에 고정 시각을 남기지 않게. */
    @AfterEach
    fun releaseClock() = testClock.release()
```

`messaging/BankMaintenanceFlowTest.kt`:
```kotlin
package io.github.hyeokjinon.banktransfer.api.messaging

import io.github.hyeokjinon.banktransfer.api.support.ApiIntegrationTest
import io.github.hyeokjinon.banktransfer.api.web.USER_ID_HEADER
import io.github.hyeokjinon.banktransfer.gateway.BankMaintenanceWindow
import io.github.hyeokjinon.banktransfer.transfer.TransferReconciler
import io.github.hyeokjinon.banktransfer.transfer.TransferStatus
import io.github.hyeokjinon.banktransfer.transfer.TransferStore
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.core.ParameterizedTypeReference
import org.springframework.http.MediaType
import java.time.Duration
import java.time.Instant
import java.time.LocalDate
import java.time.LocalTime
import java.time.ZoneId
import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

/**
 * 은행 점검 시간 흐름 (P5-1). 점검 중에는 아무것도 은행에 가지 않는다 — 점검 전에 접수된 출금은 게이트웨이의 보내기 문에 막혀
 * "연결 없음" 과 같은 길(RECEIVED → 재시도 토픽 → DLT → 재발행)을 타고, 점검이 끝나면 처리된다. 시계를 옮겨 만든다.
 */
class BankMaintenanceFlowTest : ApiIntegrationTest() {

    @Autowired private lateinit var relay: OutboxRelay
    @Autowired private lateinit var store: TransferStore
    @Autowired private lateinit var reconciler: TransferReconciler
    @Autowired private lateinit var window: BankMaintenanceWindow

    private val seoul = ZoneId.of("Asia/Seoul")
    private val mapType = object : ParameterizedTypeReference<Map<String, Any?>>() {}

    private fun kst(day: LocalDate, h: Int, m: Int): Instant = day.atTime(h, m).atZone(seoul).toInstant()

    @Test
    fun `기본 설정은 23시 50분 ~ 00시 10분 (KST), 켜짐 - yml 의 시각은 따옴표로 문자열`() {
        assertTrue(window.enabled)
        assertEquals(LocalTime.of(23, 50), window.start)
        assertEquals(LocalTime.of(0, 10), window.end)
        assertEquals(seoul, window.zone)
    }

    @Test
    fun `점검 전에 접수된 출금은 점검 중 은행에 가지 않고 RECEIVED - 점검이 끝나면 COMPLETED`() {
        val user = fundedUser()
        val account = verifiedBy(user, "1002123456789")
        val day = LocalDate.now(seoul)

        testClock.fix(kst(day, 23, 49))   // 점검 직전 접수 (발행기는 테스트에서 꺼져 있어 아직 은행에 가지 않는다)
        val body = client.post().uri("/api/v1/transfers")
            .contentType(MediaType.APPLICATION_JSON)
            .header(USER_ID_HEADER, user)
            .header("Idempotency-Key", UUID.randomUUID().toString())
            .body(mapOf("bankCode" to "090", "accountNo" to account, "amount" to 10_000, "memo" to "점검"))
            .retrieve().body(mapType)!!
        val id = UUID.fromString(body["id"] as String)
        val reference = store.findById(id)!!.reference

        testClock.fix(kst(day, 23, 55))   // 점검 중 발행 → 컨슈머가 보내려 했지만 보내기 문이 닫혀 있다
        await.atMost(Duration.ofSeconds(10)) untilAsserted {
            relay.publishPending()
            val transfer = store.findById(id)!!
            assertEquals(TransferStatus.RECEIVED, transfer.status)
            assertNotNull(transfer.sendingAt)   // 보내려 한 흔적 — 재발행 대상이 된다
        }
        assertFalse(bank.hasProcessed(reference))

        testClock.fix(kst(day.plusDays(1), 0, 11))   // 점검이 끝났다: 재시도 토픽이 남아 있으면 그쪽이, DLT 로 갔으면 재발행이 처리한다
        await.atMost(Duration.ofSeconds(20)) untilAsserted {
            reconciler.reconcile()
            relay.publishPending()
            assertEquals(TransferStatus.COMPLETED, store.findById(id)!!.status)
        }
        assertEquals(1, bank.depositsOf(account).count { it.reference == reference })   // 한 번만 갔다
    }
}
```

- [ ] **Step 2: 실패 확인**

Run: `./gradlew :apps:api:test --tests '*BankMaintenanceFlowTest*'`
Expected: FAIL — 컨텍스트 기동 실패 (`No qualifying bean of type 'io.github.hyeokjinon.banktransfer.gateway.BankMaintenanceWindow'` — `TestClockConfiguration` 이 창 빈을 찾는다)

- [ ] **Step 3: 구현**

`AppConfiguration.kt` 전체를 다음으로 바꾼다 (기존 내용을 유지하면서 시계 빈 · 점검 설정 · 게이트 연결을 더한 것):
```kotlin
package io.github.hyeokjinon.banktransfer.api.config

import io.github.hyeokjinon.banktransfer.account.AccountVerificationService
import io.github.hyeokjinon.banktransfer.account.BankPort
import io.github.hyeokjinon.banktransfer.account.RandomCodeGenerator
import io.github.hyeokjinon.banktransfer.account.VerificationRepository
import io.github.hyeokjinon.banktransfer.api.persistence.AccountVerificationJpaRepository
import io.github.hyeokjinon.banktransfer.api.persistence.JpaTransferStore
import io.github.hyeokjinon.banktransfer.api.persistence.JpaVerificationRepository
import io.github.hyeokjinon.banktransfer.api.persistence.OutboxEventJpaRepository
import io.github.hyeokjinon.banktransfer.api.persistence.TransferJpaRepository
import io.github.hyeokjinon.banktransfer.api.persistence.WalletDebit
import io.github.hyeokjinon.banktransfer.api.persistence.WalletLedger
import io.github.hyeokjinon.banktransfer.gateway.BankGatewayClient
import io.github.hyeokjinon.banktransfer.gateway.BankGatewayConfig
import io.github.hyeokjinon.banktransfer.gateway.BankMaintenanceWindow
import io.github.hyeokjinon.banktransfer.protocol.ResponseCodes
import io.github.hyeokjinon.banktransfer.transfer.RandomReferenceGenerator
import io.github.hyeokjinon.banktransfer.transfer.ReferenceGenerator
import io.github.hyeokjinon.banktransfer.transfer.TransferService
import io.github.hyeokjinon.banktransfer.transfer.TransferStore
import io.github.hyeokjinon.banktransfer.transfer.VerifiedAccounts
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import org.springframework.transaction.support.TransactionTemplate
import java.time.Clock
import java.time.Duration
import java.time.LocalTime
import java.time.ZoneId
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executor

@ConfigurationProperties("bank.gateway")
data class BankGatewayProperties(
    val host: String = "localhost",
    val port: Int = 9090,
    val responseTimeout: Duration = Duration.ofSeconds(3),
    val pollInterval: Duration = Duration.ofSeconds(30),
)

/**
 * 은행 점검 시간 (P5-1). KST 벽시계 "HH:mm" — 시작 포함 · 끝 제외, 시작 > 끝이면 자정을 넘는다.
 * 문자열로 받아 LocalTime.parse 로 읽는다. application.yml 에서는 따옴표가 필요하다: YAML 1.1 은 23:50 을 60진수 정수 1430 으로 읽는다.
 * 점검 길이는 재발행 포기(E001)까지 걸리는 약 26분보다 짧게 둔다 — 길면 점검 때문에 포기하는 출금이 생긴다.
 */
@ConfigurationProperties("bank.maintenance")
data class BankMaintenanceProperties(
    val enabled: Boolean = true,
    val start: String = "23:50",
    val end: String = "00:10",
) {
    fun toWindow() = BankMaintenanceWindow(LocalTime.parse(start), LocalTime.parse(end), ZoneId.of("Asia/Seoul"), enabled)
}

@ConfigurationProperties("account.verification")
data class VerificationProperties(
    val ttl: Duration = Duration.ofMinutes(5),
    val maxAttempts: Int = 5,
)

/** 은행이 처리를 거절한 경우(응답코드가 0000 이 아님). */
class BankRejectedException(val responseCode: String) : RuntimeException("bank rejected with code $responseCode")

/**
 * 조립(wiring)은 여기서만 한다. account 모듈은 BankPort 인터페이스만 알고,
 * 실제로 Netty 게이트웨이를 붙이는 건 이 어댑터다 (포트-어댑터 구조).
 *
 * 은행 응답 future 는 Netty 이벤트 루프가 완료한다. 그 뒤의 처리(DB 저장 등 블로킹)가 이벤트 루프에서 돌지 않도록
 * thenApplyAsync 로 앱 실행기에 넘긴다. 이벤트 루프가 막히면 같은 루프의 모든 은행 통신이 멈춘다.
 */
class BankGatewayAdapter(
    private val client: BankGatewayClient,
    private val executor: Executor,
    /** 1원인증 이체의 이용기관 거래번호 (V 접두). 결과조회 대상이 아니라 저장하지 않는다. */
    private val references: ReferenceGenerator = RandomReferenceGenerator(prefix = 'V'),
) : BankPort {
    override fun inquireHolderName(bankCode: String, accountNo: String): CompletableFuture<String?> =
        client.inquireName(bankCode, accountNo).thenApplyAsync({ res ->
            when (res.header.responseCode) {
                ResponseCodes.OK -> res.holderName
                ResponseCodes.ACCOUNT_NOT_FOUND -> null
                else -> throw BankRejectedException(res.header.responseCode)
            }
        }, executor)

    override fun transfer(bankCode: String, accountNo: String, amount: Long, memo: String): CompletableFuture<Unit> =
        client.transfer(bankCode, accountNo, amount, memo, references.next()).thenApplyAsync({ res ->
            if (!res.header.isOk) throw BankRejectedException(res.header.responseCode)
        }, executor)
}

@Configuration(proxyBeanMethods = false)
class AppConfiguration {

    /**
     * 앱 전체가 보는 시계 하나 (P5-1). 점검 시간 판단 · 상태 시각(sending_at · updated_at) · 대사가 같은 "지금" 을 보게 한다 —
     * 따로 Clock.systemUTC() 를 쓰면 테스트가 시각을 옮겨도 일부만 따라온다. 테스트는 @Primary TestClock 으로 바꾼다.
     */
    @Bean
    fun clock(): Clock = Clock.systemUTC()

    @Bean
    fun bankMaintenanceWindow(props: BankMaintenanceProperties): BankMaintenanceWindow = props.toWindow()

    @Bean(destroyMethod = "close")
    fun bankGatewayClient(props: BankGatewayProperties, maintenance: BankMaintenanceWindow, clock: Clock): BankGatewayClient =
        BankGatewayClient(
            BankGatewayConfig(
                host = props.host,
                port = props.port,
                responseTimeout = props.responseTimeout,
                pollInterval = props.pollInterval,
            ),
            // 점검 중에는 이체 · 결과조회 · 1원인증 전문을 보내지 않는다 (P5-1). "연결 없음" 과 같은 길이라
            // 이체는 RECEIVED 로 돌아가 재시도 · 재발행, 결과조회는 횟수를 세지 않는다. 웹에서 먼저 막혀도 여기가 마지막 보장이다
            sendGate = { if (maintenance.isActive(clock.instant())) "bank maintenance window" else null },
        ).start()

    @Bean
    fun bankPort(client: BankGatewayClient, @Qualifier("applicationTaskExecutor") executor: Executor, clock: Clock): BankPort =
        BankGatewayAdapter(client, executor, RandomReferenceGenerator(prefix = 'V', clock = clock))

    @Bean
    fun verificationRepository(jpa: AccountVerificationJpaRepository, tx: TransactionTemplate, clock: Clock): VerificationRepository =
        JpaVerificationRepository(jpa, tx, clock)

    /** 잔고 SQL 한 곳 (P4-1). 실패 확정 트랜잭션 안에서 JpaTransferStore 가 환불을 부르고, 충전 · 조회는 WalletController 가 부른다. */
    @Bean
    fun walletLedger(jdbc: NamedParameterJdbcTemplate, tx: TransactionTemplate, clock: Clock) = WalletLedger(jdbc, tx, clock)

    @Bean
    fun transferStore(
        transfers: TransferJpaRepository,
        outbox: OutboxEventJpaRepository,
        tx: TransactionTemplate,
        wallets: WalletLedger,
        debit: WalletDebit,
    ): TransferStore = JpaTransferStore(transfers, outbox, tx, wallets, debit)

    /** transfer 모듈은 account 모듈을 모른다. "인증된 계좌인가?"를 1원인증 저장소에 연결하는 곳은 여기뿐이다. */
    @Bean
    fun transferService(store: TransferStore, verifications: VerificationRepository, clock: Clock) = TransferService(
        store = store,
        verifiedAccounts = VerifiedAccounts { userId, bankCode, accountNo -> verifications.existsVerified(userId, bankCode, accountNo) },
        referenceGenerator = RandomReferenceGenerator(prefix = 'T', clock = clock),
        clock = clock,
    )

    @Bean
    fun accountVerificationService(
        bankPort: BankPort,
        repository: VerificationRepository,
        props: VerificationProperties,
        clock: Clock,
    ) = AccountVerificationService(
        bank = bankPort,
        repository = repository,
        codeGenerator = RandomCodeGenerator(),
        clock = clock,
        ttl = props.ttl,
        maxAttempts = props.maxAttempts,
    )
}
```

`MessagingConfiguration.kt` 의 `transferProcessor` 빈을 바꾼다 (`import java.time.Clock` 은 그대로 쓴다):
```kotlin
    @Bean
    fun transferProcessor(store: TransferStore, client: BankGatewayClient, clock: Clock) =
        TransferProcessor(store, BankTransferAdapter(client), clock)
```

`ReconcileConfiguration.kt` 의 `transferReconciler` 빈 시그니처와 `clock =` 줄을 바꾼다:
```kotlin
    @Bean
    fun transferReconciler(store: TransferStore, client: BankGatewayClient, props: ReconcileProperties, clock: Clock) =
        TransferReconciler(
            store = store,
            bank = BankInquiryAdapter(client),
            clock = clock,
```
(나머지 인자는 그대로.)

`application.yml` 의 `bank:` 절을 다음으로 바꾼다 (`gateway` 는 그대로, `maintenance` 추가):
```yaml
bank:
  gateway:
    host: localhost
    port: 9090
    response-timeout: 3s
    poll-interval: 30s
  maintenance:                 # 은행 점검 시간 (P5-1, KST). 점검 중에는 출금 접수 · 1원인증 503, 은행 송신 보류
    enabled: true
    start: "23:50"             # 따옴표 필수 — YAML 은 23:50 을 60진수 정수로 읽는다. 시작 포함 · 끝 제외, 자정을 넘어도 된다
    end: "00:10"               # 점검 길이는 재발행 포기(E001)까지의 약 26분보다 짧게 (OPERATION §9)
```

- [ ] **Step 4: 통과 확인**

Run: `./gradlew :apps:api:test --tests '*BankMaintenanceFlowTest*'`
Expected: PASS (2개)

Run: `./gradlew :apps:api:test`
Expected: PASS — 기존 통합 테스트 전부 (시계가 `TestClock` 으로 바뀌어도 고정하지 않으면 진짜 시각)

- [ ] **Step 5: 커밋**

```bash
git add apps/api
git commit -m "feat(api): 앱 시계 빈 하나 · 점검 설정(bank.maintenance.*) · 점검 중 게이트웨이 송신 보류

점검 판단과 상태 시각이 같은 시계를 봐야 해서 Clock.systemUTC() 배선을 빈 하나로 모은다.
점검 전에 접수된 출금은 점검 중 보내기 문에 막혀 RECEIVED 로 돌아갔다가 점검이 끝나면 재시도 · 재발행으로 처리된다.
테스트 시계는 진짜 시각이 점검 시간이면 비켜 가 밤에 도는 테스트가 503 으로 흔들리지 않는다.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 5: 웹 — 점검 시간 503 `BANK_MAINTENANCE` + `Retry-After`

**Files:**
- Create: `apps/api/src/main/kotlin/io/github/hyeokjinon/banktransfer/api/web/BankMaintenanceGuard.kt`
- Modify: `apps/api/src/main/kotlin/io/github/hyeokjinon/banktransfer/api/web/TransferController.kt`, `AccountVerificationController.kt`, `ApiExceptionHandler.kt`
- Modify: `apps/api/src/main/kotlin/io/github/hyeokjinon/banktransfer/api/config/AppConfiguration.kt` (가드 빈)
- Test: `apps/api/src/test/kotlin/io/github/hyeokjinon/banktransfer/api/web/ApiExceptionHandlerTest.kt` (테스트 1개), `apps/api/src/test/kotlin/io/github/hyeokjinon/banktransfer/api/BankMaintenanceApiTest.kt` (신규)

**Interfaces:**
- Consumes: Task 4 `BankMaintenanceWindow` · `Clock` 빈, `ApiIntegrationTest.testClock`
- Produces:
  - `class BankMaintenanceException(val retryAfterSeconds: Long) : RuntimeException`
  - `class BankMaintenanceGuard(window: BankMaintenanceWindow, clock: Clock) { fun ensureOpen() }` (빈 `bankMaintenanceGuard`)
  - `ApiExceptionHandler`: `BankMaintenanceException` → 503 `BANK_MAINTENANCE` + `Retry-After: <retryAfterSeconds>`; 헤더 결정은 `private fun Throwable.retryAfter(): String?` 한 곳

- [ ] **Step 1: 실패하는 테스트**

`ApiExceptionHandlerTest.kt` — 클래스 끝(마지막 `}` 앞)에 추가:
```kotlin

    @Test
    fun `점검 시간이면 503 BANK_MAINTENANCE + Retry-After 는 점검이 끝날 때까지 남은 초`() {
        val response = handler.handle(BankMaintenanceException(900))

        assertEquals(503, response.statusCode.value())
        assertEquals("BANK_MAINTENANCE", response.body!!.properties?.get("code"))
        assertEquals("900", response.headers.getFirst(HttpHeaders.RETRY_AFTER))
    }
```

`BankMaintenanceApiTest.kt`:
```kotlin
package io.github.hyeokjinon.banktransfer.api

import io.github.hyeokjinon.banktransfer.api.support.ApiIntegrationTest
import io.github.hyeokjinon.banktransfer.api.web.USER_ID_HEADER
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.core.ParameterizedTypeReference
import org.springframework.http.HttpHeaders
import org.springframework.http.MediaType
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.web.client.RestClient
import java.time.LocalDate
import java.time.ZoneId
import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

/**
 * 점검 시간의 웹 (P5-1): 은행을 건드리는 요청(출금 접수, 1원인증 요청 · 확인)만 503 BANK_MAINTENANCE + Retry-After(남은 초, 올림).
 * 조회 · 지갑 충전은 그대로. 시각은 23:55:00.5 (KST) 로 고정 → 00:10 까지 899.5초 → Retry-After 900.
 */
class BankMaintenanceApiTest : ApiIntegrationTest() {

    @Autowired private lateinit var jdbc: JdbcTemplate

    private val seoul = ZoneId.of("Asia/Seoul")
    private val mapType = object : ParameterizedTypeReference<Map<String, Any?>>() {}

    private data class Response(val status: Int, val body: Map<String, Any?>, val retryAfter: String?)

    private fun send(request: RestClient.RequestHeadersSpec<*>): Response =
        request.exchange { _, r -> Response(r.statusCode.value(), r.bodyTo(mapType) ?: emptyMap(), r.headers.getFirst(HttpHeaders.RETRY_AFTER)) }

    private fun at(h: Int, m: Int, s: Int = 0, millis: Long = 0) =
        testClock.fix(LocalDate.now(seoul).atTime(h, m, s).atZone(seoul).toInstant().plusMillis(millis))

    private fun postTransfer(user: String, account: String) = send(
        client.post().uri("/api/v1/transfers")
            .contentType(MediaType.APPLICATION_JSON)
            .header(USER_ID_HEADER, user)
            .header("Idempotency-Key", UUID.randomUUID().toString())
            .body(mapOf("bankCode" to "090", "accountNo" to account, "amount" to 10_000, "memo" to "점검")),
    )

    @Test
    fun `점검 중 출금 접수는 503 BANK_MAINTENANCE + Retry-After 900 - 출금도 차감도 없다`() {
        val user = fundedUser(100_000)
        val account = verifiedBy(user, unregisteredAccount())
        at(23, 55, 0, millis = 500)

        val response = postTransfer(user, account)

        assertEquals(503, response.status)
        assertEquals("BANK_MAINTENANCE", response.body["code"])
        assertEquals("900", response.retryAfter)
        assertEquals(0, jdbc.queryForObject("SELECT COUNT(*) FROM transfer WHERE user_id = ?", Int::class.java, user))
        assertEquals(100_000L, wallets.balanceOf(user))
    }

    @Test
    fun `점검 중 1원인증 요청 · 확인도 503 + Retry-After`() {
        val user = newUserId()
        at(0, 5)

        val request = send(
            client.post().uri("/api/v1/account-verifications")
                .contentType(MediaType.APPLICATION_JSON)
                .header(USER_ID_HEADER, user)
                .body(mapOf("bankCode" to "090", "accountNo" to "1002123456789", "holderName" to "홍길동")),
        )
        val confirm = send(
            client.post().uri("/api/v1/account-verifications/${UUID.randomUUID()}/confirm")
                .contentType(MediaType.APPLICATION_JSON)
                .header(USER_ID_HEADER, user)
                .body(mapOf("code" to "1234")),
        )

        assertEquals(503 to "BANK_MAINTENANCE", request.status to request.body["code"])
        assertEquals("300", request.retryAfter)
        assertEquals(503 to "BANK_MAINTENANCE", confirm.status to confirm.body["code"])
    }

    @Test
    fun `점검 중에도 잔고 조회 · 충전 · 출금 조회는 그대로`() {
        val user = newUserId()
        at(23, 59)

        val deposit = send(
            client.post().uri("/api/v1/wallet/deposits").contentType(MediaType.APPLICATION_JSON)
                .header(USER_ID_HEADER, user).body(mapOf("amount" to 5_000)),
        )
        val wallet = send(client.get().uri("/api/v1/wallet").header(USER_ID_HEADER, user))
        val transfer = send(client.get().uri("/api/v1/transfers/${UUID.randomUUID()}").header(USER_ID_HEADER, user))

        assertEquals(200, deposit.status)
        assertEquals(200 to 5_000, wallet.status to wallet.body["balance"])
        assertEquals(404 to "TRANSFER_NOT_FOUND", transfer.status to transfer.body["code"])
        assertNull(transfer.retryAfter)
    }

    @Test
    fun `점검 직전 · 끝나는 순간(끝 제외)에는 접수된다`() {
        val user = fundedUser(100_000)
        val account = verifiedBy(user, unregisteredAccount())

        at(23, 49, 59, millis = 999)
        assertEquals(202, postTransfer(user, account).status)
        at(0, 10)
        assertEquals(202, postTransfer(user, account).status)
    }
}
```

- [ ] **Step 2: 실패 확인**

Run: `./gradlew :apps:api:test --tests '*ApiExceptionHandlerTest*' --tests '*BankMaintenanceApiTest*'`
Expected: FAIL (컴파일: `Unresolved reference 'BankMaintenanceException'`)

- [ ] **Step 3: 구현**

`web/BankMaintenanceGuard.kt`:
```kotlin
package io.github.hyeokjinon.banktransfer.api.web

import io.github.hyeokjinon.banktransfer.gateway.BankMaintenanceWindow
import java.time.Clock

/** 점검 시간에 은행을 건드리는 요청을 받지 않는다 (P5-1). [retryAfterSeconds] = 점검이 끝날 때까지 남은 초 (올림). */
class BankMaintenanceException(val retryAfterSeconds: Long) :
    RuntimeException("bank is under maintenance, retry after $retryAfterSeconds seconds")

/**
 * 은행을 건드리는 웹 요청(출금 접수, 1원인증 요청 · 확인)의 첫 줄에서 부른다 (P5-1). 점검 중이면 503 BANK_MAINTENANCE + Retry-After.
 * 조회 · 지갑 충전 · 관리 API 는 부르지 않는다. 웹을 거치지 않는 은행 호출(컨슈머 · 정리 작업)은 게이트웨이의 보내기 문이 막는다.
 */
class BankMaintenanceGuard(private val window: BankMaintenanceWindow, private val clock: Clock) {

    fun ensureOpen() {
        val now = clock.instant()
        if (window.isActive(now)) throw BankMaintenanceException(window.remainingSeconds(now))
    }
}
```

`AppConfiguration.kt` — `bankMaintenanceWindow` 빈 아래에 추가 (import `io.github.hyeokjinon.banktransfer.api.web.BankMaintenanceGuard`):
```kotlin

    @Bean
    fun bankMaintenanceGuard(window: BankMaintenanceWindow, clock: Clock) = BankMaintenanceGuard(window, clock)
```

`TransferController.kt`:
1) 클래스 선언을 `class TransferController(private val service: TransferService, private val maintenance: BankMaintenanceGuard) {` 로.
2) `request(…)` KDoc 끝에 ` * 은행 점검 시간에는 받지 않는다 — 503 BANK_MAINTENANCE + Retry-After (P5-1).` 를 더하고, 본문 첫 줄에 `maintenance.ensureOpen()` 을 넣는다.

`AccountVerificationController.kt`:
1) 클래스 선언을 `class AccountVerificationController(private val service: AccountVerificationService, private val maintenance: BankMaintenanceGuard) {` 로.
2) `request` 를 표현식 본문에서 블록 본문으로 바꾼다:
```kotlin
    @PostMapping
    fun request(user: RequestUser, @Valid @RequestBody body: VerificationRequest): CompletableFuture<ResponseEntity<VerificationResponse>> {
        maintenance.ensureOpen()   // 점검 중에는 성명조회 · 1원 이체를 보내지 않는다 (P5-1) — 비동기로 넘기기 전에 거절한다
        return service.request(RequestVerification(user.id, body.bankCode, body.accountNo, body.holderName))
            .thenApply { ResponseEntity.status(HttpStatus.CREATED).body(VerificationResponse.from(it)) }
    }
```
3) `confirm` 도 블록 본문으로:
```kotlin
    /** 남의 인증 건이면 404 VERIFICATION_NOT_FOUND (실패 횟수도 늘지 않는다, P4-1). 점검 중이면 503 (P5-1 — 스펙 §4: 요청 · 확인 모두). */
    @PostMapping("/{id}/confirm")
    fun confirm(user: RequestUser, @PathVariable id: UUID, @Valid @RequestBody body: ConfirmRequest): VerificationResponse {
        maintenance.ensureOpen()
        return VerificationResponse.from(service.confirm(user.id, id, body.code))
    }
```

`ApiExceptionHandler.kt`:
1) `@ExceptionHandler(…)` 목록에 `BankMaintenanceException::class,` 를 더한다 (`BankRejectedException::class,` 아래).
2) `when (cause)` 에서 `is BankRejectedException -> …` 줄 아래에 추가:
```kotlin
            // P5-1: 은행 점검 시간. 연결 장애(BANK_UNAVAILABLE)와 달리 언제 끝나는지 안다 → Retry-After 에 남은 초
            is BankMaintenanceException -> HttpStatus.SERVICE_UNAVAILABLE to "BANK_MAINTENANCE"
```
3) `.headers { headers -> if (cause.isRetryable()) headers.set(HttpHeaders.RETRY_AFTER, RETRY_AFTER_SECONDS) }` 를 다음으로:
```kotlin
            .headers { headers -> cause.retryAfter()?.let { headers.set(HttpHeaders.RETRY_AFTER, it) } }
```
4) `private fun Throwable.isRetryable() = …` 와 그 KDoc 을 다음으로 바꾼다:
```kotlin
    /**
     * 같은 요청을 잠시 뒤 다시 보내면 될 수 있는 오류의 `Retry-After` 초. 다시 보내도 같은 결과인 오류는 null (헤더 없음).
     * - 잔고를 지금 못 잡음 · 동시 변경으로 포기 (P4-2): 1초. Redis 장애와 락 경합을 구분하지 않는다 — 클라이언트가 할 일이 같다
     * - 은행 점검 (P5-1): 점검이 끝날 때까지 남은 초 (올림). 그 전에 다시 보내도 같은 503 이다
     */
    private fun Throwable.retryAfter(): String? = when (this) {
        is BankMaintenanceException -> retryAfterSeconds.toString()
        is TransferException.WalletBusy, is TransferException.ConcurrentUpdate -> RETRY_AFTER_SECONDS
        else -> null
    }
```

- [ ] **Step 4: 통과 확인**

Run: `./gradlew :apps:api:test --tests '*ApiExceptionHandlerTest*' --tests '*BankMaintenanceApiTest*' --tests '*TransferApiTest*' --tests '*AccountVerificationApiTest*'`
Expected: PASS (기존 오류 응답 · `Retry-After: 1` 그대로)

- [ ] **Step 5: 커밋**

```bash
git add apps/api
git commit -m "feat(api): 점검 시간에는 출금 접수 · 1원인증 요청 · 확인을 503 BANK_MAINTENANCE + Retry-After(남은 초) 로 거절

연결 장애(BANK_UNAVAILABLE)와 달리 점검은 언제 끝나는지 알므로 Retry-After 에 남은 초를 올림해 알린다.
조회 · 충전은 은행을 건드리지 않아 그대로 받는다. 웹을 거치지 않는 은행 호출은 게이트웨이의 보내기 문이 막는다.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 6: 알림 — `AlertNotifier` · 본문(가림 · 20건 · 4,096자) · `TelegramNotifier` / `LogNotifier`

**Files:**
- Create: `apps/api/src/main/kotlin/io/github/hyeokjinon/banktransfer/api/alert/Alert.kt`
- Create: `apps/api/src/main/kotlin/io/github/hyeokjinon/banktransfer/api/alert/LogNotifier.kt`
- Create: `apps/api/src/main/kotlin/io/github/hyeokjinon/banktransfer/api/alert/TelegramNotifier.kt`
- Create: `apps/api/src/main/kotlin/io/github/hyeokjinon/banktransfer/api/alert/AlertConfiguration.kt`
- Modify: `apps/api/src/main/resources/application.yml`
- Test: `apps/api/src/test/kotlin/io/github/hyeokjinon/banktransfer/api/alert/{AlertTextTest, TelegramNotifierTest, AlertConfigurationTest}.kt`

**Interfaces:**
- Produces:
  - `enum class AlertLevel { INFO, WARN, CRITICAL }`
  - `data class Alert(level: AlertLevel, title: String, lines: List<String> = emptyList(), items: List<String> = emptyList())` — `lines` = 요약, `items` = 목록(20건까지)
  - `fun interface AlertNotifier { fun notify(alert: Alert) }`, 확장 `fun AlertNotifier.notifySafely(alert: Alert)` (던져도 삼킴)
  - `object AlertText { const val MAX_ITEMS = 20; const val MAX_CHARS = 4096; fun render(alert: Alert): String; fun maskAccountNumbers(text: String): String; fun maskAccount(accountNo: String): String }`
  - `class LogNotifier : AlertNotifier` (로거 이름 `ALERT`, INFO/WARN/ERROR, 한 줄)
  - `class TelegramNotifier(baseUrl: String, botToken: String, chatId: String, connectTimeout: Duration = 3s, readTimeout: Duration = 5s, log: AlertNotifier = LogNotifier()) : AlertNotifier`
  - `@ConfigurationProperties("alert.telegram") data class TelegramProperties(botToken: String = "", chatId: String = "", baseUrl: String = "https://api.telegram.org") { val enabled: Boolean }` (toString 은 토큰을 가림), 빈 `alertNotifier: AlertNotifier`

결정(계획): 가림 규칙 · 본문 안전망, 텔레그램을 켜도 로그 먼저 · 실패 로그에는 예외 종류 · HTTP 상태만 (위 "결정(계획) 모음").

- [ ] **Step 1: 실패하는 테스트**

`alert/AlertTextTest.kt`:
```kotlin
package io.github.hyeokjinon.banktransfer.api.alert

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class AlertTextTest {

    @Test
    fun `계좌번호는 앞 4 · 뒤 4 만 남기고 가운데는 길이와 무관하게 별 4개`() {
        assertEquals("1002****6789", AlertText.maskAccount("1002123456789"))
        assertEquals("3333****5678", AlertText.maskAccount("3333012345678"))
        assertEquals("1234****3456", AlertText.maskAccount("1234567890123456"))
        assertEquals("********", AlertText.maskAccount("12345678"))
    }

    @Test
    fun `본문의 10~16자리 숫자 덩어리는 모두 가린다 - 거래번호 · 금액 · 날짜는 그대로`() {
        val text = "090-1002123456789 T261028ABCDEFGHIJKLM 30000원 2026-10-28 5500000000001"

        assertEquals("090-1002****6789 T261028ABCDEFGHIJKLM 30000원 2026-10-28 5500****0001", AlertText.maskAccountNumbers(text))
    }

    @Test
    fun `수준 · 제목 · 요약 · 목록 순서, 목록은 20건까지 + 외 n건`() {
        val text = AlertText.render(Alert(AlertLevel.CRITICAL, "제목", listOf("요약1", "요약2"), (1..23).map { "건 $it" }))

        val lines = text.split("\n")
        assertEquals("[CRITICAL] 제목", lines[0])
        assertEquals(listOf("요약1", "요약2"), lines.subList(1, 3))
        assertEquals("- 건 1", lines[3])
        assertEquals("- 건 20", lines[22])
        assertEquals("외 3건", lines[23])
        assertEquals(24, lines.size)
    }

    @Test
    fun `4096자를 넘으면 자른다`() {
        val text = AlertText.render(Alert(AlertLevel.INFO, "x".repeat(5_000)))

        assertEquals(AlertText.MAX_CHARS, text.length)
        assertTrue(text.endsWith("…"))
        assertFalse(AlertText.render(Alert(AlertLevel.INFO, "짧다")).endsWith("…"))
    }
}
```

`alert/TelegramNotifierTest.kt`:
```kotlin
package io.github.hyeokjinon.banktransfer.api.alert

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import com.sun.net.httpserver.HttpServer
import org.slf4j.LoggerFactory
import tools.jackson.databind.json.JsonMapper
import java.net.InetSocketAddress
import java.net.ServerSocket
import java.time.Duration
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.Executors
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

/** 가짜 텔레그램 서버(JDK HttpServer)로 경로 · 본문 · 실패 처리를 본다 (P5-1). 진짜 텔레그램에는 닿지 않는다. */
class TelegramNotifierTest {

    private data class Received(val path: String, val body: String)

    private val received = CopyOnWriteArrayList<Received>()

    @Volatile private var status = 200
    @Volatile private var delay = Duration.ZERO

    private val server: HttpServer = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0).apply {
        createContext("/") { exchange ->
            received += Received(exchange.requestURI.path, exchange.requestBody.readAllBytes().toString(Charsets.UTF_8))
            Thread.sleep(delay.toMillis())
            val reply = """{"ok":${status == 200}}""".toByteArray()
            exchange.sendResponseHeaders(status, reply.size.toLong())
            exchange.responseBody.use { it.write(reply) }
        }
        executor = Executors.newCachedThreadPool()
        start()
    }

    @AfterTest
    fun stop() = server.stop(0)

    private fun notifier(readTimeout: Duration = Duration.ofSeconds(5)) =
        TelegramNotifier("http://127.0.0.1:${server.address.port}", TOKEN, "42", readTimeout = readTimeout)

    private fun captureLogs(name: String, block: () -> Unit): List<ILoggingEvent> {
        val logger = LoggerFactory.getLogger(name) as Logger
        val appender = ListAppender<ILoggingEvent>().also { it.start() }
        logger.addAppender(appender)
        try {
            block()
        } finally {
            logger.detachAppender(appender)
        }
        return appender.list
    }

    @Test
    fun `POST bot{token}_sendMessage 에 chat_id 와 일반 텍스트 - parse_mode 는 보내지 않는다`() {
        notifier().notify(Alert(AlertLevel.INFO, "일마감 대사 완료", listOf("출금 3건")))

        val request = received.single()
        assertEquals("/bot$TOKEN/sendMessage", request.path)
        val json = JsonMapper.shared().readTree(request.body)
        assertEquals("42", json.get("chat_id").asString())
        assertEquals("[INFO] 일마감 대사 완료\n출금 3건", json.get("text").asString())
        assertNull(json.get("parse_mode"))
    }

    @Test
    fun `보내는 본문도 계좌번호를 가리고 목록은 20건 + 외 n건`() {
        notifier().notify(Alert(AlertLevel.CRITICAL, "불일치", items = (1..25).map { "090-1002123456789 #$it" }))

        val text = JsonMapper.shared().readTree(received.single().body).get("text").asString()
        assertFalse("1002123456789" in text)
        assertTrue("090-1002****6789 #20" in text)
        assertFalse("#21" in text)
        assertTrue(text.endsWith("외 5건"))
    }

    @Test
    fun `보내는 본문은 4096자 이하로 자른다`() {
        notifier().notify(Alert(AlertLevel.INFO, "x".repeat(5_000)))

        val text = JsonMapper.shared().readTree(received.single().body).get("text").asString()
        assertEquals(AlertText.MAX_CHARS, text.length)
        assertTrue(text.endsWith("…"))
    }

    @Test
    fun `500 이어도 예외 없이 ERROR 로그만 - 로그에 토큰이 없다`() {
        status = 500

        val logs = captureLogs(TelegramNotifier::class.java.name) { notifier().notify(Alert(AlertLevel.WARN, "경고")) }

        val error = logs.single { it.level == Level.ERROR }
        assertTrue("HTTP 500" in error.formattedMessage)
        assertFalse(TOKEN in error.formattedMessage)
    }

    @Test
    fun `응답이 늦으면(시간 초과) 기다리지 않고 예외 없이 끝난다 - 로그에 토큰이 없다`() {
        delay = Duration.ofSeconds(2)
        val started = System.nanoTime()

        val logs = captureLogs(TelegramNotifier::class.java.name) {
            notifier(readTimeout = Duration.ofMillis(200)).notify(Alert(AlertLevel.WARN, "느림"))
        }

        assertTrue(Duration.ofNanos(System.nanoTime() - started) < Duration.ofMillis(1_500))
        val error = logs.single { it.level == Level.ERROR }
        assertFalse(TOKEN in error.formattedMessage)
        assertFalse(error.throwableProxy != null && TOKEN in error.throwableProxy.message.orEmpty())
    }

    @Test
    fun `연결이 안 돼도(아무도 듣지 않는 포트) 예외 없이 끝난다`() {
        val closedPort = ServerSocket(0).use { it.localPort }

        TelegramNotifier("http://127.0.0.1:$closedPort", TOKEN, "42").notify(Alert(AlertLevel.CRITICAL, "닿지 않음"))
    }

    @Test
    fun `텔레그램을 켜도 로그에 먼저 남긴다 - 한 줄로`() {
        val logs = captureLogs("ALERT") { notifier().notify(Alert(AlertLevel.CRITICAL, "대사 미완료", listOf("이유: FILE_NOT_READY"))) }

        val log = logs.single()
        assertEquals(Level.ERROR, log.level)
        assertEquals("[CRITICAL] 대사 미완료 | 이유: FILE_NOT_READY", log.formattedMessage)
    }

    private companion object {
        const val TOKEN = "123456:TEST-ONLY-NOT-A-REAL-TOKEN"
    }
}
```

`alert/AlertConfigurationTest.kt`:
```kotlin
package io.github.hyeokjinon.banktransfer.api.alert

import org.springframework.boot.test.context.runner.ApplicationContextRunner
import kotlin.test.Test
import kotlin.test.assertFalse
import kotlin.test.assertIs

/** 텔레그램은 토큰 · chat id 가 둘 다 있을 때만 (P5-1). 없으면 로그 — 봇이 없는 PC · CI 에서도 그대로 돈다. */
class AlertConfigurationTest {

    private val runner = ApplicationContextRunner().withUserConfiguration(AlertConfiguration::class.java)

    @Test
    fun `설정이 없으면 LogNotifier`() {
        runner.run { ctx -> assertIs<LogNotifier>(ctx.getBean(AlertNotifier::class.java)) }
    }

    @Test
    fun `토큰만 있거나 chat id 만 있으면 LogNotifier`() {
        runner.withPropertyValues("alert.telegram.bot-token=123:TEST").run { ctx -> assertIs<LogNotifier>(ctx.getBean(AlertNotifier::class.java)) }
        runner.withPropertyValues("alert.telegram.chat-id=42").run { ctx -> assertIs<LogNotifier>(ctx.getBean(AlertNotifier::class.java)) }
    }

    @Test
    fun `둘 다 있으면 TelegramNotifier - 빈 · 설정의 toString 에 토큰이 없다`() {
        runner.withPropertyValues("alert.telegram.bot-token=123:TEST-SECRET", "alert.telegram.chat-id=42").run { ctx ->
            val notifier = ctx.getBean(AlertNotifier::class.java)
            assertIs<TelegramNotifier>(notifier)
            assertFalse("TEST-SECRET" in notifier.toString())
            assertFalse("TEST-SECRET" in ctx.getBean(TelegramProperties::class.java).toString())
        }
    }
}
```

- [ ] **Step 2: 실패 확인**

Run: `./gradlew :apps:api:test --tests '*AlertTextTest*' --tests '*TelegramNotifierTest*' --tests '*AlertConfigurationTest*'`
Expected: FAIL (컴파일: `Unresolved reference 'Alert'`, `'AlertText'`, `'TelegramNotifier'`, `'AlertConfiguration'`)

- [ ] **Step 3: 구현**

`alert/Alert.kt`:
```kotlin
package io.github.hyeokjinon.banktransfer.api.alert

import org.slf4j.LoggerFactory

enum class AlertLevel { INFO, WARN, CRITICAL }

/**
 * 운영자에게 보내는 알림 한 건 (P5-1). [lines] 는 요약, [items] 는 목록(불일치 건 등) — 목록은 [AlertText.MAX_ITEMS] 건까지만 싣고 "외 n건" 으로 줄인다.
 * 사용자 id · 예금주명은 넣지 않는다. 계좌번호는 보내기 전에 가운데를 가린다 ([AlertText.render]).
 */
data class Alert(
    val level: AlertLevel,
    val title: String,
    val lines: List<String> = emptyList(),
    val items: List<String> = emptyList(),
)

/**
 * 알림 포트 (P5-1). 구현은 예외를 던지지 않는다 — 알림 실패가 대사 · 메시지 처리를 실패시키거나 되돌리지 않는다.
 * 기록의 원본은 DB(recon_issue)와 로그다.
 */
fun interface AlertNotifier {
    fun notify(alert: Alert)
}

/** 구현이 약속을 어겨 던져도 부른 쪽(대사 잡 · 컨슈머 · 정리 작업)으로 번지지 않게 한 번 더 막는다. */
fun AlertNotifier.notifySafely(alert: Alert) {
    try {
        notify(alert)
    } catch (e: Exception) {
        LoggerFactory.getLogger(AlertNotifier::class.java).error("alert '{}' could not be delivered: {}", alert.title, e.javaClass.name)
    }
}

/** 알림 본문 만들기 (텔레그램 · 로그가 같은 글을 쓴다). */
object AlertText {
    const val MAX_ITEMS = 20

    /** 텔레그램 sendMessage 의 text 한도. */
    const val MAX_CHARS = 4096

    /** 계좌번호 모양(10~16자리 숫자 덩어리). 거래번호(T… 20자)는 영문으로 시작하는 한 낱말이라 낱말 경계가 없어 걸리지 않는다. */
    private val ACCOUNT_NO = Regex("""\b\d{10,16}\b""")

    fun render(alert: Alert): String {
        val shown = alert.items.take(MAX_ITEMS)
        val text = buildString {
            append('[').append(alert.level).append("] ").append(alert.title)
            alert.lines.forEach { append('\n').append(it) }
            shown.forEach { append("\n- ").append(it) }
            if (alert.items.size > shown.size) append("\n외 ").append(alert.items.size - shown.size).append("건")
        }
        // 만든 쪽이 가리는 것을 잊어도 나가기 전에 한 번 더 가린다 (텔레그램은 외부 서비스다)
        val masked = maskAccountNumbers(text)
        return if (masked.length <= MAX_CHARS) masked else masked.take(MAX_CHARS - 1) + "…"
    }

    fun maskAccountNumbers(text: String): String = ACCOUNT_NO.replace(text) { maskAccount(it.value) }

    /** 1002123456789 → 1002****6789. 가운데는 길이와 무관하게 `****` — 자릿수도 숨긴다. 8자리 이하는 모두 가린다. */
    fun maskAccount(accountNo: String): String =
        if (accountNo.length <= 8) "*".repeat(accountNo.length) else accountNo.take(4) + "****" + accountNo.takeLast(4)
}
```

`alert/LogNotifier.kt`:
```kotlin
package io.github.hyeokjinon.banktransfer.api.alert

import org.slf4j.LoggerFactory

/**
 * 텔레그램이 없을 때(개발 PC · CI)의 알림 = 로그 한 줄 (P5-1). 텔레그램을 켜도 먼저 여기에 남긴다 — 알림 채널이 죽어도 기록은 남게.
 * 로거 이름 `ALERT` 로 모아 grep 하기 쉽게, 줄바꿈은 ` | ` 로 바꿔 한 줄로 (로그 한 건이 여러 줄로 깨지지 않게).
 */
class LogNotifier : AlertNotifier {
    private val log = LoggerFactory.getLogger("ALERT")

    override fun notify(alert: Alert) {
        val text = AlertText.render(alert).replace("\n", " | ")
        when (alert.level) {
            AlertLevel.INFO -> log.info(text)
            AlertLevel.WARN -> log.warn(text)
            AlertLevel.CRITICAL -> log.error(text)
        }
    }
}
```

`alert/TelegramNotifier.kt`:
```kotlin
package io.github.hyeokjinon.banktransfer.api.alert

import org.slf4j.LoggerFactory
import org.springframework.http.MediaType
import org.springframework.http.client.JdkClientHttpRequestFactory
import org.springframework.web.client.RestClient
import org.springframework.web.client.RestClientResponseException
import tools.jackson.databind.json.JsonMapper
import java.net.URI
import java.net.http.HttpClient
import java.time.Duration

/**
 * 텔레그램 Bot API 로 알림을 보낸다 (P5-1). `POST {baseUrl}/bot{token}/sendMessage`, 일반 텍스트(parse_mode 없음 — 이스케이프 실수로 메시지가 거절되지 않게).
 * 연결 3초 · 응답 5초. 실패(4xx · 5xx · 시간 초과 · 연결 거부)는 ERROR 로그만 남기고 삼킨다.
 * 토큰은 URL 경로에 있으므로 예외 메시지 · URL 을 로그에 남기지 않는다 — 예외 종류와 HTTP 상태만. 보내기 전에 [log] 에 먼저 남긴다.
 */
class TelegramNotifier(
    baseUrl: String,
    botToken: String,
    private val chatId: String,
    connectTimeout: Duration = Duration.ofSeconds(3),
    readTimeout: Duration = Duration.ofSeconds(5),
    private val log: AlertNotifier = LogNotifier(),
) : AlertNotifier {
    private val logger = LoggerFactory.getLogger(javaClass)
    private val uri: URI = URI.create("${baseUrl.trimEnd('/')}/bot$botToken/sendMessage")
    private val http: RestClient = RestClient.builder()
        .requestFactory(
            JdkClientHttpRequestFactory(HttpClient.newBuilder().connectTimeout(connectTimeout).build()).apply { setReadTimeout(readTimeout) },
        )
        .build()

    override fun notify(alert: Alert) {
        log.notify(alert)
        try {
            val body = JsonMapper.shared().writeValueAsString(mapOf("chat_id" to chatId, "text" to AlertText.render(alert)))
            http.post().uri(uri).contentType(MediaType.APPLICATION_JSON).body(body).retrieve().toBodilessEntity()
        } catch (e: RestClientResponseException) {
            logger.error("telegram alert '{}' was rejected: HTTP {}", alert.title, e.statusCode.value())
        } catch (e: Exception) {
            logger.error("telegram alert '{}' failed: {}", alert.title, e.javaClass.name)
        }
    }

    /** 토큰을 찍지 않는다. */
    override fun toString(): String = "TelegramNotifier(chatId=$chatId)"
}
```

`alert/AlertConfiguration.kt`:
```kotlin
package io.github.hyeokjinon.banktransfer.api.alert

import org.slf4j.LoggerFactory
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

/**
 * 텔레그램 알림 설정 (P5-1). 값은 환경변수(ALERT_TELEGRAM_BOT_TOKEN · ALERT_TELEGRAM_CHAT_ID)로만 준다 — 저장소 · 로그에 남기지 않는다 (SETUP 2-6).
 * toString 은 토큰을 가린다 (data class 기본 toString 은 모든 필드를 찍는다).
 */
@ConfigurationProperties("alert.telegram")
data class TelegramProperties(
    val botToken: String = "",
    val chatId: String = "",
    val baseUrl: String = "https://api.telegram.org",
) {
    val enabled: Boolean get() = botToken.isNotBlank() && chatId.isNotBlank()

    override fun toString(): String = "TelegramProperties(botToken=${if (botToken.isBlank()) "" else "****"}, chatId=$chatId, baseUrl=$baseUrl)"
}

/**
 * 알림 구현 하나를 고른다 (P5-1): 토큰 · chat id 가 둘 다 있으면 텔레그램(로그도 남긴다), 아니면 로그만.
 * (TelegramProperties 는 @ConfigurationPropertiesScan 도 등록하지만, 이 클래스만 떼어 쓰는 테스트를 위해 여기서도 켠다 — 같은 이름이라 한 번만 등록된다.)
 */
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(TelegramProperties::class)
class AlertConfiguration {
    private val log = LoggerFactory.getLogger(javaClass)

    @Bean
    fun alertNotifier(props: TelegramProperties): AlertNotifier =
        if (props.enabled) {
            log.info("alerts go to telegram chat {} (and the log)", props.chatId)
            TelegramNotifier(props.baseUrl, props.botToken, props.chatId)
        } else {
            log.info("alerts go to the log only (alert.telegram.bot-token / chat-id not set)")
            LogNotifier()
        }
}
```

`application.yml` 끝(`info:` 절 위)에 추가:
```yaml
alert:
  telegram:
    # 둘 다 있을 때만 텔레그램으로 보낸다 (없으면 로그만). 값은 환경변수로만 — 저장소 · 로그에 남기지 않는다 (SETUP 2-6)
    bot-token: ${ALERT_TELEGRAM_BOT_TOKEN:}
    chat-id: ${ALERT_TELEGRAM_CHAT_ID:}
    base-url: https://api.telegram.org
```

- [ ] **Step 4: 통과 확인**

Run: `./gradlew :apps:api:test --tests '*AlertTextTest*' --tests '*TelegramNotifierTest*' --tests '*AlertConfigurationTest*'`
Expected: PASS (14개). 시간 초과 테스트는 200ms 응답 한도로 1.5초 안에 끝난다.

- [ ] **Step 5: 커밋**

```bash
git add apps/api
git commit -m "feat(api): 알림 포트 AlertNotifier — 텔레그램(토큰 · chat id 둘 다 있을 때) / 로그, 가림 · 20건 · 4096자

알림 실패는 삼킨다 — 대사 · 메시지 처리를 실패시키거나 되돌리지 않는다. 텔레그램은 외부 서비스라 계좌번호를
가운데 가리고(본문 전체에 한 번 더), 사용자 id 는 넣지 않으며, 실패 로그에는 토큰이 든 URL 대신 예외 종류 · HTTP 상태만 남긴다.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 7: 기존 경고 3곳을 알림으로 — 컨슈머 CONFLICT · DLT(은행 장애가 아닌 이유) · 결과조회 한도 소진

**Files:**
- Modify: `apps/api/src/main/kotlin/io/github/hyeokjinon/banktransfer/api/messaging/TransferRequestedListener.kt`, `MessagingConfiguration.kt`
- Modify: `apps/api/src/main/kotlin/io/github/hyeokjinon/banktransfer/api/reconcile/ReconcileConfiguration.kt`
- Create (test): `apps/api/src/test/kotlin/io/github/hyeokjinon/banktransfer/api/support/RecordingAlertNotifier.kt`
- Modify (test): `apps/api/src/test/kotlin/io/github/hyeokjinon/banktransfer/api/support/ApiIntegrationTest.kt`
- Test: `apps/api/src/test/kotlin/io/github/hyeokjinon/banktransfer/api/messaging/TransferRequestedListenerTest.kt` (테스트 4개 추가), `apps/api/src/test/kotlin/io/github/hyeokjinon/banktransfer/api/reconcile/InquiryGiveUpAlertTest.kt` (신규)

**Interfaces:**
- Consumes: Task 6 `Alert` · `AlertLevel` · `AlertNotifier` · `notifySafely` · `AlertText`
- Produces:
  - `TransferRequestedListener(alerts: AlertNotifier = AlertNotifier { }, process: (UUID) -> ProcessResult)` — 알림 인자를 **앞**에 둔다(기존 `TransferRequestedListener { … }` 후행 람다 호출이 그대로 컴파일되게)
  - `internal fun inquiryGiveUpAlert(transfer: Transfer): Alert` (`reconcile` 패키지)
  - 테스트: `class RecordingAlertNotifier : AlertNotifier { val received: MutableList<Alert>; var onNotify: ((Alert) -> Unit)?; fun about(date: LocalDate): List<Alert> }`, `@TestConfiguration class AlertTestConfiguration`(`@Primary`), `ApiIntegrationTest.recordedAlerts: RecordingAlertNotifier`

- [ ] **Step 1: 실패하는 테스트**

`TransferRequestedListenerTest.kt`:
1) import 추가:
```kotlin
import io.github.hyeokjinon.banktransfer.api.alert.Alert
import io.github.hyeokjinon.banktransfer.api.alert.AlertLevel
import io.github.hyeokjinon.banktransfer.api.alert.AlertNotifier
```
2) `private class CountingAck …` 아래에 추가:
```kotlin

    /** 받은 알림을 모은다 (P5-1). */
    private class RecordingNotifier : AlertNotifier {
        val alerts = mutableListOf<Alert>()
        override fun notify(alert: Alert) {
            alerts += alert
        }
    }
```
3) 클래스 끝(마지막 `}` 앞, `realDltMessage` 함수 위)에 추가:
```kotlin

    // --- 알림 (P5-1): 사람이 봐야 하는 경고 3곳 중 두 곳이 여기다 ---

    @Test
    fun `CONFLICT 면 WARN 알림 하나 - 이체 id 만 싣는다`() {
        val alerts = RecordingNotifier()
        val id = UUID.randomUUID()

        TransferRequestedListener(alerts) { ProcessResult.CONFLICT }.on(record(id), CountingAck())

        val alert = alerts.alerts.single()
        assertEquals(AlertLevel.WARN, alert.level)
        assertTrue(alert.lines.any { id.toString() in it })
    }

    @Test
    fun `알림이 예외를 던져도 ack 한다 - 알림 실패가 메시지 처리를 바꾸지 않는다`() {
        val ack = CountingAck()

        TransferRequestedListener({ error("telegram down") }) { ProcessResult.CONFLICT }.on(record(UUID.randomUUID()), ack)

        assertEquals(1, ack.count)
    }

    @Test
    fun `DLT - 은행 장애가 아닌 이유(DB 장애 등)만 WARN 알림, 은행 연결 없음 · 잘못된 key 는 알리지 않는다`() {
        val alerts = RecordingNotifier()
        val listener = TransferRequestedListener(alerts) { ProcessResult.COMPLETED }
        val id = UUID.randomUUID().toString()

        listener.onDlt(dltRecord(id, RetryLaterException::class.java.name), CountingAck())
        listener.onDlt(dltRecord("not-a-uuid", InvalidTransferKeyException::class.java.name), CountingAck())
        listener.onDlt(dltRecord(id, "org.springframework.dao.DataAccessResourceFailureException", realDltMessage("db down")), CountingAck())

        val alert = alerts.alerts.single()
        assertEquals(AlertLevel.WARN, alert.level)
        assertTrue(alert.lines.any { id in it })
        assertTrue(alert.lines.any { "DataAccessResourceFailureException" in it })
        assertFalse(alert.lines.any { "1002123456789" in it })   // 레코드 값(계좌번호)은 싣지 않는다
    }

    @Test
    fun `기본 생성(알림 없음)도 그대로 처리한다 - 테스트 · 조립 전 기본값`() {
        val ack = CountingAck()

        TransferRequestedListener { ProcessResult.CONFLICT }.on(record(UUID.randomUUID()), ack)

        assertEquals(1, ack.count)
    }
```

`reconcile/InquiryGiveUpAlertTest.kt`:
```kotlin
package io.github.hyeokjinon.banktransfer.api.reconcile

import io.github.hyeokjinon.banktransfer.api.alert.AlertLevel
import io.github.hyeokjinon.banktransfer.api.alert.AlertText
import io.github.hyeokjinon.banktransfer.transfer.Transfer
import io.github.hyeokjinon.banktransfer.transfer.TransferStatus
import java.time.Instant
import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class InquiryGiveUpAlertTest {

    @Test
    fun `결과조회 한도 소진 알림 - WARN, 거래번호 · 금액 · 가린 계좌, 사용자 id 는 없다`() {
        val transfer = Transfer(
            id = UUID.randomUUID(), userId = "user-secret-id", idempotencyKey = "k", reference = "T261028GIVEUP0000001",
            requestHash = "h", bankCode = "090", accountNo = "1002123456789", amount = 30_000, memo = "m",
            status = TransferStatus.UNKNOWN, inquiryAttempts = 10, createdAt = Instant.EPOCH, updatedAt = Instant.EPOCH,
        )

        val alert = inquiryGiveUpAlert(transfer)
        val text = AlertText.render(alert)

        assertEquals(AlertLevel.WARN, alert.level)
        assertTrue("10회" in alert.title)
        assertTrue("T261028GIVEUP0000001" in text)
        assertTrue("090-1002****6789" in text)
        assertFalse("1002123456789" in text)
        assertFalse("user-secret-id" in text)
    }
}
```

- [ ] **Step 2: 실패 확인**

Run: `./gradlew :apps:api:test --tests '*TransferRequestedListenerTest*' --tests '*InquiryGiveUpAlertTest*'`
Expected: FAIL (컴파일: `TransferRequestedListener` 에 인자 두 개를 받는 생성자가 없다, `Unresolved reference 'inquiryGiveUpAlert'`)

- [ ] **Step 3: 구현**

`TransferRequestedListener.kt`:
1) import 추가:
```kotlin
import io.github.hyeokjinon.banktransfer.api.alert.Alert
import io.github.hyeokjinon.banktransfer.api.alert.AlertLevel
import io.github.hyeokjinon.banktransfer.api.alert.AlertNotifier
import io.github.hyeokjinon.banktransfer.api.alert.notifySafely
```
2) 클래스 KDoc 끝에 ` * 사람이 봐야 하는 두 경우(CONFLICT, 은행 장애가 아닌 이유의 DLT)는 로그와 함께 WARN 알림을 보낸다 (P5-1). 알림 실패는 처리 결과를 바꾸지 않는다.` 를 더하고, 선언을 바꾼다:
```kotlin
class TransferRequestedListener(
    /** 사람이 봐야 하는 경고를 알린다 (P5-1). 기본은 아무것도 하지 않는다 — 테스트 · 조립 전 기본값. 앞에 두어 `TransferRequestedListener { … }` 가 그대로 컴파일된다. */
    private val alerts: AlertNotifier = AlertNotifier { },
    private val process: (UUID) -> ProcessResult,
) {
```
3) CONFLICT 블록의 `log.warn(…)` 줄 아래에 추가:
```kotlin
            alerts.notifySafely(
                Alert(
                    AlertLevel.WARN, "출금 확정과 은행 결과가 어긋남 (CONFLICT)",
                    listOf("transferId=$transferId", "잔고는 그대로 — 수동 확인, 일마감 대사가 다음 날 다시 맞춘다"),
                ),
            )
```
4) `onDlt` 의 `DltKind.OTHER -> log.error(…)` 를 블록으로 바꾼다:
```kotlin
            DltKind.OTHER -> {
                log.error(
                    "transfer {} moved to DLT and is not retried automatically - check transfer status. " +
                        "republish covers only bank-not-connected: a RECEIVED transfer with sending_at NULL is skipped. exception={} cause={} original={}",
                    record.key(), causeFqcn, logSafe(rootMessage(record.headerText(KafkaHeaders.EXCEPTION_MESSAGE)), MESSAGE_LOG_MAX), original,
                )
                // 자동으로 다시 하지 않는 유일한 DLT 경우라 사람에게 알린다 (P5-1). 메시지 값 · 계좌번호는 싣지 않는다
                alerts.notifySafely(
                    Alert(
                        AlertLevel.WARN, "DLT: 자동으로 다시 처리하지 않는 출금",
                        listOf("transferId=${logSafe(record.key(), KEY_LOG_MAX)}", "exception=$causeFqcn", "출금 상태를 확인하고 손으로 다시 처리한다 (OPERATION §6)"),
                    ),
                )
            }
```
5) KDoc 의 ` * 한 줄로만 남긴다: … 알림 채널은 P5.` 끝을 `… 계좌번호는 남기지 않는다. 은행 장애가 아닌 이유는 WARN 알림도 보낸다 (P5-1).` 로 바꾼다.

`MessagingConfiguration.kt` 의 리스너 빈을 바꾼다 (import `io.github.hyeokjinon.banktransfer.api.alert.AlertNotifier`):
```kotlin
    @Bean
    fun transferRequestedListener(processor: TransferProcessor, alerts: AlertNotifier) = TransferRequestedListener(alerts, processor::process)
```

`ReconcileConfiguration.kt`:
1) import 추가: `io.github.hyeokjinon.banktransfer.api.alert.Alert`, `io.github.hyeokjinon.banktransfer.api.alert.AlertLevel`, `io.github.hyeokjinon.banktransfer.api.alert.AlertNotifier`, `io.github.hyeokjinon.banktransfer.api.alert.notifySafely`, `io.github.hyeokjinon.banktransfer.transfer.Transfer`.
2) `@Configuration(proxyBeanMethods = false) class ReconcileConfiguration` 위에 추가:
```kotlin
/**
 * 결과조회 한도 소진 알림 (P5-1). 일마감 대사가 은행 파일로 확정할 때까지 이 출금의 잔고는 묶여 있다.
 * 사용자 id 는 넣지 않는다 (Transfer.toString 에는 있다 — 알림에는 쓰지 않는다). 계좌는 AlertText 가 가운데를 가린다.
 */
internal fun inquiryGiveUpAlert(transfer: Transfer): Alert = Alert(
    AlertLevel.WARN,
    "결과조회 ${transfer.inquiryAttempts}회에도 결과를 모르는 출금 — 일마감 대사로 넘김",
    listOf(
        "transferId=${transfer.id}",
        "reference=${transfer.reference}",
        "amount=${transfer.amount}",
        "account=${transfer.bankCode}-${transfer.accountNo}",
    ),
)
```
3) `transferReconciler` 빈 전체를 다음으로 바꾼다 (`alerts` 인자 추가, `onGiveUp` 에 알림 — 나머지는 Task 4 뒤의 모양 그대로):
```kotlin
    @Bean
    fun transferReconciler(store: TransferStore, client: BankGatewayClient, props: ReconcileProperties, clock: Clock, alerts: AlertNotifier) =
        TransferReconciler(
            store = store,
            bank = BankInquiryAdapter(client),
            clock = clock,
            sendingTimeout = props.sendingTimeout,
            maxInquiries = props.maxInquiries,
            batchSize = props.batchSize,
            republishAfter = props.republishAfter,
            maxRepublish = props.maxRepublish,
            // Transfer.toString 은 계좌번호를 가린다. 알림은 사용자 id 없이 따로 만든다 (P5-1)
            onGiveUp = {
                log.warn("still UNKNOWN after {} inquiries, leave it to daily reconciliation: {}", it.inquiryAttempts, it)
                alerts.notifySafely(inquiryGiveUpAlert(it))
            },
            onAbandon = {
                log.warn("transfer abandoned after {} republishes, the bank was never reached (FAILED E001): {}", it.republishCount, it)
            },
            // 한 건이 깨져도 나머지는 계속 돈다. 건너뛴 건은 매 주기 다시 여기로 온다 — 손으로 고칠 때까지 ERROR 가 반복된다
            onItemError = { transfer, e -> log.error("reconcile skipped transfer {}: {}", transfer, e.toString(), e) },
        )
```

`support/RecordingAlertNotifier.kt` (통합 테스트 공용):
```kotlin
package io.github.hyeokjinon.banktransfer.api.support

import io.github.hyeokjinon.banktransfer.api.alert.Alert
import io.github.hyeokjinon.banktransfer.api.alert.AlertNotifier
import io.github.hyeokjinon.banktransfer.api.alert.LogNotifier
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Primary
import java.time.LocalDate
import java.util.concurrent.CopyOnWriteArrayList

/**
 * 통합 테스트의 알림 (P5-1): 받은 알림을 모으고 로그에도 남긴다. 공유 컨텍스트라 다른 테스트의 알림도 섞이므로 [about] 처럼 걸러 본다.
 * [onNotify] 를 주면 알림 안에서 부른다 — 대사 잡을 ⑥ 보고에서 붙잡아 두는 동시 실행 테스트용 (테스트가 끝나면 null 로).
 */
class RecordingAlertNotifier : AlertNotifier {
    private val log = LogNotifier()
    val received: MutableList<Alert> = CopyOnWriteArrayList()

    @Volatile
    var onNotify: ((Alert) -> Unit)? = null

    override fun notify(alert: Alert) {
        log.notify(alert)
        received += alert
        onNotify?.invoke(alert)
    }

    /** 제목에 이 날짜가 든 알림 (대사 알림은 제목에 대사일을 쓴다). */
    fun about(date: LocalDate): List<Alert> = received.filter { date.toString() in it.title }
}

/** 앱의 AlertNotifier(로그)보다 먼저 주입된다. */
@TestConfiguration(proxyBeanMethods = false)
class AlertTestConfiguration {

    @Bean
    @Primary
    fun recordingAlertNotifier() = RecordingAlertNotifier()
}
```

`ApiIntegrationTest.kt`:
1) `@Import(…, TestClockConfiguration::class)` 에 `AlertTestConfiguration::class` 를 더한다.
2) `testClock` 아래에 추가:
```kotlin

    /** 앱이 보낸 알림 (P5-1). 공유 컨텍스트라 다른 테스트의 알림도 섞인다 — 날짜 · 제목으로 걸러 본다. */
    @Autowired
    protected lateinit var recordedAlerts: RecordingAlertNotifier
```

- [ ] **Step 4: 통과 확인**

Run: `./gradlew :apps:api:test --tests '*TransferRequestedListenerTest*' --tests '*InquiryGiveUpAlertTest*' --tests '*ReconcileFlowTest*' --tests '*TransferProcessingFlowTest*'`
Expected: PASS (새 테스트 5개 + 기존 리스너 · 흐름 테스트 그대로 — 컨텍스트는 `RecordingAlertNotifier` 로 뜬다)

- [ ] **Step 5: 커밋**

```bash
git add apps/api
git commit -m "feat(api): 미뤄 둔 경고 3곳을 WARN 알림으로 — 컨슈머 CONFLICT · DLT(은행 장애 아님) · 결과조회 한도 소진

P3 ~ P4 에서 '알림은 P5' 로 남긴 경고다. 로그는 그대로 두고 알림을 더한다. 알림 실패는 메시지 처리 · 정리 작업을
바꾸지 않는다(notifySafely). 사용자 id · 계좌번호 원문은 싣지 않는다.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 8: 도메인 — 판정 `StatementMatcher`(설계 §6 표) · 미확정만 확정하는 `StatementSettler`(`E002`) (+ 변이 검사)

**Files:**
- Create: `modules/transfer/src/main/kotlin/io/github/hyeokjinon/banktransfer/transfer/StatementMatching.kt`
- Test: `modules/transfer/src/test/kotlin/io/github/hyeokjinon/banktransfer/transfer/StatementMatcherTest.kt`, `StatementSettlerTest.kt`

**Interfaces:**
- Consumes: 기존 `Transfer` · `TransferStatus` · `TransferStore.findById / transition / fail` · `maskAccountNo` (internal, 같은 모듈)
- Produces:
  - `data class BankStatementLine(bankDate: LocalDate, lineNo: Int, reference: String, bankCode: String, accountNo: String, amount: Long, processedAt: Instant)`
  - `enum class ReconIssueType(val mismatch: Boolean, val urgent: Boolean = false) { AUTO_COMPLETED, AUTO_FAILED, AMOUNT_OR_ACCOUNT_MISMATCH, COMPLETED_NOT_AT_BANK, REFUNDED_BUT_PAID, PAID_BUT_RECEIVED(urgent), UNKNOWN_REFERENCE, BALANCE_LEDGER_MISMATCH, PAID_TWICE(urgent) }`
  - `enum class ReconVerdict(val issue: ReconIssueType?) { MATCHED, AUTO_COMPLETE, AUTO_FAIL, AMOUNT_OR_ACCOUNT_MISMATCH, COMPLETED_NOT_AT_BANK, REFUNDED_BUT_PAID, PAID_BUT_RECEIVED, RETRYING, UNKNOWN_REFERENCE, PAID_TWICE }`
  - `object StatementMatcher { fun judge(transfer: Transfer?, lines: List<BankStatementLine>): ReconVerdict }`
  - `data class Settlement(verdict: ReconVerdict, transfer: Transfer?, lines: List<BankStatementLine>)` — `transfer` 는 판정한 시점(확정 전)의 상태
  - `class StatementSettler(store: TransferStore, clock: Clock = Clock.systemUTC(), maxAttempts: Int = 3) { fun settle(transferId: UUID, linesOf: (String) -> List<BankStatementLine>): Settlement; fun unknownReference(lines: List<BankStatementLine>): Settlement; companion object { const val NOT_AT_BANK = "E002" } }`

결정(계획): 판정 순서(줄 2개 이상 먼저, 우리 출금 없음 → `UNKNOWN_REFERENCE`), 자동 완료 코드 `0000`, 다시 읽기 최대 3회 (위 "결정(계획) 모음").

- [ ] **Step 1: 실패하는 테스트**

`StatementMatcherTest.kt`:
```kotlin
package io.github.hyeokjinon.banktransfer.transfer

import io.github.hyeokjinon.banktransfer.transfer.ReconVerdict.AMOUNT_OR_ACCOUNT_MISMATCH
import io.github.hyeokjinon.banktransfer.transfer.ReconVerdict.AUTO_COMPLETE
import io.github.hyeokjinon.banktransfer.transfer.ReconVerdict.AUTO_FAIL
import io.github.hyeokjinon.banktransfer.transfer.ReconVerdict.COMPLETED_NOT_AT_BANK
import io.github.hyeokjinon.banktransfer.transfer.ReconVerdict.MATCHED
import io.github.hyeokjinon.banktransfer.transfer.ReconVerdict.PAID_BUT_RECEIVED
import io.github.hyeokjinon.banktransfer.transfer.ReconVerdict.PAID_TWICE
import io.github.hyeokjinon.banktransfer.transfer.ReconVerdict.REFUNDED_BUT_PAID
import io.github.hyeokjinon.banktransfer.transfer.ReconVerdict.RETRYING
import io.github.hyeokjinon.banktransfer.transfer.ReconVerdict.UNKNOWN_REFERENCE
import io.github.hyeokjinon.banktransfer.transfer.TransferStatus.COMPLETED
import io.github.hyeokjinon.banktransfer.transfer.TransferStatus.FAILED
import io.github.hyeokjinon.banktransfer.transfer.TransferStatus.RECEIVED
import io.github.hyeokjinon.banktransfer.transfer.TransferStatus.SENDING
import io.github.hyeokjinon.banktransfer.transfer.TransferStatus.UNKNOWN
import java.time.Instant
import java.time.LocalDate
import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

/** 설계 §6 의 표를 줄마다 그대로 (P5-1). 번호는 표의 # 이다. */
class StatementMatcherTest {

    private val day = LocalDate.of(2026, 10, 28)

    private fun transfer(status: TransferStatus) = Transfer(
        id = UUID.randomUUID(), userId = "u", idempotencyKey = "k", reference = REF, requestHash = "h",
        bankCode = "090", accountNo = "1002123456789", amount = 30_000, memo = "m",
        status = status, createdAt = Instant.EPOCH, updatedAt = Instant.EPOCH,
    )

    private fun line(amount: Long = 30_000, accountNo: String = "1002123456789", bankCode: String = "090", lineNo: Int = 2, reference: String = REF) =
        BankStatementLine(day, lineNo, reference, bankCode, accountNo, amount, Instant.EPOCH)

    private fun judge(transfer: Transfer?, vararg lines: BankStatementLine) = StatementMatcher.judge(transfer, lines.toList())

    @Test
    fun `1 COMPLETED + 같은 줄 = 일치`() = assertEquals(MATCHED, judge(transfer(COMPLETED), line()))

    @Test
    fun `2 FAILED + 없음 = 일치`() = assertEquals(MATCHED, judge(transfer(FAILED)))

    @Test
    fun `3 UNKNOWN · SENDING + 같은 줄 = 자동 완료`() {
        assertEquals(AUTO_COMPLETE, judge(transfer(UNKNOWN), line()))
        assertEquals(AUTO_COMPLETE, judge(transfer(SENDING), line()))
    }

    @Test
    fun `4 UNKNOWN · SENDING + 없음 = 자동 실패`() {
        assertEquals(AUTO_FAIL, judge(transfer(UNKNOWN)))
        assertEquals(AUTO_FAIL, judge(transfer(SENDING)))
    }

    @Test
    fun `5 어느 상태든 금액이 다르면 불일치 - UNKNOWN 이어도 확정하지 않는다`() {
        for (status in TransferStatus.entries) {
            assertEquals(AMOUNT_OR_ACCOUNT_MISMATCH, judge(transfer(status), line(amount = 30_001)), "$status")
        }
    }

    @Test
    fun `5 계좌번호가 다르면 불일치`() {
        for (status in TransferStatus.entries) {
            assertEquals(AMOUNT_OR_ACCOUNT_MISMATCH, judge(transfer(status), line(accountNo = "1002123456780")), "$status")
        }
    }

    @Test
    fun `5 은행코드가 다르면 불일치`() = assertEquals(AMOUNT_OR_ACCOUNT_MISMATCH, judge(transfer(UNKNOWN), line(bankCode = "091")))

    @Test
    fun `6 COMPLETED + 없음 = COMPLETED_NOT_AT_BANK`() = assertEquals(COMPLETED_NOT_AT_BANK, judge(transfer(COMPLETED)))

    @Test
    fun `7 FAILED + 있음 = REFUNDED_BUT_PAID (환불한 돈이 나감)`() = assertEquals(REFUNDED_BUT_PAID, judge(transfer(FAILED), line()))

    @Test
    fun `8 RECEIVED + 있음 = PAID_BUT_RECEIVED (긴급 - 재발행되면 이중 지급)`() {
        assertEquals(PAID_BUT_RECEIVED, judge(transfer(RECEIVED), line()))
        assertTrue(PAID_BUT_RECEIVED.issue!!.urgent)
    }

    @Test
    fun `RECEIVED + 없음 = 재시도 중 - 넘어간다 (이슈 없음)`() {
        assertEquals(RETRYING, judge(transfer(RECEIVED)))
        assertNull(RETRYING.issue)
    }

    @Test
    fun `9 우리에게 없는 거래번호 = UNKNOWN_REFERENCE - 줄이 여럿이어도`() {
        assertEquals(UNKNOWN_REFERENCE, judge(null, line()))
        assertEquals(UNKNOWN_REFERENCE, judge(null, line(), line(lineNo = 3)))
    }

    @Test
    fun `11 같은 거래번호 2줄 이상 = PAID_TWICE (긴급) - 어느 상태든, 금액이 달라도`() {
        for (status in TransferStatus.entries) {
            assertEquals(PAID_TWICE, judge(transfer(status), line(), line(lineNo = 3, amount = 1)), "$status")
        }
        assertTrue(PAID_TWICE.issue!!.urgent)
    }

    @Test
    fun `일치 · 재시도 중만 이슈가 없고, 자동 확정은 감사 기록(불일치 아님), 나머지는 불일치`() {
        val withoutIssue = ReconVerdict.entries.filter { it.issue == null }.toSet()
        assertEquals(setOf(MATCHED, RETRYING), withoutIssue)
        assertFalse(AUTO_COMPLETE.issue!!.mismatch)
        assertFalse(AUTO_FAIL.issue!!.mismatch)
        assertTrue(ReconVerdict.entries.filter { it.issue != null && it != AUTO_COMPLETE && it != AUTO_FAIL }.all { it.issue!!.mismatch })
        assertTrue(ReconIssueType.BALANCE_LEDGER_MISMATCH.mismatch)
    }

    @Test
    fun `판정할 것이 없거나 다른 거래번호의 줄이면 거부 (호출 쪽 버그)`() {
        assertFailsWith<IllegalArgumentException> { judge(null) }
        assertFailsWith<IllegalArgumentException> { judge(transfer(UNKNOWN), line(reference = "T261028OTHERREF00001")) }
    }

    private companion object {
        const val REF = "T261028MATCHER000001"
    }
}
```

`StatementSettlerTest.kt`:
```kotlin
package io.github.hyeokjinon.banktransfer.transfer

import java.time.Clock
import java.time.Instant
import java.time.LocalDate
import java.time.ZoneOffset
import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class StatementSettlerTest {

    /**
     * 출금 하나만 담는 가짜 저장소. 조건부 전이 · 실패 확정은 현재 상태가 from 일 때만 바꾼다 (진짜와 같다).
     * [raceTo] 를 주면 첫 확정 시도 직전에 다른 쪽(정리 작업 · 컨슈머)이 그 상태로 먼저 바꾼 것처럼 만든다.
     * [snapshotStale] 이면 확정은 늘 0건이고 다시 읽어도 옛 상태가 보인다 (REPEATABLE READ 스냅숏 흉내).
     */
    private class OneTransferStore(var current: Transfer) : TransferStore {
        val calls = mutableListOf<String>()
        var raceTo: TransferStatus? = null
        var snapshotStale = false

        private fun changes(from: TransferStatus): Boolean {
            raceTo?.let { current = current.copy(status = it); raceTo = null }
            return !snapshotStale && current.status == from
        }

        override fun findById(id: UUID): Transfer? = current.takeIf { it.id == id }

        override fun transition(id: UUID, from: TransferStatus, to: TransferStatus, at: Instant, bankResponseCode: String?): Boolean {
            calls += "transition $from->$to $bankResponseCode"
            if (!changes(from)) return false
            current = current.copy(status = to, bankResponseCode = bankResponseCode)
            return true
        }

        override fun fail(id: UUID, from: TransferStatus, at: Instant, bankResponseCode: String): Boolean {
            calls += "fail $from $bankResponseCode"
            if (!changes(from)) return false
            current = current.copy(status = TransferStatus.FAILED, bankResponseCode = bankResponseCode)
            return true
        }

        override fun saveWithEvent(transfer: Transfer, event: TransferEvent): Transfer = error("not used")
        override fun findByIdempotencyKey(userId: String, key: String): Transfer? = error("not used")
        override fun findSendingBefore(cutoff: Instant, limit: Int): List<Transfer> = error("not used")
        override fun findUnknownForInquiry(maxAttempts: Int, sendingBefore: Instant, limit: Int): List<Transfer> = error("not used")
        override fun recordInquiryAttempt(id: UUID, at: Instant): Boolean = error("not used")
        override fun findRepublishable(updatedBefore: Instant, limit: Int): List<Transfer> = error("not used")
        override fun republish(id: UUID, expectedCount: Int, at: Instant): Boolean = error("not used")
    }

    private val clock = Clock.fixed(Instant.parse("2026-10-28T17:00:00Z"), ZoneOffset.UTC)

    private fun transfer(status: TransferStatus) = Transfer(
        id = UUID.randomUUID(), userId = "u", idempotencyKey = "k", reference = REF, requestHash = "h",
        bankCode = "090", accountNo = "1002123456789", amount = 30_000, memo = "m",
        status = status, createdAt = Instant.EPOCH, updatedAt = Instant.EPOCH,
    )

    private val paid = BankStatementLine(LocalDate.of(2026, 10, 28), 2, REF, "090", "1002123456789", 30_000, Instant.EPOCH)

    @Test
    fun `UNKNOWN + 같은 줄 - COMPLETED(0000) 로 확정하고 판정 시점 상태를 돌려준다`() {
        val store = OneTransferStore(transfer(TransferStatus.UNKNOWN))
        var askedFor: String? = null

        val settlement = StatementSettler(store, clock).settle(store.current.id) { askedFor = it; listOf(paid) }

        assertEquals(ReconVerdict.AUTO_COMPLETE, settlement.verdict)
        assertEquals(TransferStatus.UNKNOWN, settlement.transfer!!.status)
        assertEquals(REF, askedFor)
        assertEquals(listOf("transition UNKNOWN->COMPLETED 0000"), store.calls)
        assertEquals(TransferStatus.COMPLETED, store.current.status)
    }

    @Test
    fun `SENDING + 없음 - fail(E002) 로 확정 (환불은 저장소가 같은 트랜잭션에서)`() {
        val store = OneTransferStore(transfer(TransferStatus.SENDING))

        val settlement = StatementSettler(store, clock).settle(store.current.id) { emptyList() }

        assertEquals(ReconVerdict.AUTO_FAIL, settlement.verdict)
        assertEquals(listOf("fail SENDING E002"), store.calls)
        assertEquals("E002", store.current.bankResponseCode)
        assertEquals("E002", StatementSettler.NOT_AT_BANK)
    }

    @Test
    fun `확정된 건은 건드리지 않는다 - 판정만 돌려준다`() {
        val store = OneTransferStore(transfer(TransferStatus.COMPLETED))

        val settlement = StatementSettler(store, clock).settle(store.current.id) { emptyList() }

        assertEquals(ReconVerdict.COMPLETED_NOT_AT_BANK, settlement.verdict)
        assertTrue(store.calls.isEmpty())
    }

    @Test
    fun `경합 - 확정이 0건이면(그 사이 정리 작업이 COMPLETED) 다시 읽어 바뀐 상태로 판정한다`() {
        val store = OneTransferStore(transfer(TransferStatus.UNKNOWN)).apply { raceTo = TransferStatus.COMPLETED }

        val settlement = StatementSettler(store, clock).settle(store.current.id) { listOf(paid) }

        assertEquals(ReconVerdict.MATCHED, settlement.verdict)
        assertEquals(TransferStatus.COMPLETED, settlement.transfer!!.status)
        assertEquals(listOf("transition UNKNOWN->COMPLETED 0000"), store.calls)   // 두 번째 판정은 일치라 확정하지 않는다
    }

    @Test
    fun `경합 - 그 사이 결과조회가 FAILED 로 확정했는데 은행 줄이 있으면 REFUNDED_BUT_PAID`() {
        val store = OneTransferStore(transfer(TransferStatus.UNKNOWN)).apply { raceTo = TransferStatus.FAILED }

        val settlement = StatementSettler(store, clock).settle(store.current.id) { listOf(paid) }

        assertEquals(ReconVerdict.REFUNDED_BUT_PAID, settlement.verdict)
    }

    @Test
    fun `다시 읽어도 계속 바뀌면(옛 스냅숏) 3번 뒤 포기 - 단계가 실패하고 재시작으로 이어간다`() {
        val store = OneTransferStore(transfer(TransferStatus.UNKNOWN)).apply { snapshotStale = true }

        assertFailsWith<IllegalStateException> { StatementSettler(store, clock).settle(store.current.id) { listOf(paid) } }
        assertEquals(3, store.calls.size)
    }

    @Test
    fun `우리에게 없는 거래번호의 줄 - UNKNOWN_REFERENCE, 저장소를 보지 않는다`() {
        val store = OneTransferStore(transfer(TransferStatus.UNKNOWN))

        val settlement = StatementSettler(store, clock).unknownReference(listOf(paid))

        assertEquals(ReconVerdict.UNKNOWN_REFERENCE, settlement.verdict)
        assertEquals(null, settlement.transfer)
        assertTrue(store.calls.isEmpty())
    }

    private companion object {
        const val REF = "T261028SETTLER000001"
    }
}
```

- [ ] **Step 2: 실패 확인**

Run: `./gradlew :modules:transfer:test --tests '*StatementMatcherTest*' --tests '*StatementSettlerTest*'`
Expected: FAIL (컴파일: `Unresolved reference 'BankStatementLine'`, `'StatementMatcher'`, `'StatementSettler'`)

- [ ] **Step 3: 구현**

`StatementMatching.kt`:
```kotlin
package io.github.hyeokjinon.banktransfer.transfer

import io.github.hyeokjinon.banktransfer.transfer.TransferStatus.COMPLETED
import io.github.hyeokjinon.banktransfer.transfer.TransferStatus.FAILED
import io.github.hyeokjinon.banktransfer.transfer.TransferStatus.RECEIVED
import io.github.hyeokjinon.banktransfer.transfer.TransferStatus.SENDING
import io.github.hyeokjinon.banktransfer.transfer.TransferStatus.UNKNOWN
import java.time.Clock
import java.time.Instant
import java.time.LocalDate
import java.util.UUID

/** 은행 대사 파일의 상세 한 줄 (P5-1). 같은 거래번호가 여러 날짜 · 여러 줄에 있을 수 있다 (이중 지급). */
data class BankStatementLine(
    val bankDate: LocalDate,
    val lineNo: Int,
    val reference: String,
    val bankCode: String,
    val accountNo: String,
    val amount: Long,
    val processedAt: Instant,
) {
    override fun toString(): String =
        "BankStatementLine(bankDate=$bankDate, lineNo=$lineNo, reference=$reference, bankCode=$bankCode, accountNo=${maskAccountNo(accountNo)}, amount=$amount)"
}

/**
 * 일마감 대사가 남기는 기록의 종류 (recon_issue.type, P5-1).
 * [mismatch] = 사람이 봐야 하는 어긋남 (자동 확정 AUTO_* 는 감사 기록일 뿐 어긋남이 아니다). [urgent] = 돈이 두 번 나갔거나 나갈 수 있다.
 */
enum class ReconIssueType(val mismatch: Boolean, val urgent: Boolean = false) {
    AUTO_COMPLETED(mismatch = false),
    AUTO_FAILED(mismatch = false),
    AMOUNT_OR_ACCOUNT_MISMATCH(mismatch = true),
    COMPLETED_NOT_AT_BANK(mismatch = true),
    REFUNDED_BUT_PAID(mismatch = true),
    PAID_BUT_RECEIVED(mismatch = true, urgent = true),
    UNKNOWN_REFERENCE(mismatch = true),
    BALANCE_LEDGER_MISMATCH(mismatch = true),
    PAID_TWICE(mismatch = true, urgent = true),
}

/** 거래번호로 짝지은 판정 (설계 §6 표, 주석의 번호가 표의 #). [issue] 가 있으면 recon_issue 에 남긴다. */
enum class ReconVerdict(val issue: ReconIssueType?) {
    /** 1 COMPLETED + 같은 줄, 2 FAILED + 없음. */
    MATCHED(null),

    /** 3 UNKNOWN · SENDING + 같은 줄 → COMPLETED. */
    AUTO_COMPLETE(ReconIssueType.AUTO_COMPLETED),

    /** 4 UNKNOWN · SENDING + 없음 → FAILED E002 + 환불. */
    AUTO_FAIL(ReconIssueType.AUTO_FAILED),

    /** 5 어느 상태든 금액 · 은행코드 · 계좌가 다름 — 3 이어도 확정하지 않는다. */
    AMOUNT_OR_ACCOUNT_MISMATCH(ReconIssueType.AMOUNT_OR_ACCOUNT_MISMATCH),

    /** 6 COMPLETED + 없음. */
    COMPLETED_NOT_AT_BANK(ReconIssueType.COMPLETED_NOT_AT_BANK),

    /** 7 FAILED + 있음 — 환불한 돈이 나갔다. */
    REFUNDED_BUT_PAID(ReconIssueType.REFUNDED_BUT_PAID),

    /** 8 RECEIVED + 있음 — 재발행되면 이중 지급 (긴급). */
    PAID_BUT_RECEIVED(ReconIssueType.PAID_BUT_RECEIVED),

    /** — RECEIVED + 없음: 재시도 중 (재발행 · E001 이 맡는다). */
    RETRYING(null),

    /** 9 우리에게 없는 거래번호의 T 줄. */
    UNKNOWN_REFERENCE(ReconIssueType.UNKNOWN_REFERENCE),

    /** 11 같은 거래번호 2줄 이상 (긴급). */
    PAID_TWICE(ReconIssueType.PAID_TWICE),
}

/**
 * 일마감 대사의 판정 (P5-1, 설계 §6). 순수 함수 — 배치는 읽기 · 쓰기 · 연결만 한다.
 * [lines] = 이 거래번호의 **적재된 모든 날짜의** 은행 줄. 순서: 줄 2개 이상(이중 지급)이 금액 비교보다 먼저다 — 둘 중 하나만 맞아도 돈은 두 번 나갔다.
 */
object StatementMatcher {

    fun judge(transfer: Transfer?, lines: List<BankStatementLine>): ReconVerdict {
        if (transfer == null) {
            require(lines.isNotEmpty()) { "nothing to judge: no transfer and no bank line" }
            return ReconVerdict.UNKNOWN_REFERENCE
        }
        require(lines.all { it.reference == transfer.reference }) { "bank lines of another reference than ${transfer.reference}" }
        if (lines.size >= 2) return ReconVerdict.PAID_TWICE
        val line = lines.singleOrNull()
            ?: return when (transfer.status) {
                COMPLETED -> ReconVerdict.COMPLETED_NOT_AT_BANK
                FAILED -> ReconVerdict.MATCHED
                UNKNOWN, SENDING -> ReconVerdict.AUTO_FAIL
                RECEIVED -> ReconVerdict.RETRYING
            }
        if (line.amount != transfer.amount || line.bankCode != transfer.bankCode || line.accountNo != transfer.accountNo) {
            return ReconVerdict.AMOUNT_OR_ACCOUNT_MISMATCH
        }
        return when (transfer.status) {
            COMPLETED -> ReconVerdict.MATCHED
            UNKNOWN, SENDING -> ReconVerdict.AUTO_COMPLETE
            FAILED -> ReconVerdict.REFUNDED_BUT_PAID
            RECEIVED -> ReconVerdict.PAID_BUT_RECEIVED
        }
    }
}

/** 판정 하나와 그때의 우리 출금 · 은행 줄 (기록 · 알림용). [transfer] 는 판정한 시점의 상태다 — 자동 확정 전. */
data class Settlement(val verdict: ReconVerdict, val transfer: Transfer?, val lines: List<BankStatementLine>)

/**
 * 판정하고, **결과를 모르는 출금(UNKNOWN · SENDING)만** 확정한다 (P5-1, 설계 §2 "자동 보정 범위").
 * 이미 확정된 건을 뒤집으면 보정이 새 사고가 된다(환불한 돈을 다시 빼려는데 잔고 부족 등) → 판정만 돌려주고 기록 · 알림은 부르는 쪽이 한다.
 * 확정은 기존 조건부 전이(→ COMPLETED, 0000)와 [TransferStore.fail](→ FAILED E002 + 환불, 한 트랜잭션)을 그대로 쓴다.
 * 0건이면(그 사이 정리 작업 · 컨슈머가 바꿈) 다시 읽어 바뀐 상태로 다시 판정한다. [maxAttempts] 번 넘게 계속 바뀌면 예외 —
 * 배치 단계가 실패하고 재시작으로 이어간다 (저장소가 옛 스냅숏을 보는 설정이면 여기서 드러난다).
 */
class StatementSettler(
    private val store: TransferStore,
    private val clock: Clock = Clock.systemUTC(),
    private val maxAttempts: Int = 3,
) {

    fun settle(transferId: UUID, linesOf: (String) -> List<BankStatementLine>): Settlement {
        var transfer = read(transferId)
        val lines = linesOf(transfer.reference)
        repeat(maxAttempts) {
            val verdict = StatementMatcher.judge(transfer, lines)
            val settled = when (verdict) {
                ReconVerdict.AUTO_COMPLETE -> store.transition(transfer.id, transfer.status, COMPLETED, clock.instant(), BANK_OK)
                ReconVerdict.AUTO_FAIL -> store.fail(transfer.id, transfer.status, clock.instant(), NOT_AT_BANK)
                else -> true
            }
            if (settled) return Settlement(verdict, transfer, lines)
            transfer = read(transferId)
        }
        error("transfer $transferId kept changing during daily reconciliation ($maxAttempts attempts) — the step fails and resumes on restart")
    }

    /** 우리에게 없는 거래번호의 은행 줄 (④). 저장소를 보지 않는다. */
    fun unknownReference(lines: List<BankStatementLine>): Settlement = Settlement(StatementMatcher.judge(null, lines), null, lines)

    private fun read(id: UUID): Transfer = store.findById(id) ?: error("transfer $id vanished during daily reconciliation")

    companion object {
        /** 일마감 대사로 "은행이 처리하지 않았다" 를 확정 (이 서비스의 자체 코드 — E001 재발행 포기와 구분). 돈은 움직이지 않았다. */
        const val NOT_AT_BANK = "E002"
        private const val BANK_OK = "0000"
    }
}
```

- [ ] **Step 4: 통과 확인**

Run: `./gradlew :modules:transfer:test`
Expected: PASS (새 테스트 22개 + 기존 도메인 테스트)

- [ ] **Step 5: 변이 검사 (일회성, 커밋하지 않음)** — 표 테스트가 비교 하나하나를 실제로 지키는지 확인한다 (스펙 §10).
  1. `StatementMatcher.judge` 의 `line.amount != transfer.amount || ` 를 지운다 → Run: `./gradlew :modules:transfer:test --tests '*StatementMatcherTest*'` → `5 어느 상태든 금액이 다르면 불일치` 가 FAIL 해야 한다 (예상: `UNKNOWN` 에서 `AUTO_COMPLETE` 가 나와 단언 실패). 실제 실패 테스트 이름 · 메시지를 적어 둔다. 되돌린다.
  2. `|| line.accountNo != transfer.accountNo` 를 지운다 → `5 계좌번호가 다르면 불일치` FAIL. 되돌린다.
  3. `lines.size >= 2` 를 `lines.size > 2` 로 → `11 같은 거래번호 2줄 이상` FAIL. 되돌린다.
  4. 줄이 없을 때의 `RECEIVED -> ReconVerdict.RETRYING` 을 `RECEIVED -> ReconVerdict.AUTO_FAIL` 로 → `RECEIVED + 없음 = 재시도 중` FAIL. 되돌린다.
  5. `StatementSettler.settle` 의 `transfer = read(transferId)` 를 지운다 → `StatementSettlerTest` 의 `경합 - … 다시 읽어 바뀐 상태로 판정한다` FAIL. 되돌린다.
  6. `git diff modules/transfer/src/main` 이 이 Task 의 새 파일 외에 없는지, 다섯 곳이 모두 되돌려졌는지 확인하고 `./gradlew :modules:transfer:test` 가 다시 PASS 하는지 본다.
  7. 1~5 의 결과를 한 줄씩(예: "금액 비교 제거 → `5 어느 상태든 …` 실패: expected AMOUNT_OR_ACCOUNT_MISMATCH but was AUTO_COMPLETE (UNKNOWN)") 아래 커밋 본문에 넣는다 (Task 12 에서 ADR-0010 결과 절로 옮긴다).

- [ ] **Step 6: 커밋**

```bash
git add modules/transfer
git commit -m "feat(transfer): 일마감 대사 판정 StatementMatcher(설계 §6 표) · 미확정만 확정하는 StatementSettler(E002)

판정은 순수 함수라 표를 줄마다 그대로 테스트한다. 결과를 모르는 출금만 기존 조건부 전이 · fail(E002 + 환불)로 확정하고,
이미 확정된 건은 판정만 돌려준다(뒤집으면 보정이 새 사고가 된다). 0건이면 다시 읽어 바뀐 상태로 다시 판정한다.
변이 검사: <Step 5 의 실제 결과 다섯 줄>

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```
(`<Step 5 의 실제 결과 다섯 줄>` 은 Step 5-7 에서 적어 둔 실제 출력으로 바꿔 쓴다 — 계획 작성 시점에는 알 수 없는 실행 결과다.)

---

### Task 9: Flyway V4 — Spring Batch 6 메타 표 · `bank_statement_line` · `recon_issue`, JDBC 잡 저장소, 대사 SQL `ReconRepository`

**Files:**
- Modify: `apps/api/build.gradle.kts` (의존성 1줄)
- Modify: `apps/api/src/main/resources/application.yml`
- Create: `apps/api/src/main/resources/db/migration/V4__daily_reconciliation.sql`
- Create: `apps/api/src/main/kotlin/io/github/hyeokjinon/banktransfer/api/dailyrecon/ReconRepository.kt`
- Test: `apps/api/src/test/kotlin/io/github/hyeokjinon/banktransfer/api/DatabaseSetupTest.kt` (테스트 2개 추가), `apps/api/src/test/kotlin/io/github/hyeokjinon/banktransfer/api/dailyrecon/ReconRepositoryTest.kt` (신규)

**Interfaces:**
- Consumes: Task 8 `BankStatementLine`, `ReconIssueType`
- Produces:
  - 표 `BATCH_*`(9개), `bank_statement_line`, `recon_issue` (`ref_key` 생성 컬럼, `uk_recon_issue (recon_date, type, ref_key)`)
  - `data class ReconIssue(reconDate: LocalDate, type: ReconIssueType, reference: String? = null, transferId: UUID? = null, userId: String? = null, ourValue: String? = null, bankValue: String? = null)` (reference 와 userId 둘 다 없으면 `IllegalArgumentException`)
  - `data class ReconIssueRow(id: Long, type: ReconIssueType, reference: String?, ourValue: String?, bankValue: String?)` (사용자 id 는 싣지 않는다)
  - `data class LedgerMismatch(userId: String, balance: Long, ledgerSum: Long)`
  - `class ReconRepository(jdbc: NamedParameterJdbcTemplate) { fun insertLines(lines: List<BankStatementLine>, loadedAt: Instant); fun linesOf(reference: String): List<BankStatementLine>; fun countLines(bankDate: LocalDate, referencePrefix: String): Int; fun insertIssue(issue: ReconIssue, at: Instant); fun issueCounts(reconDate: LocalDate): Map<ReconIssueType, Int>; fun issuesOf(reconDate: LocalDate): List<ReconIssueRow>; fun ledgerMismatches(): List<LedgerMismatch> }`
  - 테스트: `ReconTestDates.unique(): LocalDate` (`dailyrecon` 테스트 패키지)

결정(계획): `ref_key` STORED 생성 컬럼 + NOT NULL, `resolution_note VARCHAR(500)`, 열린 이슈 인덱스, 중복은 `ON DUPLICATE KEY UPDATE` no-op, `sending_at` 인덱스 추가 안 함 (위 "결정(계획) 모음").

- [ ] **Step 1: 실패하는 테스트**

`dailyrecon/ReconTestDates.kt` (테스트 소스, 대사 테스트 공용):
```kotlin
package io.github.hyeokjinon.banktransfer.api.dailyrecon

import java.time.LocalDate
import kotlin.random.Random

/**
 * 공유 DB 의 대사 테스트는 테스트마다 다른 대사일을 쓴다 (설계 §10): 잡 인스턴스 · 이슈 · 적재 줄이 날짜로 갈리고,
 * ③ 은 그날(KST) 보낸 출금만 본다. 진짜 날짜와 겹치지 않게 먼 미래(2100 ~ 7575년)에서 고른다. 앞뒤 하루도 쓸 수 있게 여유를 둔다.
 */
object ReconTestDates {
    fun unique(): LocalDate = LocalDate.of(2100, 1, 1).plusDays(Random.nextLong(0, 2_000_000))
}
```

`dailyrecon/ReconRepositoryTest.kt`:
```kotlin
package io.github.hyeokjinon.banktransfer.api.dailyrecon

import io.github.hyeokjinon.banktransfer.api.support.ApiIntegrationTest
import io.github.hyeokjinon.banktransfer.transfer.BankStatementLine
import io.github.hyeokjinon.banktransfer.transfer.ReconIssueType
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.jdbc.core.JdbcTemplate
import java.time.Instant
import java.time.ZoneId
import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class ReconRepositoryTest : ApiIntegrationTest() {

    @Autowired private lateinit var repo: ReconRepository
    @Autowired private lateinit var jdbc: JdbcTemplate

    private val seoul = ZoneId.of("Asia/Seoul")

    private fun line(day: java.time.LocalDate, lineNo: Int, reference: String, amount: Long = 30_000) =
        BankStatementLine(day, lineNo, reference, "090", "1002123456789", amount, day.atTime(10, 15, 30).atZone(seoul).toInstant())

    @Test
    fun `은행 줄 - 같은 줄을 다시 넣어도 그대로, 거래번호의 모든 날짜 줄을 날짜 · 줄 순으로, 처리 시각은 KST 로 저장`() {
        val day = ReconTestDates.unique()
        val ref = "T" + UUID.randomUUID().toString().replace("-", "").take(19).uppercase()
        val lines = listOf(line(day, 2, ref), line(day, 3, "V" + ref.drop(1), amount = 1))

        repo.insertLines(lines, Instant.now())
        repo.insertLines(lines, Instant.now())                     // ② 재시작 — 무시
        repo.insertLines(listOf(line(day.plusDays(1), 2, ref)), Instant.now())

        assertEquals(listOf(day to 2, day.plusDays(1) to 2), repo.linesOf(ref).map { it.bankDate to it.lineNo })
        assertEquals(lines.first(), repo.linesOf(ref).first())
        assertEquals(1, repo.countLines(day, "T"))
        assertEquals(1, repo.countLines(day, "V"))
        assertEquals(
            "10:15:30",
            jdbc.queryForObject("SELECT DATE_FORMAT(processed_at, '%H:%i:%s') FROM bank_statement_line WHERE bank_date = ? AND line_no = 2", String::class.java, day),
        )
    }

    @Test
    fun `이슈 - 같은 날 · 종류 · 키는 한 번만, 지갑 이슈는 user_id 가 키`() {
        val day = ReconTestDates.unique()
        val ref = "T261028ISSUE" + UUID.randomUUID().toString().take(8).uppercase()
        val issue = ReconIssue(day, ReconIssueType.PAID_TWICE, reference = ref, transferId = UUID.randomUUID(), userId = "u-1", ourValue = "COMPLETED 1000", bankValue = "1000; 1000")

        repo.insertIssue(issue, Instant.now())
        repo.insertIssue(issue, Instant.now())                     // 재시작 · 재실행 — 늘지 않는다
        repo.insertIssue(issue.copy(type = ReconIssueType.AUTO_COMPLETED), Instant.now())
        repo.insertIssue(ReconIssue(day, ReconIssueType.BALANCE_LEDGER_MISMATCH, userId = "wallet-user", ourValue = "balance=1", bankValue = "ledgerSum=0"), Instant.now())

        assertEquals(
            mapOf(ReconIssueType.PAID_TWICE to 1, ReconIssueType.AUTO_COMPLETED to 1, ReconIssueType.BALANCE_LEDGER_MISMATCH to 1),
            repo.issueCounts(day),
        )
        assertEquals(listOf(ref, ref, null), repo.issuesOf(day).map { it.reference })
        assertEquals("wallet-user", jdbc.queryForObject("SELECT ref_key FROM recon_issue WHERE recon_date = ? AND type = 'BALANCE_LEDGER_MISMATCH'", String::class.java, day))
        assertFailsWith<IllegalArgumentException> { ReconIssue(day, ReconIssueType.UNKNOWN_REFERENCE) }
    }

    @Test
    fun `잔고 ≠ 원장 합인 지갑을 찾는다 - 맞는 지갑은 없다`() {
        val healthy = fundedUser(10_000)
        val broken = fundedUser(10_000).also { jdbc.update("UPDATE wallet SET balance = balance + 1 WHERE user_id = ?", it) }

        val mismatches = repo.ledgerMismatches()

        assertTrue(mismatches.contains(LedgerMismatch(broken, 10_001, 10_000)))
        assertFalse(mismatches.any { it.userId == healthy })
    }
}
```

`DatabaseSetupTest.kt` — 클래스 끝(마지막 `}` 앞)에 추가 (import: `org.springframework.batch.core.job.parameters.JobParametersBuilder`, `org.springframework.batch.core.repository.JobRepository`, `java.time.LocalDate`, `java.util.UUID`):
```kotlin

    @Autowired
    private lateinit var jobRepository: JobRepository

    @Test
    fun `V4 가 Spring Batch 메타 표 · bank_statement_line · recon_issue 를 만들었다`() {
        val tables = jdbc.queryForList(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = DATABASE()",
            String::class.java,
        ).toSet()
        val expected = setOf(
            "BATCH_JOB_INSTANCE", "BATCH_JOB_EXECUTION", "BATCH_JOB_EXECUTION_PARAMS", "BATCH_STEP_EXECUTION",
            "BATCH_STEP_EXECUTION_CONTEXT", "BATCH_JOB_EXECUTION_CONTEXT", "BATCH_STEP_EXECUTION_SEQ", "BATCH_JOB_EXECUTION_SEQ",
            "BATCH_JOB_INSTANCE_SEQ", "bank_statement_line", "recon_issue",
        )
        assertEquals(expected, tables.intersect(expected))

        fun indexColumns(table: String, index: String) = jdbc.queryForList(
            "SELECT column_name FROM information_schema.statistics WHERE table_schema = DATABASE() AND table_name = ? AND index_name = ? ORDER BY seq_in_index",
            String::class.java, table, index,
        )
        assertEquals(listOf("bank_date", "line_no"), indexColumns("bank_statement_line", "PRIMARY"))
        assertEquals(listOf("recon_date", "type", "ref_key"), indexColumns("recon_issue", "uk_recon_issue"))
        for (table in listOf("bank_statement_line", "recon_issue")) {
            val collation = jdbc.queryForObject(
                "SELECT collation_name FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = ? AND column_name = 'reference'",
                String::class.java, table,
            )
            assertEquals("ascii_bin", collation, "$table.reference")
        }
    }

    @Test
    fun `잡 저장소는 JDBC 다 - 잡 인스턴스가 BATCH_JOB_INSTANCE 에 남는다 (starter-batch 만이면 메모리 저장소)`() {
        val name = "p5-1-probe-${UUID.randomUUID()}"

        jobRepository.createJobInstance(name, JobParametersBuilder().addLocalDate("reconDate", LocalDate.of(2100, 1, 1)).toJobParameters())

        assertEquals(1, jdbc.queryForObject("SELECT COUNT(*) FROM BATCH_JOB_INSTANCE WHERE JOB_NAME = ?", Int::class.java, name))
    }
```

- [ ] **Step 2: 실패 확인**

Run: `./gradlew :apps:api:test --tests '*DatabaseSetupTest*' --tests '*ReconRepositoryTest*'`
Expected: FAIL (컴파일: `Unresolved reference 'batch'` — 의존성이 아직 없다, `Unresolved reference 'ReconRepository'`)

- [ ] **Step 3: 구현**

`apps/api/build.gradle.kts` 의 `implementation("org.springframework.boot:spring-boot-starter-kafka")` 줄 아래에 추가:
```kotlin
    // 일마감 대사 (P5-1). Boot 4 는 배치 자동 설정을 나눴다: starter-batch 만이면 메모리 잡 저장소(ResourcelessJobRepository)라
    // 재시작 · 중복 거부가 프로세스를 넘지 못한다. JDBC 잡 저장소(BATCH_* 표)는 이 스타터가 켠다. 버전은 Boot BOM (Spring Batch 6.0.5)
    implementation("org.springframework.boot:spring-boot-starter-batch-jdbc")
```

`application.yml` 의 `spring:` 절 안(`jpa:` 블록 아래)에 추가:
```yaml
  batch:
    job:
      enabled: false           # 기동 때 잡을 자동 실행하지 않는다 (기본 true). 대사는 스케줄 · 관리 API 가 날짜를 주어 돌린다 (P5-1)
    jdbc:
      initialize-schema: never # BATCH_* 표는 Flyway V4 가 만든다 (스키마는 Flyway 로만)
```

`V4__daily_reconciliation.sql`:
```sql
-- P5-1 일마감 대사. 정의와 이유는 docs/database.md §4-6 ~ §4-8 과 ADR-0010. V1 ~ V3 은 고치지 않는다.

-- 1) Spring Batch 6.0.5 메타데이터 표. spring-batch-core-6.0.5.jar 의 org/springframework/batch/core/schema-mysql.sql 을
--    한 글자도 바꾸지 않고 옮겼다 (spring.batch.jdbc.initialize-schema=never — 스키마는 Flyway 로만).
--    Spring Batch 를 올릴 때는 새 버전의 스크립트와 비교해 차이만 새 V 파일로 반영한다.
--    주의: 이 표들의 시각은 Spring Batch 가 LocalDateTime.now() 로 쓴다 = JVM 기본 시간대 (KST 저장 정책의 예외, database.md §2).
-- Autogenerated: do not edit this file

CREATE TABLE BATCH_JOB_INSTANCE (
	JOB_INSTANCE_ID BIGINT  NOT NULL PRIMARY KEY,
	VERSION BIGINT,
	JOB_NAME VARCHAR(100) NOT NULL,
	JOB_KEY VARCHAR(32) NOT NULL,
	constraint JOB_INST_UN unique (JOB_NAME, JOB_KEY)
) ENGINE=InnoDB;

CREATE TABLE BATCH_JOB_EXECUTION (
	JOB_EXECUTION_ID BIGINT  NOT NULL PRIMARY KEY,
	VERSION BIGINT,
	JOB_INSTANCE_ID BIGINT NOT NULL,
	CREATE_TIME DATETIME(6) NOT NULL,
	START_TIME DATETIME(6) DEFAULT NULL,
	END_TIME DATETIME(6) DEFAULT NULL,
	STATUS VARCHAR(10),
	EXIT_CODE VARCHAR(2500),
	EXIT_MESSAGE VARCHAR(2500),
	LAST_UPDATED DATETIME(6),
	constraint JOB_INST_EXEC_FK foreign key (JOB_INSTANCE_ID)
	references BATCH_JOB_INSTANCE(JOB_INSTANCE_ID)
) ENGINE=InnoDB;

CREATE TABLE BATCH_JOB_EXECUTION_PARAMS (
	JOB_EXECUTION_ID BIGINT NOT NULL,
	PARAMETER_NAME VARCHAR(100) NOT NULL,
	PARAMETER_TYPE VARCHAR(100) NOT NULL,
	PARAMETER_VALUE VARCHAR(2500),
	IDENTIFYING CHAR(1) NOT NULL,
	constraint JOB_EXEC_PARAMS_FK foreign key (JOB_EXECUTION_ID)
	references BATCH_JOB_EXECUTION(JOB_EXECUTION_ID)
) ENGINE=InnoDB;

CREATE TABLE BATCH_STEP_EXECUTION (
	STEP_EXECUTION_ID BIGINT  NOT NULL PRIMARY KEY,
	VERSION BIGINT NOT NULL,
	STEP_NAME VARCHAR(100) NOT NULL,
	JOB_EXECUTION_ID BIGINT NOT NULL,
	CREATE_TIME DATETIME(6) NOT NULL,
	START_TIME DATETIME(6) DEFAULT NULL,
	END_TIME DATETIME(6) DEFAULT NULL,
	STATUS VARCHAR(10),
	COMMIT_COUNT BIGINT,
	READ_COUNT BIGINT,
	FILTER_COUNT BIGINT,
	WRITE_COUNT BIGINT,
	READ_SKIP_COUNT BIGINT,
	WRITE_SKIP_COUNT BIGINT,
	PROCESS_SKIP_COUNT BIGINT,
	ROLLBACK_COUNT BIGINT,
	EXIT_CODE VARCHAR(2500),
	EXIT_MESSAGE VARCHAR(2500),
	LAST_UPDATED DATETIME(6),
	constraint JOB_EXEC_STEP_FK foreign key (JOB_EXECUTION_ID)
	references BATCH_JOB_EXECUTION(JOB_EXECUTION_ID)
) ENGINE=InnoDB;

CREATE TABLE BATCH_STEP_EXECUTION_CONTEXT (
	STEP_EXECUTION_ID BIGINT NOT NULL PRIMARY KEY,
	SHORT_CONTEXT VARCHAR(2500) NOT NULL,
	SERIALIZED_CONTEXT TEXT,
	constraint STEP_EXEC_CTX_FK foreign key (STEP_EXECUTION_ID)
	references BATCH_STEP_EXECUTION(STEP_EXECUTION_ID)
) ENGINE=InnoDB;

CREATE TABLE BATCH_JOB_EXECUTION_CONTEXT (
	JOB_EXECUTION_ID BIGINT NOT NULL PRIMARY KEY,
	SHORT_CONTEXT VARCHAR(2500) NOT NULL,
	SERIALIZED_CONTEXT TEXT,
	constraint JOB_EXEC_CTX_FK foreign key (JOB_EXECUTION_ID)
	references BATCH_JOB_EXECUTION(JOB_EXECUTION_ID)
) ENGINE=InnoDB;

CREATE TABLE BATCH_STEP_EXECUTION_SEQ (
	ID BIGINT NOT NULL,
	UNIQUE_KEY CHAR(1) NOT NULL,
	constraint UNIQUE_KEY_UN unique (UNIQUE_KEY)
) ENGINE=InnoDB;

INSERT INTO BATCH_STEP_EXECUTION_SEQ (ID, UNIQUE_KEY) select * from (select 0 as ID, '0' as UNIQUE_KEY) as tmp where not exists(select * from BATCH_STEP_EXECUTION_SEQ);

CREATE TABLE BATCH_JOB_EXECUTION_SEQ (
	ID BIGINT NOT NULL,
	UNIQUE_KEY CHAR(1) NOT NULL,
	constraint UNIQUE_KEY_UN unique (UNIQUE_KEY)
) ENGINE=InnoDB;

INSERT INTO BATCH_JOB_EXECUTION_SEQ (ID, UNIQUE_KEY) select * from (select 0 as ID, '0' as UNIQUE_KEY) as tmp where not exists(select * from BATCH_JOB_EXECUTION_SEQ);

CREATE TABLE BATCH_JOB_INSTANCE_SEQ (
	ID BIGINT NOT NULL,
	UNIQUE_KEY CHAR(1) NOT NULL,
	constraint UNIQUE_KEY_UN unique (UNIQUE_KEY)
) ENGINE=InnoDB;

INSERT INTO BATCH_JOB_INSTANCE_SEQ (ID, UNIQUE_KEY) select * from (select 0 as ID, '0' as UNIQUE_KEY) as tmp where not exists(select * from BATCH_JOB_INSTANCE_SEQ);

-- 2) 은행 대사 파일의 상세 줄. append-only. 같은 파일을 다시 넣으면(② 재시작) (bank_date, line_no) 로 무시한다.
CREATE TABLE bank_statement_line (
  bank_date    DATE         NOT NULL,                                          -- 파일의 처리일 (KST)
  line_no      INT          NOT NULL,                                          -- 파일 안의 물리 줄 번호 (헤더 = 1)
  reference    CHAR(20)     CHARACTER SET ascii COLLATE ascii_bin NOT NULL,    -- 이용기관 거래번호 (T… 출금, V… 1원인증)
  bank_code    VARCHAR(3)   NOT NULL,
  account_no   VARCHAR(16)  NOT NULL,
  amount       BIGINT       NOT NULL,
  processed_at DATETIME(6)  NOT NULL,                                          -- 은행 처리 시각 (KST)
  loaded_at    DATETIME(6)  NOT NULL,
  PRIMARY KEY (bank_date, line_no),
  INDEX idx_statement_reference (reference)                                   -- ③ ④ 가 거래번호로 모든 날짜의 줄을 찾는다
);

-- 3) 대사가 찾은 어긋남과 자동 확정의 감사 기록. append-only — 사람이 SQL 로 resolved_at · resolution_note 만 채운다.
CREATE TABLE recon_issue (
  id              BIGINT       NOT NULL AUTO_INCREMENT PRIMARY KEY,
  recon_date      DATE         NOT NULL,
  type            VARCHAR(40)  NOT NULL,                                       -- AUTO_COMPLETED · AUTO_FAILED · … (ReconIssueType)
  reference       CHAR(20)     CHARACTER SET ascii COLLATE ascii_bin NULL,
  transfer_id     CHAR(36)     NULL,
  user_id         VARCHAR(36)  CHARACTER SET ascii COLLATE ascii_bin NULL,
  our_value       VARCHAR(200) NULL,
  bank_value      VARCHAR(200) NULL,
  -- 이슈를 가리키는 키: 거래번호, 지갑 이슈(거래번호 없음)면 user_id. 둘 다 없으면 INSERT 가 실패한다 (NOT NULL)
  ref_key         VARCHAR(36)  CHARACTER SET ascii COLLATE ascii_bin GENERATED ALWAYS AS (COALESCE(reference, user_id)) STORED NOT NULL,
  created_at      DATETIME(6)  NOT NULL,
  resolved_at     DATETIME(6)  NULL,
  resolution_note VARCHAR(500) NULL,
  -- 같은 날 · 같은 종류 · 같은 키는 한 번만: 잡을 어디서 다시 시작해도 이슈가 늘지 않는다
  UNIQUE KEY uk_recon_issue (recon_date, type, ref_key),
  INDEX idx_recon_issue_open (resolved_at, recon_date)                        -- 아직 처리하지 않은 이슈 (database.md §9)
);
```
(1) 의 `CREATE TABLE BATCH_…` ~ `INSERT INTO BATCH_JOB_INSTANCE_SEQ …` 는 jar 파일 전문이다 — 탭 들여쓰기까지 그대로 둔다. 구현할 때 다시 확인하려면: `unzip -p ~/.gradle/caches/modules-2/files-2.1/org.springframework.batch/spring-batch-core/6.0.5/*/spring-batch-core-6.0.5.jar org/springframework/batch/core/schema-mysql.sql | diff --strip-trailing-cr - <(sed -n '/^-- Autogenerated/,/^INSERT INTO BATCH_JOB_INSTANCE_SEQ/p' apps/api/src/main/resources/db/migration/V4__daily_reconciliation.sql)` → 차이 없음 (의존성을 추가한 뒤 한 번 빌드해야 jar 가 캐시에 있다).

`dailyrecon/ReconRepository.kt`:
```kotlin
package io.github.hyeokjinon.banktransfer.api.dailyrecon

import io.github.hyeokjinon.banktransfer.transfer.BankStatementLine
import io.github.hyeokjinon.banktransfer.transfer.ReconIssueType
import org.springframework.jdbc.core.RowMapper
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import java.sql.Timestamp
import java.sql.Types
import java.time.Instant
import java.time.LocalDate
import java.util.UUID

/** recon_issue 한 줄로 남길 것 (P5-1). 지갑 이슈는 거래번호가 없고 userId 가 키다. */
data class ReconIssue(
    val reconDate: LocalDate,
    val type: ReconIssueType,
    val reference: String? = null,
    val transferId: UUID? = null,
    val userId: String? = null,
    val ourValue: String? = null,
    val bankValue: String? = null,
) {
    init {
        require(reference != null || userId != null) { "an issue needs a reference or a user id (ref_key)" }
    }
}

/** 저장된 이슈 (알림 목록 · 보고용). 사용자 id 는 싣지 않는다 — 알림에 넣지 않는 값이다. */
data class ReconIssueRow(val id: Long, val type: ReconIssueType, val reference: String?, val ourValue: String?, val bankValue: String?)

/** 잔고 ≠ 원장 합인 지갑 (⑤). */
data class LedgerMismatch(val userId: String, val balance: Long, val ledgerSum: Long)

/**
 * 일마감 대사의 SQL (P5-1). bank_statement_line · recon_issue 는 append-only 기록이고, 같은 것을 다시 넣으면 무시한다 —
 * 잡이 어디서 다시 시작해도 결과가 같다. 시각은 Timestamp(Instant) 로 넘기고 KST 변환은 JDBC 설정이 한다 (database.md §2), 날짜는 LocalDate 그대로.
 * 쓰기는 부르는 쪽(배치 단계)의 트랜잭션에 합류한다.
 */
class ReconRepository(private val jdbc: NamedParameterJdbcTemplate) {

    fun insertLines(lines: List<BankStatementLine>, loadedAt: Instant) {
        if (lines.isEmpty()) return
        val at = Timestamp.from(loadedAt)
        jdbc.batchUpdate(
            "INSERT INTO bank_statement_line (bank_date, line_no, reference, bank_code, account_no, amount, processed_at, loaded_at) " +
                "VALUES (:bankDate, :lineNo, :reference, :bankCode, :accountNo, :amount, :processedAt, :loadedAt) " +
                // 같은 파일을 다시 넣으면(② 재시작) 그대로 둔다 — 은행이 한 번 보낸 줄은 기록이라 덮어쓰지 않는다
                "ON DUPLICATE KEY UPDATE line_no = line_no",
            lines.map { line ->
                MapSqlParameterSource()
                    .addValue("bankDate", line.bankDate)
                    .addValue("lineNo", line.lineNo)
                    .addValue("reference", line.reference)
                    .addValue("bankCode", line.bankCode)
                    .addValue("accountNo", line.accountNo)
                    .addValue("amount", line.amount)
                    .addValue("processedAt", Timestamp.from(line.processedAt))
                    .addValue("loadedAt", at)
            }.toTypedArray(),
        )
    }

    /** 이 거래번호의 **적재된 모든 날짜의** 줄 (날짜 · 줄 순). 이중 지급(⑪)과 늦게 처리된 건을 보려면 날짜를 가리지 않는다. */
    fun linesOf(reference: String): List<BankStatementLine> =
        jdbc.query(
            "SELECT bank_date, line_no, reference, bank_code, account_no, amount, processed_at FROM bank_statement_line " +
                "WHERE reference = :reference ORDER BY bank_date, line_no",
            MapSqlParameterSource("reference", reference),
            RowMapper { rs, _ ->
                BankStatementLine(
                    bankDate = rs.getObject("bank_date", LocalDate::class.java),
                    lineNo = rs.getInt("line_no"),
                    reference = rs.getString("reference"),
                    bankCode = rs.getString("bank_code"),
                    accountNo = rs.getString("account_no"),
                    amount = rs.getLong("amount"),
                    processedAt = rs.getTimestamp("processed_at").toInstant(),
                )
            },
        )

    /** 파일 [bankDate] 의 줄 수 — 출금(T) · 1원인증(V) 을 접두로 가른다. */
    fun countLines(bankDate: LocalDate, referencePrefix: String): Int =
        jdbc.queryForObject(
            "SELECT COUNT(*) FROM bank_statement_line WHERE bank_date = :bankDate AND reference LIKE :pattern",
            MapSqlParameterSource().addValue("bankDate", bankDate).addValue("pattern", "$referencePrefix%"),
            Int::class.java,
        )!!

    fun insertIssue(issue: ReconIssue, at: Instant) {
        jdbc.update(
            "INSERT INTO recon_issue (recon_date, type, reference, transfer_id, user_id, our_value, bank_value, created_at) " +
                "VALUES (:reconDate, :type, :reference, :transferId, :userId, :ourValue, :bankValue, :createdAt) " +
                // 같은 날 · 같은 종류 · 같은 키는 한 번만 (uk_recon_issue) — 재시작 · 재실행해도 늘지 않는다
                "ON DUPLICATE KEY UPDATE id = id",
            MapSqlParameterSource()
                .addValue("reconDate", issue.reconDate)
                .addValue("type", issue.type.name)
                .addValue("reference", issue.reference, Types.CHAR)
                .addValue("transferId", issue.transferId?.toString(), Types.CHAR)
                .addValue("userId", issue.userId, Types.VARCHAR)
                .addValue("ourValue", issue.ourValue?.take(VALUE_MAX), Types.VARCHAR)
                .addValue("bankValue", issue.bankValue?.take(VALUE_MAX), Types.VARCHAR)
                .addValue("createdAt", Timestamp.from(at)),
        )
    }

    fun issueCounts(reconDate: LocalDate): Map<ReconIssueType, Int> =
        jdbc.query(
            "SELECT type, COUNT(*) AS n FROM recon_issue WHERE recon_date = :reconDate GROUP BY type",
            MapSqlParameterSource("reconDate", reconDate),
            RowMapper { rs, _ -> ReconIssueType.valueOf(rs.getString("type")) to rs.getInt("n") },
        ).toMap()

    fun issuesOf(reconDate: LocalDate): List<ReconIssueRow> =
        jdbc.query(
            "SELECT id, type, reference, our_value, bank_value FROM recon_issue WHERE recon_date = :reconDate ORDER BY id",
            MapSqlParameterSource("reconDate", reconDate),
            RowMapper { rs, _ ->
                ReconIssueRow(rs.getLong("id"), ReconIssueType.valueOf(rs.getString("type")), rs.getString("reference"), rs.getString("our_value"), rs.getString("bank_value"))
            },
        )

    /** 잔고 ≠ 원장 합인 지갑 (database.md §9 "잔고 = 원장 합 검증" 과 같은 식). 전체를 훑는다 — 하루 한 번이라 감수한다. */
    fun ledgerMismatches(): List<LedgerMismatch> =
        jdbc.query(
            "SELECT w.user_id, w.balance, COALESCE(SUM(CASE l.type WHEN 'WITHDRAW' THEN -l.amount ELSE l.amount END), 0) AS ledger_sum " +
                "FROM wallet w LEFT JOIN wallet_ledger l ON l.user_id = w.user_id " +
                "GROUP BY w.user_id, w.balance HAVING w.balance <> ledger_sum",
            MapSqlParameterSource(),
            RowMapper { rs, _ -> LedgerMismatch(rs.getString("user_id"), rs.getLong("balance"), rs.getLong("ledger_sum")) },
        )

    private companion object {
        /** our_value · bank_value 열 길이. */
        const val VALUE_MAX = 200
    }
}
```
`ReconRepository` 빈은 Task 10 의 `DailyReconJobConfiguration` 이 만든다. 이 Task 에서 테스트가 주입받을 수 있게 지금 최소 설정을 만든다 — `apps/api/src/main/kotlin/io/github/hyeokjinon/banktransfer/api/dailyrecon/DailyReconJobConfiguration.kt`:
```kotlin
package io.github.hyeokjinon.banktransfer.api.dailyrecon

import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate

/** 일마감 대사 조립 (P5-1). 잡 · 단계는 Task 10, 실행기 · 스케줄은 Task 11 에서 더한다. */
@Configuration(proxyBeanMethods = false)
class DailyReconJobConfiguration {

    @Bean
    fun reconRepository(jdbc: NamedParameterJdbcTemplate) = ReconRepository(jdbc)
}
```

- [ ] **Step 4: 통과 확인**

Run: `./gradlew :apps:api:test --tests '*DatabaseSetupTest*' --tests '*ReconRepositoryTest*'`
Expected: PASS (Flyway 가 V4 를 적용하고 Boot 가 JDBC 잡 저장소로 뜬다. `spring.batch.job.enabled=false` 라 기동 때 아무 잡도 돌지 않는다)

Run: `./gradlew :apps:api:test`
Expected: PASS (기존 통합 테스트 전부)

- [ ] **Step 5: 커밋**

```bash
git add apps/api
git commit -m "feat(api): Flyway V4 — Spring Batch 6 메타 표 · bank_statement_line · recon_issue, JDBC 잡 저장소, 대사 SQL

Boot 4 의 starter-batch 만으로는 메모리 잡 저장소라 '끝난 날짜 거부 · 실패하면 이어서 · 하나만 실행' 이 프로세스를 넘지 못한다 →
starter-batch-jdbc. 메타 표는 jar 의 schema-mysql.sql 을 그대로 Flyway 로 만든다. 줄 · 이슈는 append-only, 다시 넣으면 무시해
잡이 어디서 다시 시작해도 결과가 같다. 이슈 키는 거래번호, 지갑이면 user_id (생성 컬럼).

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 10: 대사 잡 `dailyReconciliationJob` — ① 파일 검증 ② 적재(500) ③ 출금 대조(100) ④ 은행 줄 대조(100) ⑤ 잔고 · 원장 ⑥ 보고

**Files:**
- Create: `apps/api/src/main/kotlin/io/github/hyeokjinon/banktransfer/api/dailyrecon/DailyRecon.kt`
- Create: `apps/api/src/main/kotlin/io/github/hyeokjinon/banktransfer/api/dailyrecon/DailyReconSteps.kt`
- Create: `apps/api/src/main/kotlin/io/github/hyeokjinon/banktransfer/api/dailyrecon/ReconSummary.kt`
- Modify: `apps/api/src/main/kotlin/io/github/hyeokjinon/banktransfer/api/dailyrecon/DailyReconJobConfiguration.kt` (Task 9 의 최소 설정을 전체 교체)
- Modify: `apps/api/src/main/resources/application.yml`
- Modify (test): `apps/api/src/test/kotlin/io/github/hyeokjinon/banktransfer/api/support/ApiIntegrationTest.kt`
- Test: `apps/api/src/test/kotlin/io/github/hyeokjinon/banktransfer/api/dailyrecon/{DailyReconTestSupport, DailyReconJobTest, StatementSettlerIsolationTest}.kt` (신규)

**Interfaces:**
- Consumes: Task 1 `ReconFileFormat.read` · `ReconLine`, Task 6 `Alert` · `AlertLevel` · `AlertNotifier` · `notifySafely`, Task 7 `RecordingAlertNotifier` · `recordedAlerts`, Task 8 `StatementSettler` · `Settlement` · `ReconVerdict` · `ReconIssueType` · `BankStatementLine`, Task 9 `ReconRepository` · `ReconIssue` · `ReconIssueRow` · `ReconTestDates`
- Produces:
  - `object DailyRecon { JOB_NAME = "dailyReconciliationJob"; RECON_DATE = "reconDate"; VALIDATE_FILE · LOAD_STATEMENT · MATCH_TRANSFERS · MATCH_BANK_LINES · CHECK_LEDGER · REPORT (단계 이름); LOAD_CHUNK_SIZE = 500; FILE_NOT_READY; FILE_INVALID; KST; fun parameters(date: LocalDate): JobParameters; fun dayRange(date: LocalDate): Pair<Instant, Instant> }`
  - `@ConfigurationProperties("daily-recon") data class DailyReconProperties(fileDir: String = "./bank-files", chunkSize: Int = 100, schedule: Schedule = Schedule()) { data class Schedule(enabled: Boolean = true, cron: String = "0 0 1-6 * * *", zone: String = "Asia/Seoul"); fun reconFile(date: LocalDate): Path }`
  - `sealed class ReconFileException(val code: String, message: String)`, `ReconFileNotReadyException(date)`, `ReconFileInvalidException(date, reason)`
  - `data class BankLineRef(reference: String, transferId: UUID?)`, `class VerdictCounts`, `class SettlingWriter<T : Any>(…) : ItemStreamWriter<T>`, `fun reconIssueOf(reconDate: LocalDate, type: ReconIssueType, settlement: Settlement): ReconIssue`
  - `data class ReconSummary(reconDate, transfers, bankLines, verificationLines, matched, retrying, autoCompleted, autoFailed, mismatches: Map<ReconIssueType, Int>) { val mismatchCount: Int }`
  - `class ReconSummaryReader(jobRepository: JobRepository, repo: ReconRepository) { fun read(instance: JobInstance, date: LocalDate): ReconSummary; fun completionAlert(summary: ReconSummary): Alert }`
  - 빈 `dailyReconciliationJob: Job` (단계 6개), 스텝 범위 빈 `statementFileReader` · `transferIdReader` · `transferSettlingWriter` · `bankLineReader` · `bankLineSettlingWriter`
  - 테스트: `abstract class DailyReconTestSupport : ApiIntegrationTest` — `seed(user, status, sentAt, amount, id)`, `lineOf(transfer, amount)`, `writeBankFile(date, details)`, `runJob(date)`, `status(transfer)`, `issueRefs(date)`, `at(date, hour)`

결정(계획): ③ ④ READ COMMITTED, ④ 거래번호마다 첫 줄 하나, 판정 건수는 단계 ExecutionContext · 자동 확정 · 불일치 건수는 recon_issue, 알림 목록은 불일치만(긴급 먼저), 헤더 은행코드는 검증하지 않음 (위 "결정(계획) 모음").

- [ ] **Step 1: 실패하는 테스트**

`ApiIntegrationTest.kt`:
1) import 추가: `java.nio.file.Files`, `java.nio.file.Path`.
2) `companion object` 안 `val bank …` 아래에 추가:
```kotlin

        /** 대사 파일 디렉터리 (P5-1). 테스트가 RECON_yyyyMMdd.dat 를 직접 써 넣는다 — 저장소의 ./bank-files 를 건드리지 않게 임시 디렉터리. */
        val bankFiles: Path = Files.createTempDirectory("bank-files-")
```
3) `bankProperties` 에 줄 추가:
```kotlin
            registry.add("daily-recon.file-dir") { bankFiles.toString() }
```

`dailyrecon/DailyReconTestSupport.kt`:
```kotlin
package io.github.hyeokjinon.banktransfer.api.dailyrecon

import io.github.hyeokjinon.banktransfer.api.support.ApiIntegrationTest
import io.github.hyeokjinon.banktransfer.protocol.ReconDetail
import io.github.hyeokjinon.banktransfer.protocol.ReconFileFormat
import io.github.hyeokjinon.banktransfer.transfer.RandomReferenceGenerator
import io.github.hyeokjinon.banktransfer.transfer.Transfer
import io.github.hyeokjinon.banktransfer.transfer.TransferEvent
import io.github.hyeokjinon.banktransfer.transfer.TransferStatus
import io.github.hyeokjinon.banktransfer.transfer.TransferStore
import org.springframework.batch.core.job.Job
import org.springframework.batch.core.job.JobExecution
import org.springframework.batch.core.launch.JobOperator
import org.springframework.batch.core.repository.JobRepository
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.jdbc.core.JdbcTemplate
import java.nio.file.Files
import java.time.Instant
import java.time.LocalDate
import java.time.LocalTime
import java.util.UUID

/**
 * 대사 테스트 공용 (P5-1, 공유 DB). 테스트마다 [ReconTestDates.unique] 로 다른 대사일을 쓰고, 그날 보낸 출금을 심고, 은행 파일을 쓴다.
 * 심은 출금은 아웃박스를 발행된 것으로 표시한다 — 다른 테스트의 발행기가 집어 가 은행으로 보내지 않게.
 */
abstract class DailyReconTestSupport : ApiIntegrationTest() {

    @Autowired protected lateinit var store: TransferStore
    @Autowired protected lateinit var jdbc: JdbcTemplate
    @Autowired protected lateinit var props: DailyReconProperties
    @Autowired protected lateinit var jobOperator: JobOperator
    @Autowired protected lateinit var job: Job
    @Autowired protected lateinit var jobRepository: JobRepository

    private val references = RandomReferenceGenerator()

    /** 대사일 [date] 의 [hour] 시 (KST). */
    protected fun at(date: LocalDate, hour: Int = 10): Instant = date.atTime(hour, 0).atZone(DailyRecon.KST).toInstant()

    /**
     * [status] 인 출금 하나를 [sentAt] 에 은행에 보낸 것으로 심는다 (접수 차감 · WITHDRAW 포함, FAILED 는 환불까지).
     * RECEIVED 는 "보내려다 못 보냄"(sending_at 있음). 계좌는 모의 은행에 없는 번호 — 혹시 처리돼도 공용 계좌에 입금이 생기지 않는다.
     */
    protected fun seed(user: String, status: TransferStatus, sentAt: Instant, amount: Long = 10_000, id: UUID = UUID.randomUUID()): Transfer {
        val transfer = Transfer(
            id = id, userId = user, idempotencyKey = UUID.randomUUID().toString(), reference = references.next(), requestHash = "h".repeat(64),
            bankCode = "090", accountNo = unregisteredAccount(), amount = amount, memo = "대사",
            status = TransferStatus.RECEIVED, createdAt = sentAt, updatedAt = sentAt,
        )
        store.saveWithEvent(transfer, TransferEvent.TransferRequested(transfer.id, sentAt))
        jdbc.update("UPDATE outbox_event SET published_at = NOW(6) WHERE aggregate_id = ?", transfer.id.toString())
        store.transition(transfer.id, TransferStatus.RECEIVED, TransferStatus.SENDING, sentAt)
        when (status) {
            TransferStatus.RECEIVED -> store.transition(transfer.id, TransferStatus.SENDING, TransferStatus.RECEIVED, sentAt)
            TransferStatus.SENDING -> Unit
            TransferStatus.UNKNOWN -> store.transition(transfer.id, TransferStatus.SENDING, TransferStatus.UNKNOWN, sentAt)
            TransferStatus.COMPLETED -> store.transition(transfer.id, TransferStatus.SENDING, TransferStatus.COMPLETED, sentAt, "0000")
            TransferStatus.FAILED -> store.fail(transfer.id, TransferStatus.SENDING, sentAt, "1001")
        }
        return store.findById(transfer.id)!!
    }

    /** 은행이 이 출금을 처리했다는 상세 한 줄 (금액만 바꿔 불일치를 만든다). */
    protected fun lineOf(transfer: Transfer, amount: Long = transfer.amount) =
        ReconDetail(transfer.reference, transfer.bankCode, transfer.accountNo, amount, LocalTime.of(10, 0, 1))

    protected fun writeBankFile(date: LocalDate, details: List<ReconDetail>) {
        val file = props.reconFile(date)
        Files.createDirectories(file.parent)
        Files.write(file, ReconFileFormat.write(date, "090", details))
    }

    protected fun runJob(date: LocalDate): JobExecution = jobOperator.start(job, DailyRecon.parameters(date))

    protected fun status(transfer: Transfer): TransferStatus = store.findById(transfer.id)!!.status

    /** 이슈 종류 → 거래번호 목록 (지갑 이슈는 null). */
    protected fun issueRefs(date: LocalDate): Map<String, List<String?>> =
        jdbc.queryForList("SELECT type, reference FROM recon_issue WHERE recon_date = ? ORDER BY id", date)
            .groupBy({ it["type"] as String }, { it["reference"] as String? })
}
```

`dailyrecon/DailyReconJobTest.kt`:
```kotlin
package io.github.hyeokjinon.banktransfer.api.dailyrecon

import io.github.hyeokjinon.banktransfer.api.alert.AlertLevel
import io.github.hyeokjinon.banktransfer.api.alert.AlertText
import io.github.hyeokjinon.banktransfer.protocol.ReconDetail
import io.github.hyeokjinon.banktransfer.transfer.ReconIssueType
import io.github.hyeokjinon.banktransfer.transfer.TransferStatus.COMPLETED
import io.github.hyeokjinon.banktransfer.transfer.TransferStatus.FAILED
import io.github.hyeokjinon.banktransfer.transfer.TransferStatus.RECEIVED
import io.github.hyeokjinon.banktransfer.transfer.TransferStatus.SENDING
import io.github.hyeokjinon.banktransfer.transfer.TransferStatus.UNKNOWN
import org.springframework.batch.core.BatchStatus
import org.springframework.beans.factory.annotation.Autowired
import java.time.Instant
import java.time.LocalTime
import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/** 일마감 대사 잡 전 칸 (P5-1, 설계 §6 · §10). 실제 Spring Batch · Testcontainers MySQL. */
class DailyReconJobTest : DailyReconTestSupport() {

    @Autowired private lateinit var repo: ReconRepository
    @Autowired private lateinit var summaries: ReconSummaryReader

    @Test
    fun `전 칸 - 설계 §6 의 각 경우를 심은 출금과 은행 파일로 잡을 돌리면 미확정만 확정하고 나머지는 기록 · 알린다`() {
        val day = ReconTestDates.unique()
        val user = fundedUser(1_000_000)
        val sent = at(day)
        val completed = seed(user, COMPLETED, sent)                    // 1 일치
        val failed = seed(user, FAILED, sent)                          // 2 일치
        val unknownPaid = seed(user, UNKNOWN, sent)                    // 3 자동 완료
        val sendingPaid = seed(user, SENDING, sent)                    // 3 자동 완료 (SENDING)
        val unknownNotPaid = seed(user, UNKNOWN, sent)                 // 4 자동 실패 + 환불
        val wrongAmount = seed(user, UNKNOWN, sent)                    // 5 금액이 다름 → 확정하지 않는다
        val completedMissing = seed(user, COMPLETED, sent)             // 6
        val refundedPaid = seed(user, FAILED, sent)                    // 7
        val receivedPaid = seed(user, RECEIVED, sent)                  // 8 (긴급)
        val retrying = seed(user, RECEIVED, sent)                      // — 넘어감
        val paidTwice = seed(user, COMPLETED, sent)                    // 11 (긴급)
        val lateUnknown = seed(user, UNKNOWN, at(day.minusDays(1)))    // ④ 전날 보낸 출금이 오늘 파일에 → 같은 판정(자동 완료)
        val strangerRef = "T" + UUID.randomUUID().toString().replace("-", "").take(19).uppercase()   // 9 우리에게 없는 거래번호
        val brokenWallet = fundedUser(1_000).also { jdbc.update("UPDATE wallet SET balance = balance + 1 WHERE user_id = ?", it) }   // 10
        writeBankFile(
            day,
            listOf(
                lineOf(completed), lineOf(unknownPaid), lineOf(sendingPaid), lineOf(wrongAmount, amount = wrongAmount.amount + 1),
                lineOf(refundedPaid), lineOf(receivedPaid), lineOf(paidTwice), lineOf(paidTwice), lineOf(lateUnknown),
                ReconDetail(strangerRef, "090", "1002123456789", 5_000, LocalTime.of(11, 0)),
                ReconDetail("V" + strangerRef.drop(1), "090", "1002123456789", 1, LocalTime.of(11, 1)),   // 1원인증 — 대조하지 않고 건수만
            ),
        )
        val balanceBefore = wallets.balanceOf(user)

        val execution = runJob(day)

        assertEquals(BatchStatus.COMPLETED, execution.status)
        // 결과를 모르던 것만 확정했다
        assertEquals(COMPLETED, status(unknownPaid))
        assertEquals("0000", store.findById(unknownPaid.id)!!.bankResponseCode)
        assertEquals(COMPLETED, status(sendingPaid))
        assertEquals(COMPLETED, status(lateUnknown))
        val autoFailed = store.findById(unknownNotPaid.id)!!
        assertEquals(FAILED to "E002", autoFailed.status to autoFailed.bankResponseCode)
        assertEquals(listOf("WITHDRAW", "REFUND"), ledgerTypes(unknownNotPaid.id))
        assertEquals(balanceBefore + unknownNotPaid.amount, wallets.balanceOf(user))
        assertEquals(ledgerSum(user), wallets.balanceOf(user))
        // 확정된 건 · 불일치 · 재시도 중은 건드리지 않았다
        assertEquals(UNKNOWN, status(wrongAmount))
        assertEquals(COMPLETED, status(completedMissing))
        assertEquals(FAILED, status(refundedPaid))
        assertEquals(RECEIVED, status(receivedPaid))
        assertEquals(RECEIVED, status(retrying))
        // 이슈 — 종류마다 정확히 그 건
        val issues = issueRefs(day)
        assertEquals(setOf(unknownPaid.reference, sendingPaid.reference, lateUnknown.reference), issues.getValue("AUTO_COMPLETED").toSet())
        assertEquals(listOf(unknownNotPaid.reference), issues["AUTO_FAILED"])
        assertEquals(listOf(wrongAmount.reference), issues["AMOUNT_OR_ACCOUNT_MISMATCH"])
        assertEquals(listOf(completedMissing.reference), issues["COMPLETED_NOT_AT_BANK"])
        assertEquals(listOf(refundedPaid.reference), issues["REFUNDED_BUT_PAID"])
        assertEquals(listOf(receivedPaid.reference), issues["PAID_BUT_RECEIVED"])
        assertEquals(listOf(strangerRef), issues["UNKNOWN_REFERENCE"])
        assertEquals(listOf(paidTwice.reference), issues["PAID_TWICE"])
        assertEquals(
            1,
            jdbc.queryForObject(
                "SELECT COUNT(*) FROM recon_issue WHERE recon_date = ? AND type = 'BALANCE_LEDGER_MISMATCH' AND user_id = ?",
                Int::class.java, day, brokenWallet,
            ),
        )
        assertFalse(issues.values.flatten().any { it in setOf(completed.reference, failed.reference, retrying.reference) })
        // 보고 (⑥): 요약 + 불일치 목록, 긴급 먼저, 사용자 id · 계좌 원문 없음
        val alert = recordedAlerts.about(day).single()
        assertEquals(AlertLevel.CRITICAL, alert.level)
        assertEquals("출금 11건 (D일 송신): 일치 2 · 자동 완료 3 · 자동 실패(E002, 환불) 1 · 재시도 중 1", alert.lines[0])
        assertEquals("은행 줄 10건 (1원인증 1건은 대조하지 않음)", alert.lines[1])
        assertTrue(alert.items.first().startsWith("[긴급]"))
        val text = AlertText.render(alert)
        assertFalse(user in text)
        assertFalse(brokenWallet in text)
        assertFalse("1002123456789" in text)
    }

    @Test
    fun `완료 알림 - 불일치가 없으면 INFO, 있으면 CRITICAL 이고 긴급이 먼저`() {
        val clean = ReconTestDates.unique()
        val summary = ReconSummary(clean, 3, 3, 0, 3, 0, 0, 0, emptyMap())

        val info = summaries.completionAlert(summary)

        assertEquals(AlertLevel.INFO, info.level)
        assertTrue("완료 — 불일치 없음" in info.title)
        assertTrue(info.items.isEmpty())

        val dirty = ReconTestDates.unique()
        repo.insertIssue(ReconIssue(dirty, ReconIssueType.COMPLETED_NOT_AT_BANK, reference = "T261028NOTATBANK0001"), Instant.now())
        repo.insertIssue(ReconIssue(dirty, ReconIssueType.PAID_TWICE, reference = "T261028PAIDTWICE0001"), Instant.now())
        repo.insertIssue(ReconIssue(dirty, ReconIssueType.AUTO_COMPLETED, reference = "T261028AUTOCOMPL0001"), Instant.now())

        val critical = summaries.completionAlert(summary.copy(reconDate = dirty, mismatches = repo.issueCounts(dirty).filterKeys { it.mismatch }))

        assertEquals(AlertLevel.CRITICAL, critical.level)
        assertTrue("불일치 2건 (긴급 1)" in critical.title)
        assertEquals(2, critical.items.size)                       // 자동 확정은 목록에 없다 (요약 건수로만)
        assertTrue(critical.items[0].startsWith("[긴급] PAID_TWICE T261028PAIDTWICE0001"))
    }
}
```

`dailyrecon/StatementSettlerIsolationTest.kt`:
```kotlin
package io.github.hyeokjinon.banktransfer.api.dailyrecon

import io.github.hyeokjinon.banktransfer.transfer.BankStatementLine
import io.github.hyeokjinon.banktransfer.transfer.ReconVerdict
import io.github.hyeokjinon.banktransfer.transfer.Settlement
import io.github.hyeokjinon.banktransfer.transfer.StatementSettler
import io.github.hyeokjinon.banktransfer.transfer.TransferStatus
import io.github.hyeokjinon.banktransfer.transfer.TransferStore
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.transaction.PlatformTransactionManager
import org.springframework.transaction.TransactionDefinition
import org.springframework.transaction.support.TransactionTemplate
import java.time.Clock
import java.time.Instant
import java.util.UUID
import java.util.concurrent.CompletableFuture
import java.util.concurrent.TimeUnit
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

/**
 * 경합 (P5-1, 설계 §10): 판정 직전에 다른 쪽(정리 작업)이 상태를 바꾸면 다시 읽어 바뀐 상태로 판정한다 — 실제 MySQL · JPA 저장소로.
 * ③ ④ 청크 트랜잭션을 READ COMMITTED 로 두는 이유도 같이 보인다: REPEATABLE READ 면 청크 첫 읽기의 스냅숏 때문에 바뀐 상태를 못 본다.
 */
class StatementSettlerIsolationTest : DailyReconTestSupport() {

    @Autowired private lateinit var transactionManager: PlatformTransactionManager

    /** 첫 확정 시도 직전에 다른 스레드 · 트랜잭션이 이 출금을 COMPLETED 로 확정하고 커밋한다 (결과조회 확정 흉내). */
    private fun racingStore(): TransferStore = object : TransferStore by store {
        private var raced = false

        override fun transition(id: UUID, from: TransferStatus, to: TransferStatus, at: Instant, bankResponseCode: String?): Boolean {
            if (!raced) {
                raced = true
                CompletableFuture.runAsync { store.transition(id, TransferStatus.UNKNOWN, TransferStatus.COMPLETED, Instant.now(), "0000") }
                    .get(10, TimeUnit.SECONDS)
            }
            return store.transition(id, from, to, at, bankResponseCode)
        }
    }

    /** 청크 트랜잭션 흉내: [isolation] 으로 열고, 첫 읽기로 스냅숏을 만든 뒤 판정한다. */
    private fun settleInChunk(isolation: Int, transferId: UUID, line: BankStatementLine): Settlement {
        val settler = StatementSettler(racingStore(), Clock.systemUTC())
        return TransactionTemplate(transactionManager).apply { isolationLevel = isolation }.execute {
            store.findById(transferId)
            settler.settle(transferId) { listOf(line) }
        }!!
    }

    private fun unknownWithLine(): Pair<UUID, BankStatementLine> {
        val day = ReconTestDates.unique()
        val transfer = seed(fundedUser(), TransferStatus.UNKNOWN, at(day))
        return transfer.id to BankStatementLine(day, 2, transfer.reference, transfer.bankCode, transfer.accountNo, transfer.amount, at(day))
    }

    @Test
    fun `READ COMMITTED - 확정이 0건이면 다시 읽어 바뀐 상태(COMPLETED)로 판정한다 = 일치, 두 번 확정하지 않는다`() {
        val (id, line) = unknownWithLine()

        val settlement = settleInChunk(TransactionDefinition.ISOLATION_READ_COMMITTED, id, line)

        assertEquals(ReconVerdict.MATCHED, settlement.verdict)
        assertEquals(TransferStatus.COMPLETED, settlement.transfer!!.status)
        assertEquals(TransferStatus.COMPLETED, store.findById(id)!!.status)
    }

    @Test
    fun `REPEATABLE READ 였다면 스냅숏 때문에 바뀐 상태를 못 보고 3번 뒤 포기한다 - ③ ④ 를 READ COMMITTED 로 두는 이유`() {
        val (id, line) = unknownWithLine()

        assertFailsWith<IllegalStateException> { settleInChunk(TransactionDefinition.ISOLATION_REPEATABLE_READ, id, line) }
        assertEquals(TransferStatus.COMPLETED, store.findById(id)!!.status)   // 다른 쪽의 확정은 그대로다
    }
}
```

- [ ] **Step 2: 실패 확인**

Run: `./gradlew :apps:api:test --tests '*DailyReconJobTest*' --tests '*StatementSettlerIsolationTest*'`
Expected: FAIL (컴파일: `Unresolved reference 'DailyReconProperties'`, `'DailyRecon'`, `'ReconSummaryReader'`, `'ReconSummary'`)

- [ ] **Step 3: 구현**

`dailyrecon/DailyRecon.kt`:
```kotlin
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
}

/** 일마감 대사 설정 (P5-1, 설계 §9). */
@ConfigurationProperties("daily-recon")
data class DailyReconProperties(
    /** 은행 파일 디렉터리 (`recon/RECON_yyyyMMdd.dat`). 실제로는 은행 SFTP 에서 받아 오는 자리. 상대 경로는 작업 디렉터리(저장소 루트) 기준. */
    val fileDir: String = "./bank-files",
    /** ③ · ④ 청크 크기. ② 적재는 500 고정. */
    val chunkSize: Int = 100,
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
```

`dailyrecon/DailyReconSteps.kt`:
```kotlin
package io.github.hyeokjinon.banktransfer.api.dailyrecon

import io.github.hyeokjinon.banktransfer.api.alert.AlertNotifier
import io.github.hyeokjinon.banktransfer.api.alert.notifySafely
import io.github.hyeokjinon.banktransfer.api.dailyrecon.DailyRecon.FILE_INVALID
import io.github.hyeokjinon.banktransfer.api.dailyrecon.DailyRecon.FILE_NOT_READY
import io.github.hyeokjinon.banktransfer.api.dailyrecon.DailyRecon.KST
import io.github.hyeokjinon.banktransfer.api.dailyrecon.DailyRecon.RECON_DATE
import io.github.hyeokjinon.banktransfer.protocol.ProtocolException
import io.github.hyeokjinon.banktransfer.protocol.ReconFileFormat
import io.github.hyeokjinon.banktransfer.protocol.ReconLine
import io.github.hyeokjinon.banktransfer.transfer.BankStatementLine
import io.github.hyeokjinon.banktransfer.transfer.ReconIssueType
import io.github.hyeokjinon.banktransfer.transfer.ReconVerdict
import io.github.hyeokjinon.banktransfer.transfer.Settlement
import org.slf4j.LoggerFactory
import org.springframework.batch.core.scope.context.ChunkContext
import org.springframework.batch.core.step.StepContribution
import org.springframework.batch.core.step.tasklet.Tasklet
import org.springframework.batch.infrastructure.item.Chunk
import org.springframework.batch.infrastructure.item.ExecutionContext
import org.springframework.batch.infrastructure.item.ItemStreamWriter
import org.springframework.batch.infrastructure.item.support.AbstractItemCountingItemStreamItemReader
import org.springframework.batch.infrastructure.repeat.RepeatStatus
import java.nio.file.Files
import java.nio.file.Path
import java.time.Clock
import java.time.LocalDate
import java.util.EnumMap
import java.util.UUID

/** ① 이 판정한 대사 파일 문제 (P5-1). [code] 가 실패 이유로 그대로 나간다. */
sealed class ReconFileException(val code: String, message: String) : RuntimeException(message)

/** 파일이 아직 없다 — 은행 파일은 보통 00:05 에 온다. 다음 정각에 다시 한다. */
class ReconFileNotReadyException(date: LocalDate) : ReconFileException(FILE_NOT_READY, "recon file for $date is not there yet")

/** 파일이 있지만 믿을 수 없다 (날짜 · 건수 · 합계 · 길이). 사람이 봐야 한다. */
class ReconFileInvalidException(date: LocalDate, reason: String) : ReconFileException(FILE_INVALID, "recon file for $date is invalid: $reason")

internal fun reconDateOf(chunkContext: ChunkContext): LocalDate =
    checkNotNull(chunkContext.stepContext.stepExecution.jobParameters.getLocalDate(RECON_DATE)) { "job parameter $RECON_DATE is missing" }

/** ① 대사 파일이 있고 형식이 맞는지 (헤더 날짜 = D, 트레일러 건수 · 합계 = 상세). 헤더의 은행코드는 기록만 하고 보지 않는다. */
class ValidateFileTasklet(private val props: DailyReconProperties) : Tasklet {

    override fun execute(contribution: StepContribution, chunkContext: ChunkContext): RepeatStatus {
        val date = reconDateOf(chunkContext)
        val file = props.reconFile(date)
        if (!Files.isRegularFile(file)) throw ReconFileNotReadyException(date)
        try {
            ReconFileFormat.read(Files.readAllBytes(file), date)
        } catch (e: ProtocolException) {
            throw ReconFileInvalidException(date, e.message ?: "unreadable")
        }
        return RepeatStatus.FINISHED
    }
}

/**
 * ② 파일 D 의 상세를 읽는다 (1원인증 V 줄도 적재한다 — 대조하지 않고 건수만 보고). ① 이 검증했지만 열 때 다시 검증한다
 * (그 사이 파일이 바뀌었으면 여기서 실패). 재시작하면 읽은 건수만큼 건너뛴다 (AbstractItemCountingItemStreamItemReader).
 */
class StatementFileReader(private val file: Path, private val reconDate: LocalDate) : AbstractItemCountingItemStreamItemReader<BankStatementLine>() {
    private var lines: List<BankStatementLine> = emptyList()

    init {
        setName("statementFileReader")
    }

    override fun doOpen() {
        lines = ReconFileFormat.read(Files.readAllBytes(file), reconDate).lines.map { it.toStatementLine(reconDate) }
    }

    override fun doRead(): BankStatementLine? = lines.getOrNull(currentItemCount - 1)

    override fun doClose() {
        lines = emptyList()
    }
}

/** 파일의 처리시각(HHmmss, KST)을 대사일의 Instant 로. */
internal fun ReconLine.toStatementLine(bankDate: LocalDate) = BankStatementLine(
    bankDate = bankDate,
    lineNo = lineNo,
    reference = detail.reference,
    bankCode = detail.bankCode,
    accountNo = detail.accountNo,
    amount = detail.amount,
    processedAt = bankDate.atTime(detail.processedAt).atZone(KST).toInstant(),
)

/** ④ 의 한 건: 파일 D 의 T 줄 중 ③ 이 다루지 않은 거래번호 (거래번호마다 첫 줄 하나)와 그 거래번호의 우리 출금 id (없으면 null). */
data class BankLineRef(val reference: String, val transferId: UUID?)

/** 판정별 건수. 단계의 ExecutionContext 에 `verdict.<판정>` 으로 저장한다 — 청크가 커밋될 때만 저장되므로 재시작해도 두 번 세지 않는다. */
class VerdictCounts {
    private val counts = EnumMap<ReconVerdict, Int>(ReconVerdict::class.java)

    fun add(verdict: ReconVerdict) {
        counts[verdict] = this[verdict] + 1
    }

    operator fun get(verdict: ReconVerdict): Int = counts[verdict] ?: 0

    val total: Int get() = counts.values.sum()

    fun saveTo(context: ExecutionContext) = ReconVerdict.entries.forEach { context.putInt(key(it), this[it]) }

    fun restore(context: ExecutionContext) {
        counts.clear()
        ReconVerdict.entries.forEach { verdict -> context.getInt(key(verdict), 0).takeIf { it > 0 }?.let { counts[verdict] = it } }
    }

    companion object {
        fun key(verdict: ReconVerdict) = "verdict.${verdict.name}"

        fun from(context: ExecutionContext?): VerdictCounts = VerdictCounts().also { counts -> context?.let { counts.restore(it) } }
    }
}

/**
 * ③ · ④ 의 쓰기: 한 건마다 [settle] 로 판정 · (미확정만) 확정하고, 이슈가 있으면 recon_issue 에 남긴다 (같은 것은 무시).
 * 확정 · 환불 · 이슈는 모두 청크 트랜잭션에 합류한다 — 청크가 실패하면 함께 롤백되고 재시작 때 같은 결과로 다시 쓴다 (이중 환불 없음).
 * 판정별 건수는 [ItemStreamWriter.update] 로 청크마다 단계 ExecutionContext 에 저장한다.
 */
class SettlingWriter<T : Any>(
    private val reconDate: LocalDate,
    private val repo: ReconRepository,
    private val clock: Clock,
    private val settle: (T) -> Settlement,
) : ItemStreamWriter<T> {
    private val counts = VerdictCounts()

    override fun open(executionContext: ExecutionContext) = counts.restore(executionContext)

    override fun update(executionContext: ExecutionContext) = counts.saveTo(executionContext)

    override fun write(chunk: Chunk<out T>) {
        for (item in chunk.items) {
            val settlement = settle(item)
            counts.add(settlement.verdict)
            settlement.verdict.issue?.let { repo.insertIssue(reconIssueOf(reconDate, it, settlement), clock.instant()) }
        }
    }
}

/**
 * 판정 하나를 이슈 한 줄로. our_value = 판정 시점의 우리 상태 · 금액 · 계좌, bank_value = 이 거래번호의 은행 줄들(날짜#줄).
 * DB 에는 계좌 원문을 둔다(운영자가 찾는다) — 알림으로 나갈 때 AlertText 가 가린다.
 */
fun reconIssueOf(reconDate: LocalDate, type: ReconIssueType, settlement: Settlement): ReconIssue {
    val transfer = settlement.transfer
    return ReconIssue(
        reconDate = reconDate,
        type = type,
        reference = transfer?.reference ?: settlement.lines.first().reference,
        transferId = transfer?.id,
        userId = transfer?.userId,
        ourValue = transfer?.let { "${it.status} ${it.amount} ${it.bankCode}-${it.accountNo}" },
        bankValue = settlement.lines.takeIf { it.isNotEmpty() }
            ?.joinToString("; ") { "${it.amount} ${it.bankCode}-${it.accountNo} ${it.bankDate}#${it.lineNo}" },
    )
}

/** ⑤ 잔고 ≠ 원장 합인 지갑을 이슈로 (대사일과 무관하게 지금 전체). 고칠 때까지 날마다 다시 나온다. */
class CheckLedgerTasklet(private val repo: ReconRepository, private val clock: Clock) : Tasklet {

    override fun execute(contribution: StepContribution, chunkContext: ChunkContext): RepeatStatus {
        val date = reconDateOf(chunkContext)
        for (mismatch in repo.ledgerMismatches()) {
            repo.insertIssue(
                ReconIssue(date, ReconIssueType.BALANCE_LEDGER_MISMATCH, userId = mismatch.userId, ourValue = "balance=${mismatch.balance}", bankValue = "ledgerSum=${mismatch.ledgerSum}"),
                clock.instant(),
            )
        }
        return RepeatStatus.FINISHED
    }
}

/** ⑥ 단계별 건수 · recon_issue 로 요약을 만들어 알린다. 알림 실패가 대사를 실패시키거나 되돌리지 않는다 — 기록의 원본은 recon_issue. */
class ReportTasklet(private val summaries: ReconSummaryReader, private val alerts: AlertNotifier) : Tasklet {
    private val log = LoggerFactory.getLogger(javaClass)

    override fun execute(contribution: StepContribution, chunkContext: ChunkContext): RepeatStatus {
        val date = reconDateOf(chunkContext)
        val summary = summaries.read(chunkContext.stepContext.stepExecution.jobExecution.jobInstance, date)
        log.info("daily reconciliation {} completed: {}", date, summary)
        alerts.notifySafely(summaries.completionAlert(summary))
        return RepeatStatus.FINISHED
    }
}
```

`dailyrecon/ReconSummary.kt`:
```kotlin
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
    /** 파일 D 의 출금 줄(T). */
    val bankLines: Int,
    /** 파일 D 의 1원인증 줄(V) — 대조하지 않고 건수만. */
    val verificationLines: Int,
    val matched: Int,
    val retrying: Int,
    val autoCompleted: Int,
    val autoFailed: Int,
    /** 사람이 봐야 하는 어긋남, 종류별 (recon_issue 기준 — 재시작해도 늘지 않는다). */
    val mismatches: Map<ReconIssueType, Int>,
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
            bankLines = repo.countLines(date, "T"),
            verificationLines = repo.countLines(date, "V"),
            matched = transfers[ReconVerdict.MATCHED] + bankOnly[ReconVerdict.MATCHED],
            retrying = transfers[ReconVerdict.RETRYING] + bankOnly[ReconVerdict.RETRYING],
            autoCompleted = issues[ReconIssueType.AUTO_COMPLETED] ?: 0,
            autoFailed = issues[ReconIssueType.AUTO_FAILED] ?: 0,
            mismatches = issues.filterKeys { it.mismatch },
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
                "자동 실패(E002, 환불) ${summary.autoFailed} · 재시도 중 ${summary.retrying}",
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
```

`dailyrecon/DailyReconJobConfiguration.kt` 전체를 다음으로 바꾼다:
```kotlin
package io.github.hyeokjinon.banktransfer.api.dailyrecon

import io.github.hyeokjinon.banktransfer.api.alert.AlertNotifier
import io.github.hyeokjinon.banktransfer.api.dailyrecon.DailyRecon.CHECK_LEDGER
import io.github.hyeokjinon.banktransfer.api.dailyrecon.DailyRecon.JOB_NAME
import io.github.hyeokjinon.banktransfer.api.dailyrecon.DailyRecon.LOAD_CHUNK_SIZE
import io.github.hyeokjinon.banktransfer.api.dailyrecon.DailyRecon.LOAD_STATEMENT
import io.github.hyeokjinon.banktransfer.api.dailyrecon.DailyRecon.MATCH_BANK_LINES
import io.github.hyeokjinon.banktransfer.api.dailyrecon.DailyRecon.MATCH_TRANSFERS
import io.github.hyeokjinon.banktransfer.api.dailyrecon.DailyRecon.RECON_DATE
import io.github.hyeokjinon.banktransfer.api.dailyrecon.DailyRecon.REPORT
import io.github.hyeokjinon.banktransfer.api.dailyrecon.DailyRecon.VALIDATE_FILE
import io.github.hyeokjinon.banktransfer.transfer.BankStatementLine
import io.github.hyeokjinon.banktransfer.transfer.StatementSettler
import io.github.hyeokjinon.banktransfer.transfer.TransferStore
import org.springframework.batch.core.configuration.annotation.StepScope
import org.springframework.batch.core.job.Job
import org.springframework.batch.core.job.builder.JobBuilder
import org.springframework.batch.core.job.parameters.DefaultJobParametersValidator
import org.springframework.batch.core.repository.JobRepository
import org.springframework.batch.core.step.builder.StepBuilder
import org.springframework.batch.infrastructure.item.ItemStreamReader
import org.springframework.batch.infrastructure.item.ItemStreamWriter
import org.springframework.batch.infrastructure.item.database.Order
import org.springframework.batch.infrastructure.item.database.builder.JdbcPagingItemReaderBuilder
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import org.springframework.transaction.PlatformTransactionManager
import org.springframework.transaction.TransactionDefinition
import org.springframework.transaction.interceptor.DefaultTransactionAttribute
import java.sql.Timestamp
import java.time.Clock
import java.time.LocalDate
import java.util.UUID
import javax.sql.DataSource

/**
 * 일마감 대사 잡 (P5-1, 설계 §5). API 앱 안의 Spring Batch — 확정 · 환불에 기존 TransferStore 경로를 그대로 쓴다.
 * ① validateFile ② loadStatement(청크 500) ③ matchTransfers(청크 100) ④ matchBankLines(청크 100) ⑤ checkLedger ⑥ report.
 * 잡 저장소(JDBC)가 날짜마다 인스턴스 하나를 두어 끝난 날짜는 거부하고, 실패하면 멈춘 단계 · 마지막 커밋 청크부터 잇는다.
 *
 * Spring Batch 6 주의 (계획의 "버전 · API 확인 결과" 6 · 8):
 * - chunk(size) 의 트랜잭션 관리자 기본값은 ResourcelessTransactionManager(트랜잭션 없음)다 → 모든 단계에 JPA 의 transactionManager 를 넘긴다.
 * - 스텝 범위(@StepScope) 빈은 CGLIB 프록시라 Kotlin(final) 클래스를 그대로 돌려줄 수 없다 → 반환형을 인터페이스(ItemStreamReader/Writer)로 둔다.
 */
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(DailyReconProperties::class)
class DailyReconJobConfiguration {

    @Bean
    fun reconRepository(jdbc: NamedParameterJdbcTemplate) = ReconRepository(jdbc)

    @Bean
    fun statementSettler(store: TransferStore, clock: Clock) = StatementSettler(store, clock)

    @Bean
    fun reconSummaryReader(jobRepository: JobRepository, repo: ReconRepository) = ReconSummaryReader(jobRepository, repo)

    @Bean
    fun dailyReconciliationJob(
        jobRepository: JobRepository,
        transactionManager: PlatformTransactionManager,
        props: DailyReconProperties,
        repo: ReconRepository,
        summaries: ReconSummaryReader,
        alerts: AlertNotifier,
        clock: Clock,
        @Qualifier("statementFileReader") statementFile: ItemStreamReader<BankStatementLine>,
        @Qualifier("transferIdReader") transferIds: ItemStreamReader<UUID>,
        @Qualifier("transferSettlingWriter") transferWriter: ItemStreamWriter<UUID>,
        @Qualifier("bankLineReader") bankLines: ItemStreamReader<BankLineRef>,
        @Qualifier("bankLineSettlingWriter") bankLineWriter: ItemStreamWriter<BankLineRef>,
    ): Job {
        // ③ ④ 는 READ COMMITTED: 조건부 전이가 0건이면(그 사이 정리 작업 등이 바꿈) 다시 읽어 판정하는데, MySQL 기본 REPEATABLE READ 는
        // 청크 첫 읽기(리더)의 스냅숏을 다시 보여 줘 바뀐 상태를 보지 못한다 (발행기와 같은 이유 — MessagingConfiguration)
        val readCommitted = DefaultTransactionAttribute().apply { isolationLevel = TransactionDefinition.ISOLATION_READ_COMMITTED }

        val validateFile = StepBuilder(VALIDATE_FILE, jobRepository).tasklet(ValidateFileTasklet(props), transactionManager).build()
        val loadStatement = StepBuilder(LOAD_STATEMENT, jobRepository).chunk<BankStatementLine, BankStatementLine>(LOAD_CHUNK_SIZE)
            .reader(statementFile)
            .writer { chunk -> repo.insertLines(chunk.items, clock.instant()) }
            .transactionManager(transactionManager)
            .build()
        val matchTransfers = StepBuilder(MATCH_TRANSFERS, jobRepository).chunk<UUID, UUID>(props.chunkSize)
            .reader(transferIds)
            .writer(transferWriter)
            .transactionManager(transactionManager)
            .transactionAttribute(readCommitted)
            .build()
        val matchBankLines = StepBuilder(MATCH_BANK_LINES, jobRepository).chunk<BankLineRef, BankLineRef>(props.chunkSize)
            .reader(bankLines)
            .writer(bankLineWriter)
            .transactionManager(transactionManager)
            .transactionAttribute(readCommitted)
            .build()
        val checkLedger = StepBuilder(CHECK_LEDGER, jobRepository).tasklet(CheckLedgerTasklet(repo, clock), transactionManager).build()
        val report = StepBuilder(REPORT, jobRepository).tasklet(ReportTasklet(summaries, alerts), transactionManager).build()

        return JobBuilder(JOB_NAME, jobRepository)
            .validator(DefaultJobParametersValidator(arrayOf(RECON_DATE), emptyArray()))
            .start(validateFile)
            .next(loadStatement)
            .next(matchTransfers)
            .next(matchBankLines)
            .next(checkLedger)
            .next(report)
            .build()
    }

    /** ② 파일 D 의 상세. */
    @Bean
    @StepScope
    fun statementFileReader(
        props: DailyReconProperties,
        @Value("#{jobParameters['reconDate']}") reconDate: LocalDate,
    ): ItemStreamReader<BankStatementLine> = StatementFileReader(props.reconFile(reconDate), reconDate)

    /** ③ sending_at 이 D일(KST)인 출금 id, id 순 페이징. sending_at = 마지막으로 은행에 보내려 한 시각. */
    @Bean
    @StepScope
    fun transferIdReader(
        dataSource: DataSource,
        props: DailyReconProperties,
        @Value("#{jobParameters['reconDate']}") reconDate: LocalDate,
    ): ItemStreamReader<UUID> {
        val (from, to) = DailyRecon.dayRange(reconDate)
        return JdbcPagingItemReaderBuilder<UUID>()
            .name("transferIdReader")
            .dataSource(dataSource)
            .selectClause("id")
            .fromClause("transfer")
            // 시각은 Timestamp(Instant) 로 넘긴다 — KST 벽시계 변환은 JDBC 설정(connectionTimeZone)이 한다 (database.md §2)
            .whereClause("sending_at >= :from AND sending_at < :to")
            .parameterValues(mapOf<String, Any>("from" to Timestamp.from(from), "to" to Timestamp.from(to)))
            .sortKeys(mapOf("id" to Order.ASCENDING))
            .pageSize(props.chunkSize)
            .rowMapper { rs, _ -> UUID.fromString(rs.getString("id")) }
            .build()
    }

    /** ③ 판정 · 확정. 거래번호로 적재된 모든 날짜의 줄을 찾는다. */
    @Bean
    @StepScope
    fun transferSettlingWriter(
        settler: StatementSettler,
        repo: ReconRepository,
        clock: Clock,
        @Value("#{jobParameters['reconDate']}") reconDate: LocalDate,
    ): ItemStreamWriter<UUID> = SettlingWriter(reconDate, repo, clock) { id: UUID -> settler.settle(id, repo::linesOf) }

    /**
     * ④ 파일 D 의 T 줄 중 ③ 이 다루지 않은 것: 우리에게 없는 거래번호, 다른 날 보낸(또는 보낸 적 없는) 출금.
     * 같은 거래번호가 여러 줄이면(이중 지급) 첫 줄 하나만 읽는다 — 판정은 그 거래번호의 모든 줄로 한 번.
     */
    @Bean
    @StepScope
    fun bankLineReader(
        dataSource: DataSource,
        props: DailyReconProperties,
        @Value("#{jobParameters['reconDate']}") reconDate: LocalDate,
    ): ItemStreamReader<BankLineRef> {
        val (from, to) = DailyRecon.dayRange(reconDate)
        return JdbcPagingItemReaderBuilder<BankLineRef>()
            .name("bankLineReader")
            .dataSource(dataSource)
            .selectClause("l.line_no, l.reference, t.id AS transfer_id")
            .fromClause("bank_statement_line l LEFT JOIN transfer t ON t.reference = l.reference")
            .whereClause(
                "l.bank_date = :reconDate AND l.reference LIKE 'T%' " +
                    "AND (t.id IS NULL OR t.sending_at IS NULL OR t.sending_at < :from OR t.sending_at >= :to) " +
                    "AND l.line_no = (SELECT MIN(f.line_no) FROM bank_statement_line f WHERE f.bank_date = l.bank_date AND f.reference = l.reference)",
            )
            .parameterValues(mapOf<String, Any>("reconDate" to reconDate, "from" to Timestamp.from(from), "to" to Timestamp.from(to)))
            .sortKeys(mapOf("line_no" to Order.ASCENDING))
            .pageSize(props.chunkSize)
            .rowMapper { rs, _ -> BankLineRef(rs.getString("reference"), rs.getString("transfer_id")?.let(UUID::fromString)) }
            .build()
    }

    /** ④ 판정 · 확정: 우리 출금이 있으면 ③ 과 같은 판정, 없으면 UNKNOWN_REFERENCE. */
    @Bean
    @StepScope
    fun bankLineSettlingWriter(
        settler: StatementSettler,
        repo: ReconRepository,
        clock: Clock,
        @Value("#{jobParameters['reconDate']}") reconDate: LocalDate,
    ): ItemStreamWriter<BankLineRef> = SettlingWriter(reconDate, repo, clock) { ref: BankLineRef ->
        val lines = repo.linesOf(ref.reference)
        ref.transferId?.let { id -> settler.settle(id) { lines } } ?: settler.unknownReference(lines)
    }
}
```
정렬 키 `line_no` 는 별칭 없이 쓴다: 다음 페이지 조건이 `line_no > :_line_no` 로 붙는데 바깥 쿼리에서 `line_no` 는 `bank_statement_line l` 에만 있어 모호하지 않다(`transfer` 에는 없다).

`application.yml` 끝(`alert:` 절 위)에 추가:
```yaml
daily-recon:                   # 일마감 대사 (P5-1, ADR-0010)
  file-dir: ./bank-files       # 은행 대사 파일 디렉터리 (recon/RECON_yyyyMMdd.dat). 작업 디렉터리 = 저장소 루트 (git 제외)
  chunk-size: 100              # ③ 출금 대조 · ④ 은행 줄 대조 청크. ② 적재는 500 고정
```

- [ ] **Step 4: 통과 확인**

Run: `./gradlew :apps:api:test --tests '*DailyReconJobTest*' --tests '*StatementSettlerIsolationTest*'`
Expected: PASS (4개)

Run: `./gradlew :apps:api:test`
Expected: PASS (잡 빈이 생겨도 `spring.batch.job.enabled=false` 라 기동 때 돌지 않는다)

- [ ] **Step 5: 커밋**

```bash
git add apps/api
git commit -m "feat(api): 일마감 대사 잡 — ① 파일 검증 ② 적재(500) ③ 출금 대조(100) ④ 은행 줄 대조(100) ⑤ 잔고 · 원장 ⑥ 보고

판정은 도메인(StatementSettler)이 하고 배치는 읽기 · 쓰기 · 연결만 한다. 확정 · 환불 · 이슈가 청크 트랜잭션에 합류해
실패하면 함께 롤백되고 재시작 때 같은 결과를 쓴다. ③ ④ 는 READ COMMITTED — REPEATABLE READ 스냅숏이면 '다시 읽기' 가
바뀐 상태를 못 본다(경합 테스트로 확인). Spring Batch 6 의 chunk(size) 는 트랜잭션 관리자를 명시하지 않으면 트랜잭션이 없다.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 11: 실행 — `DailyReconLauncher`(중복 · 재시작 · 동시 실행 · 실패 알림) · 매시 스케줄 · `POST /admin/reconciliations`

**Files:**
- Create: `apps/api/src/main/kotlin/io/github/hyeokjinon/banktransfer/api/dailyrecon/DailyReconLauncher.kt`
- Create: `apps/api/src/main/kotlin/io/github/hyeokjinon/banktransfer/api/dailyrecon/DailyReconScheduler.kt`
- Create: `apps/api/src/main/kotlin/io/github/hyeokjinon/banktransfer/api/web/ReconciliationController.kt`
- Modify: `apps/api/src/main/kotlin/io/github/hyeokjinon/banktransfer/api/dailyrecon/DailyReconJobConfiguration.kt` (가드 리스너 · 런처 · 스케줄러 빈)
- Modify: `apps/api/src/main/kotlin/io/github/hyeokjinon/banktransfer/api/web/ApiExceptionHandler.kt`
- Modify: `apps/api/src/main/resources/application.yml`, `.run/5. API (부하 측정).run.xml`
- Modify (test): `apps/api/src/test/kotlin/io/github/hyeokjinon/banktransfer/api/support/ApiIntegrationTest.kt`
- Test: `apps/api/src/test/kotlin/io/github/hyeokjinon/banktransfer/api/dailyrecon/{DailyReconLauncherTest, DailyReconSchedulerTest}.kt`, `apps/api/src/test/kotlin/io/github/hyeokjinon/banktransfer/api/web/ReconciliationApiTest.kt` (신규), `ApiExceptionHandlerTest.kt` (테스트 1개 추가)

**Interfaces:**
- Consumes: Task 10 `DailyRecon` · `ReconSummaryReader` · `ReconFileException` · `DailyReconProperties.schedule` · `DailyReconTestSupport`, Task 6 알림, Task 7 `recordedAlerts`
- Produces:
  - `enum class ReconTrigger { SCHEDULE, SCHEDULE_FINAL, MANUAL }`, `data class ReconFailure(code: String, message: String)`
  - `sealed interface ReconRun { val date: LocalDate; Completed(date, summary: ReconSummary); Failed(date, failure: ReconFailure); AlreadyCompleted(date); Running(date) }`
  - `class DailyReconLauncher(jobOperator: JobOperator, job: Job, summaries: ReconSummaryReader, alerts: AlertNotifier) { fun run(date: LocalDate, trigger: ReconTrigger): ReconRun }`
  - `class SingleRunGuard(jobRepository: JobRepository) : JobExecutionListener`
  - `class ReconRunningException(date)`, `ReconAlreadyCompletedException(date)`, `ReconDateInFutureException(date, today)`
  - `class DailyReconScheduler(launch: (LocalDate, ReconTrigger) -> ReconRun, clock: Clock, cron: String, zone: String, alerts: AlertNotifier) { fun run(); internal fun isFinalRound(now: ZonedDateTime): Boolean }`
  - `POST /admin/reconciliations?date=yyyy-MM-dd` → 200 `ReconRunResponse(date, status: "COMPLETED" | "FAILED", reason: ReconFailure?, summary: ReconSummary?)`, 409 `RECON_ALREADY_COMPLETED` · `RECON_RUNNING`, 400 `RECON_DATE_IN_FUTURE`

결정(계획): 실패 이유 코드 · 실패 알림 정책 · 마지막 회차 = cron 다음 실행이 다음 날 · 스케줄 풀 3 · `SingleRunGuard` · `RECON_DATE_IN_FUTURE` (위 "결정(계획) 모음", "스펙과 다른 점" 1).

- [ ] **Step 1: 실패하는 테스트**

`ApiIntegrationTest.kt` 의 `@SpringBootTest(properties = [...])` 목록 끝(`"transfer.retry.max-delay-ms=2000",` 아래)에 추가:
```kotlin
        // 일마감 대사 스케줄은 끈다 (P5-1): 테스트가 실행기를 직접 부른다. 켜 두면 01 ~ 06시에 진짜 시각으로 전날 대사가 돈다
        "daily-recon.schedule.enabled=false",
```

`dailyrecon/DailyReconLauncherTest.kt`:
```kotlin
package io.github.hyeokjinon.banktransfer.api.dailyrecon

import io.github.hyeokjinon.banktransfer.api.alert.AlertLevel
import io.github.hyeokjinon.banktransfer.transfer.TransferStatus
import org.springframework.batch.core.BatchStatus
import org.springframework.batch.infrastructure.item.ExecutionContext
import org.springframework.beans.factory.annotation.Autowired
import java.nio.file.Files
import java.time.LocalDateTime
import java.util.UUID
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
import kotlin.test.assertTrue

/** 실행 결과 · 재실행 거부 · 재시작 · 동시 실행 · 실패 알림 (P5-1, 설계 §5-2 · §10). */
class DailyReconLauncherTest : DailyReconTestSupport() {

    @Autowired private lateinit var launcher: DailyReconLauncher

    private fun executions(day: java.time.LocalDate): Int =
        jobRepository.getJobInstance(DailyRecon.JOB_NAME, DailyRecon.parameters(day))?.let { jobRepository.getJobExecutions(it).size } ?: 0

    @Test
    fun `끝난 날짜를 다시 돌리면 AlreadyCompleted - 환불 · 이슈가 늘지 않는다`() {
        val day = ReconTestDates.unique()
        val user = fundedUser()
        val notPaid = seed(user, TransferStatus.UNKNOWN, at(day))           // 파일에 없음 → 자동 실패 + 환불
        val missing = seed(user, TransferStatus.COMPLETED, at(day))         // 파일에 없음 → COMPLETED_NOT_AT_BANK
        writeBankFile(day, emptyList())

        assertIs<ReconRun.Completed>(launcher.run(day, ReconTrigger.MANUAL))
        val issues = issueRefs(day)
        val balance = wallets.balanceOf(user)

        assertIs<ReconRun.AlreadyCompleted>(launcher.run(day, ReconTrigger.SCHEDULE))

        assertEquals(issues, issueRefs(day))
        assertEquals(listOf("WITHDRAW", "REFUND"), ledgerTypes(notPaid.id))
        assertEquals(balance, wallets.balanceOf(user))
        assertEquals(listOf(missing.reference), issues["COMPLETED_NOT_AT_BANK"])
        assertEquals(1, executions(day))
    }

    @Test
    fun `재시작 - ③ 중간 청크에서 실패하면 잡이 실패하고, 고친 뒤 다시 돌리면 멈춘 곳부터 끝까지 - 결과는 한 번에 끝낸 것과 같다 (이중 환불 없음)`() {
        val day = ReconTestDates.unique()
        val user = fundedUser(10_000_000)
        val broken = fundedUser(1_000)
        // ③ 은 id 순으로 100건씩 읽는다. 151건 중 121번째(두 번째 청크)를 지갑이 없는 사용자의 출금으로 — 환불이 실패한다
        // (지갑 행이 없으면 WalletLedger.refund 의 check 가 던진다: 손으로 고칠 때까지 같은 자리에서 실패하는 실제 장애 모양)
        val ids = List(151) { UUID.randomUUID() }.sortedBy { it.toString() }
        val transfers = ids.mapIndexed { i, id -> seed(if (i == 120) broken else user, TransferStatus.UNKNOWN, at(day), amount = 1_000, id = id) }
        jdbc.update("DELETE FROM wallet WHERE user_id = ?", broken)
        writeBankFile(day, emptyList())                                       // 은행은 아무것도 처리하지 않았다 → 151건 모두 자동 실패 + 환불

        val first = launcher.run(day, ReconTrigger.SCHEDULE)

        assertEquals("ERROR", assertIs<ReconRun.Failed>(first).failure.code)
        assertEquals(List(100) { TransferStatus.FAILED } + List(51) { TransferStatus.UNKNOWN }, transfers.map { status(it) })   // 첫 청크만 커밋
        assertEquals(AlertLevel.CRITICAL, recordedAlerts.about(day).single().level)

        jdbc.update("INSERT INTO wallet (user_id, balance, version, created_at, updated_at) VALUES (?, 0, 0, NOW(6), NOW(6))", broken)   // 운영자가 고쳤다
        val second = launcher.run(day, ReconTrigger.SCHEDULE)

        val summary = assertIs<ReconRun.Completed>(second).summary
        assertEquals(151, summary.transfers)                                  // 첫 실행의 100건 + 재시작의 51건 (단계 건수가 이어진다)
        assertEquals(151, summary.autoFailed)
        assertTrue(transfers.all { status(it) == TransferStatus.FAILED })
        assertEquals(
            151,
            jdbc.queryForObject(
                "SELECT COUNT(*) FROM wallet_ledger WHERE type = 'REFUND' AND transfer_id IN (${ids.joinToString(",") { "'$it'" }})",
                Int::class.java,
            ),
        )
        assertEquals(10_000_000L, wallets.balanceOf(user))
        assertEquals(1_000L, wallets.balanceOf(broken))
        assertEquals(2, executions(day))
    }

    @Test
    fun `동시 실행 - 같은 날짜를 두 곳에서 돌리면 하나만 실행되고 다른 하나는 Running`() {
        val day = ReconTestDates.unique()
        writeBankFile(day, emptyList())
        val entered = CountDownLatch(1)
        val release = CountDownLatch(1)
        // 첫 실행을 ⑥ 보고(완료 알림)에서 붙잡아 둔다 — 잡이 돌고 있는 동안 두 번째가 온다
        recordedAlerts.onNotify = { if (day.toString() in it.title) { entered.countDown(); release.await(20, TimeUnit.SECONDS) } }
        try {
            val first = CompletableFuture.supplyAsync { launcher.run(day, ReconTrigger.MANUAL) }
            assertTrue(entered.await(20, TimeUnit.SECONDS))

            val second = launcher.run(day, ReconTrigger.MANUAL)
            release.countDown()

            assertIs<ReconRun.Running>(second)
            assertIs<ReconRun.Completed>(first.get(20, TimeUnit.SECONDS))
            assertEquals(1, executions(day))
        } finally {
            release.countDown()
            recordedAlerts.onNotify = null
        }
    }

    @Test
    fun `같은 날짜의 실행이 동시에 둘 만들어지면(실패한 날짜의 동시 재시작) 먼저 만든 쪽만 계속한다 - SingleRunGuard`() {
        val day = ReconTestDates.unique()
        val params = DailyRecon.parameters(day)
        val instance = jobRepository.createJobInstance(DailyRecon.JOB_NAME, params)
        val earlier = jobRepository.createJobExecution(instance, params, ExecutionContext())
        val later = jobRepository.createJobExecution(instance, params, ExecutionContext())
        val guard = SingleRunGuard(jobRepository)
        try {
            guard.beforeJob(earlier)                                                  // 먼저 만든 쪽은 계속한다
            assertFailsWith<ReconRunningException> { guard.beforeJob(later) }       // 나중 쪽은 멈춘다 → 실행기가 Running 으로 답한다
        } finally {
            // 이 날짜는 다시 쓰지 않지만 '돌고 있는 실행' 으로 남기지 않는다
            listOf(earlier, later).forEach {
                it.status = BatchStatus.FAILED
                it.endTime = LocalDateTime.now()
                jobRepository.update(it)
            }
        }
    }

    @Test
    fun `파일이 아직 없으면 FILE_NOT_READY 로 실패 - 마지막 회차 전에는 알리지 않고, 파일을 놓고 다시 돌리면 완료`() {
        val day = ReconTestDates.unique()

        val first = launcher.run(day, ReconTrigger.SCHEDULE)

        assertEquals(DailyRecon.FILE_NOT_READY, assertIs<ReconRun.Failed>(first).failure.code)
        assertTrue(recordedAlerts.about(day).isEmpty())

        writeBankFile(day, emptyList())
        assertIs<ReconRun.Completed>(launcher.run(day, ReconTrigger.SCHEDULE))
        assertTrue("완료" in recordedAlerts.about(day).single().title)
    }

    @Test
    fun `마지막 회차(06시)에도 끝내지 못하면 대사 미완료 긴급 알림, 수동 실행의 FILE_NOT_READY 는 알리지 않는다`() {
        val manual = ReconTestDates.unique()
        assertIs<ReconRun.Failed>(launcher.run(manual, ReconTrigger.MANUAL))
        assertTrue(recordedAlerts.about(manual).isEmpty())

        val final = ReconTestDates.unique()
        assertIs<ReconRun.Failed>(launcher.run(final, ReconTrigger.SCHEDULE_FINAL))
        val alert = recordedAlerts.about(final).single()
        assertEquals(AlertLevel.CRITICAL, alert.level)
        assertTrue("미완료" in alert.title)
        assertTrue(alert.lines.any { DailyRecon.FILE_NOT_READY in it })
    }

    @Test
    fun `형식이 틀린 파일은 FILE_INVALID 로 실패하고 긴급 알림 - 출금은 그대로`() {
        val day = ReconTestDates.unique()
        val unknown = seed(fundedUser(), TransferStatus.UNKNOWN, at(day))
        val file = props.reconFile(day)
        Files.createDirectories(file.parent)
        Files.writeString(file, "H${day.toString().replace("-", "")}090\n")   // 59바이트가 아니고 트레일러도 없다

        val run = launcher.run(day, ReconTrigger.SCHEDULE)

        assertEquals(DailyRecon.FILE_INVALID, assertIs<ReconRun.Failed>(run).failure.code)
        assertEquals(AlertLevel.CRITICAL, recordedAlerts.about(day).single().level)
        assertEquals(TransferStatus.UNKNOWN, status(unknown))
    }
}
```

`dailyrecon/DailyReconSchedulerTest.kt`:
```kotlin
package io.github.hyeokjinon.banktransfer.api.dailyrecon

import io.github.hyeokjinon.banktransfer.api.alert.Alert
import io.github.hyeokjinon.banktransfer.api.alert.AlertLevel
import io.github.hyeokjinon.banktransfer.api.alert.AlertNotifier
import java.time.Clock
import java.time.Instant
import java.time.LocalDate
import java.time.ZoneOffset
import kotlin.test.Test
import kotlin.test.assertEquals

/** 매시 회차 (P5-1): 시계는 UTC, 날짜 · 마지막 회차 판단은 KST. 실행기는 함수로 바꿔 끼운다. */
class DailyReconSchedulerTest {

    private val calls = mutableListOf<Pair<LocalDate, ReconTrigger>>()
    private val alerts = mutableListOf<Alert>()

    private fun scheduler(nowUtc: String, run: (LocalDate, ReconTrigger) -> ReconRun = { date, trigger -> calls += date to trigger; ReconRun.AlreadyCompleted(date) }) =
        DailyReconScheduler(run, Clock.fixed(Instant.parse(nowUtc), ZoneOffset.UTC), "0 0 1-6 * * *", "Asia/Seoul", AlertNotifier { alerts += it })

    @Test
    fun `01시 회차는 전날(KST) 대사 - 마지막 회차가 아니다`() {
        scheduler("2026-10-29T16:00:00Z").run()   // KST 10-30 01:00

        assertEquals(listOf(LocalDate.of(2026, 10, 29) to ReconTrigger.SCHEDULE), calls)
    }

    @Test
    fun `05시는 마지막이 아니고 06시는 마지막 회차 - 같은 cron 의 다음 실행이 다음 날`() {
        scheduler("2026-10-29T20:00:00Z").run()   // KST 05:00
        scheduler("2026-10-29T21:00:00Z").run()   // KST 06:00

        assertEquals(listOf(ReconTrigger.SCHEDULE, ReconTrigger.SCHEDULE_FINAL), calls.map { it.second })
        assertEquals(setOf(LocalDate.of(2026, 10, 29)), calls.map { it.first }.toSet())
    }

    @Test
    fun `실행이 예외를 던져도 스케줄러는 던지지 않고 긴급 알림`() {
        scheduler("2026-10-29T16:00:00Z") { _, _ -> error("db down") }.run()

        assertEquals(AlertLevel.CRITICAL, alerts.single().level)
    }
}
```

`web/ReconciliationApiTest.kt`:
```kotlin
package io.github.hyeokjinon.banktransfer.api.web

import io.github.hyeokjinon.banktransfer.api.dailyrecon.DailyReconLauncher
import io.github.hyeokjinon.banktransfer.api.dailyrecon.DailyReconTestSupport
import io.github.hyeokjinon.banktransfer.api.dailyrecon.ReconTestDates
import io.github.hyeokjinon.banktransfer.api.dailyrecon.ReconTrigger
import io.github.hyeokjinon.banktransfer.transfer.TransferStatus
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.core.ParameterizedTypeReference
import java.time.LocalDate
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/** 관리 API (P5-1, 설계 §5-2): 200 {status, summary} (실패해도 200 + FAILED + 이유), 409 두 가지, 내일 이후 400. 인증은 범위 밖. */
class ReconciliationApiTest : DailyReconTestSupport() {

    @Autowired private lateinit var launcher: DailyReconLauncher

    private val mapType = object : ParameterizedTypeReference<Map<String, Any?>>() {}

    private data class Response(val status: Int, val body: Map<String, Any?>)

    private fun post(date: String?): Response =
        client.post().uri(if (date == null) "/admin/reconciliations" else "/admin/reconciliations?date=$date")
            .exchange { _, r -> Response(r.statusCode.value(), r.bodyTo(mapType) ?: emptyMap()) }

    @Suppress("UNCHECKED_CAST")
    private fun Response.summary() = body["summary"] as Map<String, Any?>

    @Suppress("UNCHECKED_CAST")
    private fun Response.reason() = body["reason"] as Map<String, Any?>

    @Test
    fun `완료하면 200 COMPLETED + 요약, 다시 보내면 409 RECON_ALREADY_COMPLETED`() {
        val day = ReconTestDates.unique()
        val paid = seed(fundedUser(), TransferStatus.UNKNOWN, at(day))
        writeBankFile(day, listOf(lineOf(paid)))

        val first = post(day.toString())

        assertEquals(200, first.status)
        assertEquals("COMPLETED", first.body["status"])
        assertEquals(day.toString(), first.body["date"])
        assertEquals(1, first.summary()["transfers"])
        assertEquals(1, first.summary()["autoCompleted"])
        assertEquals(TransferStatus.COMPLETED, status(paid))

        val again = post(day.toString())

        assertEquals(409 to "RECON_ALREADY_COMPLETED", again.status to again.body["code"])
    }

    @Test
    fun `파일이 없으면 200 FAILED + 이유 FILE_NOT_READY (요청은 처리됐고 결과가 실패)`() {
        val response = post(ReconTestDates.unique().toString())

        assertEquals(200, response.status)
        assertEquals("FAILED", response.body["status"])
        assertEquals("FILE_NOT_READY", response.reason()["code"])
        assertEquals(null, response.body["summary"])
    }

    @Test
    fun `내일 이후 날짜는 400 RECON_DATE_IN_FUTURE - 오늘(KST)은 손 확인용으로 허용, 점검 시간에도 그대로`() {
        val today = ReconTestDates.unique()
        testClock.fix(today.atTime(23, 55).atZone(java.time.ZoneId.of("Asia/Seoul")).toInstant())   // 점검 시간 — 관리 API 는 막지 않는다

        val tomorrow = post(today.plusDays(1).toString())
        val sameDay = post(today.toString())

        assertEquals(400 to "RECON_DATE_IN_FUTURE", tomorrow.status to tomorrow.body["code"])
        assertEquals(200 to "FAILED", sameDay.status to sameDay.body["status"])   // 파일이 없을 뿐 실행은 됐다
    }

    @Test
    fun `날짜가 없거나 형식이 틀리면 400 INVALID_REQUEST`() {
        assertEquals(400 to "INVALID_REQUEST", post(null).let { it.status to it.body["code"] })
        assertEquals(400 to "INVALID_REQUEST", post("2026-13-01").let { it.status to it.body["code"] })
        assertEquals(400 to "INVALID_REQUEST", post("20261028").let { it.status to it.body["code"] })
    }

    @Test
    fun `실행 중이면 409 RECON_RUNNING`() {
        val day = ReconTestDates.unique()
        writeBankFile(day, emptyList())
        val entered = CountDownLatch(1)
        val release = CountDownLatch(1)
        recordedAlerts.onNotify = { if (day.toString() in it.title) { entered.countDown(); release.await(20, TimeUnit.SECONDS) } }
        try {
            val running = CompletableFuture.supplyAsync { launcher.run(day, ReconTrigger.SCHEDULE) }
            assertTrue(entered.await(20, TimeUnit.SECONDS))

            val response = post(day.toString())
            release.countDown()

            assertEquals(409 to "RECON_RUNNING", response.status to response.body["code"])
            running.get(20, TimeUnit.SECONDS)
        } finally {
            release.countDown()
            recordedAlerts.onNotify = null
        }
    }
}
```

`ApiExceptionHandlerTest.kt` — 클래스 끝(마지막 `}` 앞)에 추가 (import `io.github.hyeokjinon.banktransfer.api.dailyrecon.ReconAlreadyCompletedException`, `…ReconDateInFutureException`, `…ReconRunningException`, `java.time.LocalDate`):
```kotlin

    @Test
    fun `대사 - 이미 완료 409 RECON_ALREADY_COMPLETED, 실행 중 409 RECON_RUNNING, 내일 이후 400 RECON_DATE_IN_FUTURE`() {
        val day = LocalDate.of(2026, 10, 28)

        val completed = handler.handle(ReconAlreadyCompletedException(day))
        val running = handler.handle(ReconRunningException(day))
        val future = handler.handle(ReconDateInFutureException(day.plusDays(2), day))

        assertEquals(409 to "RECON_ALREADY_COMPLETED", completed.statusCode.value() to completed.body!!.properties?.get("code"))
        assertEquals(409 to "RECON_RUNNING", running.statusCode.value() to running.body!!.properties?.get("code"))
        assertEquals(400 to "RECON_DATE_IN_FUTURE", future.statusCode.value() to future.body!!.properties?.get("code"))
        assertNull(running.headers.getFirst(HttpHeaders.RETRY_AFTER))
    }
```

- [ ] **Step 2: 실패 확인**

Run: `./gradlew :apps:api:test --tests '*DailyReconLauncherTest*' --tests '*DailyReconSchedulerTest*' --tests '*ReconciliationApiTest*' --tests '*ApiExceptionHandlerTest*'`
Expected: FAIL (컴파일: `Unresolved reference 'DailyReconLauncher'`, `'ReconRun'`, `'ReconTrigger'`, `'SingleRunGuard'`, `'DailyReconScheduler'`, `'ReconAlreadyCompletedException'`)

- [ ] **Step 3: 구현**

`dailyrecon/DailyReconLauncher.kt`:
```kotlin
package io.github.hyeokjinon.banktransfer.api.dailyrecon

import io.github.hyeokjinon.banktransfer.api.alert.Alert
import io.github.hyeokjinon.banktransfer.api.alert.AlertLevel
import io.github.hyeokjinon.banktransfer.api.alert.AlertNotifier
import io.github.hyeokjinon.banktransfer.api.alert.notifySafely
import org.slf4j.LoggerFactory
import org.springframework.batch.core.BatchStatus
import org.springframework.batch.core.job.Job
import org.springframework.batch.core.job.JobExecution
import org.springframework.batch.core.launch.JobExecutionAlreadyRunningException
import org.springframework.batch.core.launch.JobInstanceAlreadyCompleteException
import org.springframework.batch.core.launch.JobOperator
import org.springframework.batch.core.launch.JobRestartException
import org.springframework.batch.core.listener.JobExecutionListener
import org.springframework.batch.core.repository.JobRepository
import org.springframework.dao.DuplicateKeyException
import org.springframework.dao.PessimisticLockingFailureException
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

class ReconRunningException(date: LocalDate) : RuntimeException("daily reconciliation for $date is already running")

class ReconAlreadyCompletedException(date: LocalDate) : RuntimeException("daily reconciliation for $date is already completed")

class ReconDateInFutureException(date: LocalDate, today: LocalDate) : RuntimeException("recon date $date is after today ($today, KST)")

/**
 * 대사일 하나의 잡을 이 스레드에서 끝까지 돌린다 (P5-1). Spring Batch 6 의 JobOperator 는 기본이 동기 실행(SyncTaskExecutor)이다.
 * 같은 날짜는 잡 저장소가 인스턴스 하나로 둔다: 끝났으면 [ReconRun.AlreadyCompleted], 돌고 있으면 [ReconRun.Running], 실패했으면 멈춘 곳부터 다시.
 * 처음 시작하는 날짜를 두 곳이 동시에 시작하면 진 쪽은 잡 인스턴스 UNIQUE 에서 DuplicateKeyException(직렬화 격리라 데드락일 수도)을 받는다 → Running.
 * 실패한 날짜를 두 곳이 동시에 재시작하는 경우는 [SingleRunGuard] 가 나중 실행을 멈춘다 → Running.
 *
 * 실패 알림 (설계 §8): 스케줄의 마지막 회차는 이유와 무관하게 "대사 미완료" CRITICAL, 그 전 회차 · 수동의 FILE_NOT_READY 는 알리지 않음
 * (은행 파일은 보통 00:05 에 온다), 그 밖 실패는 CRITICAL. 완료 알림은 잡의 ⑥ 보고가 보낸다.
 */
class DailyReconLauncher(
    private val jobOperator: JobOperator,
    private val job: Job,
    private val summaries: ReconSummaryReader,
    private val alerts: AlertNotifier,
) {
    private val log = LoggerFactory.getLogger(javaClass)

    fun run(date: LocalDate, trigger: ReconTrigger): ReconRun {
        val execution = try {
            jobOperator.start(job, DailyRecon.parameters(date))
        } catch (e: JobInstanceAlreadyCompleteException) {
            log.info("daily reconciliation {} is already completed, skipped ({})", date, trigger)
            return ReconRun.AlreadyCompleted(date)
        } catch (e: JobExecutionAlreadyRunningException) {
            return running(date, trigger)
        } catch (e: DuplicateKeyException) {
            return running(date, trigger)
        } catch (e: PessimisticLockingFailureException) {
            return running(date, trigger)
        } catch (e: JobRestartException) {
            // 지난 실행이 UNKNOWN(롤백할 수 없는 실패)으로 끝났다 — 사람이 BATCH_* 상태를 확인해야 한다 (OPERATION §9)
            return failed(date, ReconFailure("RESTART_REFUSED", e.message.orEmpty()), trigger)
        }
        return outcomeOf(date, execution, trigger)
    }

    private fun outcomeOf(date: LocalDate, execution: JobExecution, trigger: ReconTrigger): ReconRun {
        if (execution.status == BatchStatus.COMPLETED) return ReconRun.Completed(date, summaries.read(execution.jobInstance, date))
        val causes = execution.allFailureExceptions.flatMap { generateSequence(it) { cause -> cause.cause }.toList() }
        if (causes.any { it is ReconRunningException }) return running(date, trigger)
        val failure = causes.firstNotNullOfOrNull { it as? ReconFileException }?.let { ReconFailure(it.code, it.message.orEmpty()) }
            ?: ReconFailure("ERROR", causes.firstOrNull()?.javaClass?.name ?: execution.exitStatus.exitCode)
        return failed(date, failure, trigger)
    }

    private fun running(date: LocalDate, trigger: ReconTrigger): ReconRun {
        log.info("daily reconciliation {} is already running elsewhere, skipped ({})", date, trigger)
        return ReconRun.Running(date)
    }

    private fun failed(date: LocalDate, failure: ReconFailure, trigger: ReconTrigger): ReconRun {
        when {
            trigger == ReconTrigger.SCHEDULE_FINAL -> alerts.notifySafely(
                Alert(
                    AlertLevel.CRITICAL, "일마감 대사 $date 미완료 — 마지막 회차까지 끝내지 못함",
                    listOf("이유: ${failure.code} ${failure.message}", "확인 뒤 POST /admin/reconciliations?date=$date 로 다시 실행 (OPERATION §9)"),
                ),
            )
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
 * 같은 날짜의 잡이 동시에 둘 돌지 않게 하는 마지막 확인 (P5-1, 계획 "스펙과 다른 점" 1).
 * Spring Batch 6.0.5 의 시작 경로는 "돌고 있는 실행이 있나" 를 본 뒤 별도 트랜잭션에서 실행을 만든다 — 실패한 날짜를 두 곳이 동시에
 * 재시작하면 둘 다 확인을 통과할 수 있다(새 날짜는 잡 인스턴스 UNIQUE 가 막는다). 그래서 실행이 시작된 직후, 같은 인스턴스에 먼저 만든(작은 id)
 * 실행이 아직 돌고 있으면 이 실행을 멈춘다 — 이 실행만 FAILED 로 끝나고 [DailyReconLauncher] 는 "실행 중" 으로 답한다.
 * 먼저 실행이 끝난 뒤의 재시작은 막지 않는다 (돌고 있는 실행이 없다).
 */
class SingleRunGuard(private val jobRepository: JobRepository) : JobExecutionListener {

    override fun beforeJob(jobExecution: JobExecution) {
        val earlierRunning = jobRepository.findRunningJobExecutions(jobExecution.jobInstance.jobName)
            .any { it.jobInstanceId == jobExecution.jobInstanceId && it.id < jobExecution.id }
        if (earlierRunning) {
            throw ReconRunningException(checkNotNull(jobExecution.jobParameters.getLocalDate(DailyRecon.RECON_DATE)))
        }
    }
}
```

`dailyrecon/DailyReconScheduler.kt`:
```kotlin
package io.github.hyeokjinon.banktransfer.api.dailyrecon

import io.github.hyeokjinon.banktransfer.api.alert.Alert
import io.github.hyeokjinon.banktransfer.api.alert.AlertLevel
import io.github.hyeokjinon.banktransfer.api.alert.AlertNotifier
import io.github.hyeokjinon.banktransfer.api.alert.notifySafely
import org.slf4j.LoggerFactory
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.scheduling.support.CronExpression
import java.time.Clock
import java.time.LocalDate
import java.time.ZoneId
import java.time.ZonedDateTime

/**
 * 매일 01:00 ~ 06:00 매시 정각(KST)에 전날 대사를 돌린다 (P5-1, 설계 §5-2). 파일이 아직 없으면 다음 정각에 다시(멈춘 곳부터),
 * 이미 끝났으면 조용히 넘어간다. 마지막 회차 = 같은 cron 의 다음 실행이 다음 날인 회차 — cron 을 바꿔도 따로 고칠 값이 없다.
 * 대사는 이 스케줄 스레드에서 동기로 돈다 (스케줄 풀 3 — 발행기 · 정리 작업과 나눠 쓴다). 테스트는 이 빈을 끄고 실행기를 직접 부른다.
 */
class DailyReconScheduler(
    /** 실행기 (테스트는 함수로 바꿔 끼운다). */
    private val launch: (LocalDate, ReconTrigger) -> ReconRun,
    private val clock: Clock,
    cron: String,
    zone: String,
    private val alerts: AlertNotifier,
) {
    private val log = LoggerFactory.getLogger(javaClass)
    private val cron = CronExpression.parse(cron)
    private val zone = ZoneId.of(zone)

    @Scheduled(cron = "\${daily-recon.schedule.cron:0 0 1-6 * * *}", zone = "\${daily-recon.schedule.zone:Asia/Seoul}")
    fun run() {
        val now = ZonedDateTime.ofInstant(clock.instant(), zone)
        val date = now.toLocalDate().minusDays(1)
        try {
            val result = launch(date, if (isFinalRound(now)) ReconTrigger.SCHEDULE_FINAL else ReconTrigger.SCHEDULE)
            log.info("daily reconciliation round {}: {}", now.toLocalTime(), result)
        } catch (e: Exception) {
            // 실행기는 실패를 결과로 돌려준다. 여기 오는 것은 시작조차 못 한 경우(DB 장애 등)다
            log.error("daily reconciliation {} could not be started", date, e)
            alerts.notifySafely(Alert(AlertLevel.CRITICAL, "일마감 대사 $date 를 시작하지 못함", listOf(e.javaClass.name)))
        }
    }

    internal fun isFinalRound(now: ZonedDateTime): Boolean = cron.next(now)?.toLocalDate() != now.toLocalDate()
}
```

`DailyReconJobConfiguration.kt`:
1) import 추가: `io.github.hyeokjinon.banktransfer.api.alert.AlertNotifier`(이미 있음), `org.springframework.batch.core.launch.JobOperator`, `org.springframework.boot.autoconfigure.condition.ConditionalOnProperty`.
2) `dailyReconciliationJob` 의 `.validator(…)` 줄 아래에 추가:
```kotlin
            .listener(SingleRunGuard(jobRepository))   // 실패한 날짜의 동시 재시작 — Spring Batch 6 의 시작 확인 사이의 틈을 막는다
```
3) 클래스 끝(마지막 `}` 앞)에 추가:
```kotlin

    @Bean
    fun dailyReconLauncher(jobOperator: JobOperator, job: Job, summaries: ReconSummaryReader, alerts: AlertNotifier) =
        DailyReconLauncher(jobOperator, job, summaries, alerts)

    @Bean
    @ConditionalOnProperty(prefix = "daily-recon.schedule", name = ["enabled"], havingValue = "true", matchIfMissing = true)
    fun dailyReconScheduler(launcher: DailyReconLauncher, clock: Clock, props: DailyReconProperties, alerts: AlertNotifier) =
        DailyReconScheduler(launcher::run, clock, props.schedule.cron, props.schedule.zone, alerts)
```

`web/ReconciliationController.kt`:
```kotlin
package io.github.hyeokjinon.banktransfer.api.web

import io.github.hyeokjinon.banktransfer.api.dailyrecon.DailyRecon
import io.github.hyeokjinon.banktransfer.api.dailyrecon.DailyReconLauncher
import io.github.hyeokjinon.banktransfer.api.dailyrecon.ReconAlreadyCompletedException
import io.github.hyeokjinon.banktransfer.api.dailyrecon.ReconDateInFutureException
import io.github.hyeokjinon.banktransfer.api.dailyrecon.ReconFailure
import io.github.hyeokjinon.banktransfer.api.dailyrecon.ReconRun
import io.github.hyeokjinon.banktransfer.api.dailyrecon.ReconRunningException
import io.github.hyeokjinon.banktransfer.api.dailyrecon.ReconSummary
import io.github.hyeokjinon.banktransfer.api.dailyrecon.ReconTrigger
import org.springframework.format.annotation.DateTimeFormat
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController
import java.time.Clock
import java.time.LocalDate

/** 대사 실행 결과. status = COMPLETED 면 summary, FAILED 면 reason. */
data class ReconRunResponse(val date: LocalDate, val status: String, val reason: ReconFailure?, val summary: ReconSummary?)

/**
 * 일마감 대사 수동 실행 (P5-1, 관리용 — 인증은 다른 API 와 같이 범위 밖). 점검 시간에도 그대로 받는다.
 * 이 요청 스레드에서 끝까지 돌리고 결과를 돌려준다. 실패해도 200 (status = FAILED + 이유) — 요청은 처리됐고 결과가 실패다.
 * 이미 끝난 날짜 · 실행 중이면 409, 내일 이후(KST)면 400. 오늘은 손 확인용으로 허용한다 (그 뒤 출금은 오늘 파일에 없다).
 */
@RestController
@RequestMapping("/admin/reconciliations")
class ReconciliationController(private val launcher: DailyReconLauncher, private val clock: Clock) {

    @PostMapping
    fun run(@RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) date: LocalDate): ReconRunResponse {
        val today = LocalDate.ofInstant(clock.instant(), DailyRecon.KST)
        if (date.isAfter(today)) throw ReconDateInFutureException(date, today)
        return when (val run = launcher.run(date, ReconTrigger.MANUAL)) {
            is ReconRun.Completed -> ReconRunResponse(date, "COMPLETED", null, run.summary)
            is ReconRun.Failed -> ReconRunResponse(date, "FAILED", run.failure, null)
            is ReconRun.AlreadyCompleted -> throw ReconAlreadyCompletedException(date)
            is ReconRun.Running -> throw ReconRunningException(date)
        }
    }
}
```

`ApiExceptionHandler.kt`:
1) import 추가: `io.github.hyeokjinon.banktransfer.api.dailyrecon.ReconAlreadyCompletedException`, `…ReconDateInFutureException`, `…ReconRunningException`.
2) `@ExceptionHandler(…)` 목록에 `ReconAlreadyCompletedException::class, ReconRunningException::class, ReconDateInFutureException::class,` 를 더한다.
3) `when (cause)` 의 `is TransferException.IllegalTransition -> …` 줄 아래에 추가:
```kotlin
            // P5-1 일마감 대사 관리 API. 같은 날짜는 잡 인스턴스 하나 — 끝났으면 다시 돌리지 않고, 돌고 있으면 기다리지 않는다
            is ReconAlreadyCompletedException -> HttpStatus.CONFLICT to "RECON_ALREADY_COMPLETED"
            is ReconRunningException -> HttpStatus.CONFLICT to "RECON_RUNNING"
            is ReconDateInFutureException -> HttpStatus.BAD_REQUEST to "RECON_DATE_IN_FUTURE"
```

`application.yml`:
1) `spring.task.scheduling.pool.size` 를 바꾼다:
```yaml
        size: 3   # 발행기(0.5초) · 정리 작업(10초) · 일마감 대사(01 ~ 06시, 동기로 수 초)가 서로 막지 않게 (P5-1 에서 2 → 3)
```
2) `daily-recon:` 절(Task 10) 끝에 추가:
```yaml
  schedule:
    enabled: true              # 부하 측정 실행 구성 · 테스트는 false
    cron: "0 0 1-6 * * *"      # 01:00 ~ 06:00 매시 정각에 전날 대사. 파일이 없으면 다음 정각에 다시, 마지막 회차(06시)에도 못 끝내면 긴급 알림
    zone: Asia/Seoul
```

`.run/5. API (부하 측정).run.xml` 의 `PROGRAM_PARAMETERS` 값 끝에 ` --daily-recon.schedule.enabled=false` 를 더한다 (측정 중에 대사가 끼어들지 않게).

- [ ] **Step 4: 통과 확인**

Run: `./gradlew :apps:api:test --tests '*DailyReconLauncherTest*' --tests '*DailyReconSchedulerTest*' --tests '*ReconciliationApiTest*' --tests '*ApiExceptionHandlerTest*'`
Expected: PASS (새 테스트 16개). 재시작 테스트는 151건을 심어 수 초 걸린다.

Run: `./gradlew build`
Expected: BUILD SUCCESSFUL (전 모듈 테스트)

- [ ] **Step 5: 커밋**

```bash
git add apps/api .run
git commit -m "feat(api): 대사 실행기 · 01~06시 매시 스케줄 · POST /admin/reconciliations (200 / 409 RECON_* / 400)

같은 날짜는 잡 저장소가 하나만 둔다: 끝났으면 409, 돌고 있으면 409, 실패했으면 멈춘 곳부터 잇는다(재시작해도 이중 환불 없음).
Spring Batch 6 의 시작 확인과 실행 생성 사이의 틈(실패한 날짜의 동시 재시작)은 SingleRunGuard 가 막는다.
파일이 아직 없으면 마지막 회차(cron 의 다음 실행이 다음 날)에서만 '대사 미완료' 를 알리고, 그 밖 실패는 바로 알린다.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 12: 결정 기록 · 문서 · 손 확인 자료

**Files:**
- Create: `docs/adr/0010-daily-reconciliation.md`
- Create: `http/daily-recon.http`
- Create: `postman/collections/bank-transfer/6-daily-recon/.resources/definition.yaml` 와 요청 5개 (`01-maintenance-transfer`, `02-maintenance-wallet`, `03-run-recon`, `04-run-again`, `05-tomorrow` `.request.yaml`)
- Create: `.run/6. API (대사 손 확인).run.xml`
- Modify: `docs/protocol.md`, `docs/database.md`, `OPERATION.md`, `SETUP.md`, `ARCHITECTURE.md`, `README.md`, `HANDOVER.md`, `CHANGELOG.md`, `postman/collections/bank-transfer/.resources/definition.yaml`

**Interfaces:**
- Consumes: Task 1 ~ 11 의 이름 · 설정 · 명령 · 오류 코드, Task 8 커밋 본문의 변이 검사 결과(`git log --grep '일마감 대사 판정' --format=%B`), Step 9 의 전체 테스트 수
- Produces: ADR-0010 (상태: 제안 — 머지 때 채택)

- [ ] **Step 1: ADR-0010** — 기존 ADR 형식(제목 · 상태 · 단계 · 맥락 · 결정 · 결과 · 검토한 다른 선택지):
  - 제목: `# ADR-0010 일마감 대사는 은행 대사 파일과 맞추고 결과를 모르는 출금만 자동 확정한다 — 점검 시간으로 날짜 경계를 맞추고 API 앱 안의 Spring Batch 로 돌린다`
  - 상태: 제안 (2026-10-29). 단계: P5-1 · 설계: [P5-1 설계](../superpowers/specs/2026-10-29-p5-1-daily-reconciliation-design.md) · [ADR-0006](0006-inquire-instead-of-resend.md) (결과조회) · [ADR-0008](0008-wallet-debit-on-intake-refund-on-failure.md) (`fail()` 환불) · [database.md](../database.md) §4-6 ~ §4-8 · [protocol.md](../protocol.md) "대사 파일"
  - 맥락: 설계 §1 의 네 갈래(결과조회 10번에도 모르는 `UNKNOWN`, 확정 · 환불 뒤 늦은 "처리됨" CONFLICT, 미전송으로 보고 다시 보냈는데 실제로는 간 건, 사람 · 버그가 고친 행)는 우리 DB 안에서는 멀쩡해 보인다 → 은행이라는 독립된 기록과 맞춰야 드러난다. 날짜 경계가 흐리면 "D일 출금" 과 "D일 파일" 이 어긋난다.
  - 결정: (1) 은행 쪽 기록 = 일자별 대사 파일(고정길이 EUC-KR 헤더 · 상세 · 트레일러, 규격 v0.3), 형식 코드는 `bank-protocol` 하나 (2) 자동 보정은 **`UNKNOWN` · `SENDING` 만**: 있으면 `COMPLETED`(0000) + `AUTO_COMPLETED`, 없으면 `fail(E002)` + 환불 + `AUTO_FAILED`. 확정된 건의 어긋남(설계 §6 의 5 ~ 11)은 `recon_issue` + 알림만 (3) 판정은 순수 함수 `StatementMatcher`, 확정은 `StatementSettler`(기존 조건부 전이 · `fail` 그대로, 0건이면 다시 읽어 판정 최대 3회) (4) API 앱 안의 Spring Batch 6 (JDBC 잡 저장소 = `spring-boot-starter-batch-jdbc`, 메타 표는 Flyway V4), 6단계 · 청크 500/100, ③ ④ READ COMMITTED (5) 날짜마다 잡 인스턴스 하나 — 끝난 날짜 거부 409, 실패하면 멈춘 곳부터, 동시 실행 하나만(`SingleRunGuard` 로 Spring Batch 6 시작 경로의 틈을 막음) (6) 점검 시간 23:50 ~ 00:10 KST: 웹 503 `BANK_MAINTENANCE` + `Retry-After`, 게이트웨이 `SendGate` 로 송신 보류, `Clock` 빈 하나 (7) 스케줄 01 ~ 06시 매시(`FILE_NOT_READY` 면 다음 정각, 마지막 회차에서만 "대사 미완료"), 수동 `POST /admin/reconciliations` (8) 알림 `AlertNotifier` + 텔레그램(토큰 · chat id 둘 다 있을 때) / 로그, 실패는 삼킨다, 가림 · 20건 · 4,096자, 기존 경고 3곳 WARN.
  - 결과: 증명 테스트 이름 — `StatementMatcherTest`(§6 표 줄마다), `StatementSettlerTest`(미확정만 확정 · 경합 다시 읽기 · 3회 포기), `StatementSettlerIsolationTest`(READ COMMITTED 는 다시 읽어 일치, REPEATABLE READ 는 스냅숏 때문에 포기 — 격리 수준을 낮춘 근거), `DailyReconJobTest`(전 칸: 전이 · 환불 · 이슈 종류별 · 알림 요약), `DailyReconLauncherTest`(재실행 거부 · ③ 중간 청크 실패 → 재시작 → 이중 환불 없음 · 동시 실행 하나 · `SingleRunGuard` · `FILE_NOT_READY` · 마지막 회차 · `FILE_INVALID`), `DailyReconSchedulerTest`, `ReconciliationApiTest`(200/409/400), `ReconFileFormatTest`, `BankJournalTest` · `MockBankRestartTest`, `BankMaintenanceWindowTest`(UTC 시계 → KST 판정), `BankMaintenanceApiTest` · `BankMaintenanceFlowTest`, `TelegramNotifierTest`(가짜 서버 · 500 · 시간 초과 · 토큰이 로그에 없음). **변이 검사**: Task 8 커밋 본문의 결과 다섯 줄을 그대로 옮긴다. 한계: 설계 §11 그대로 (점검 직전 출금을 은행이 20분 넘게 붙잡으면 그날은 자동 실패 → 다음 날 ④ 가 `REFUNDED_BUT_PAID` 로 알림 · 자동 복구 없음, 확정된 건의 어긋남은 SQL 로 처리, 개발 PC 는 켜져 있을 때만 스케줄, 관리 API 인증 없음, 텔레그램은 외부 서비스, 같은 프로세스에서 배치) + Spring Batch 메타 표 시각은 JVM 기본 시간대, ③ 은 `sending_at` 범위를 전체 훑기(인덱스 없음), ⑤ 는 지갑 전체 훑기.
  - 다른 선택지 표: 전문으로 거래내역을 페이징해 받기 — 실제 은행 · 금결원과 거리가 멀고 Batch 의 파일 읽기 · 재시작을 못 쓴다 / 확정된 건도 자동 보정 — 환불한 돈을 다시 빼려다 잔고 부족 등 보정이 새 사고가 된다 / 별도 배치 앱 — 저장소 코드 이전이 대사보다 큰 일(P8) / Spring Batch 없이 `@Scheduled` 만 — 재시작 지점 · 날짜별 중복 거부 · 동시 실행 방지를 직접 만들어야 한다 / `starter-batch`(메모리 잡 저장소) — 재시작 · 중복 거부가 프로세스를 넘지 못한다 / 점검 시간 없이 `processed_at` 으로 날짜를 맞추기 — 자정 직전 송신 · 자정 뒤 처리 건이 날짜를 넘나들어 매일 어긋남이 생긴다 / 점검을 웹에서만 막기 — 컨슈머 · 정리 작업 · 재발행은 웹을 거치지 않는다 / `LocalTime.now(clock)` — 서버 시계가 UTC 면 9시간 어긋난다 / 텔레그램 `parse_mode=MarkdownV2` — 이스케이프 실수 하나로 메시지가 거절된다 / 알림 재전송 큐 — 범위 밖(P5-2), 기록의 원본은 `recon_issue`.

- [ ] **Step 2: `docs/protocol.md` v0.3** — 제목을 `# 모의 은행 전문 규격 v0.3` 으로. `## 6. 세션 규칙` 뒤 · `## 변경 이력` 앞에 새 절:
  ````markdown
  ## 7. 대사 파일 (v0.3)

  은행이 하루에 한 번 만드는 파일이다. 그날 **실제로 처리(입금)한 이체만** 담는다 — 계좌가 없어 거절한 건은 돈이 움직이지 않았으므로 없다. 1원인증 송금(`V…`)도 들어간다.
  필드 규칙은 전문(§2)과 같다: AN 왼쪽 정렬 · 공백, N 오른쪽 정렬 · 0, EUC-KR, 길이는 바이트. 한 줄 = 한 레코드 + `\n`. 레코드는 모두 **59바이트**(가장 긴 상세 기준)로 공백을 채운다. 마지막 레코드 뒤에도 줄바꿈이 있다 — 없으면 쓰다 만 파일로 보고 거부한다.

  | 레코드 | 필드 (길이) | 합계 |
  |---|---|---|
  | 헤더 `H` | 구분 `H`(1) · 처리일 `yyyyMMdd`(8) · 은행코드(3) | 12 → 59 |
  | 상세 `D` | 구분 `D`(1) · 이용기관 거래번호 AN(20) · 은행코드 AN(3) · 계좌번호 AN(16) · 금액 N(13) · 처리시각 `HHmmss`(6) | 59 |
  | 트레일러 `T` | 구분 `T`(1) · 상세 건수 N(9) · 금액 합계 N(18) | 28 → 59 |

  - 파일 이름 `RECON_yyyyMMdd.dat`, 위치 `{bank-files}/recon/`. 날짜 = 처리 시각의 KST 날짜. 처리한 이체가 없는 날도 헤더 + 0건 트레일러 파일을 만든다.
  - 받는 쪽 검증: 모든 줄 59바이트, 첫 줄 `H` · 마지막 줄 `T`, 헤더 날짜 = 대사일, 트레일러 건수 · 합계 = 상세. 하나라도 틀리면 그 파일을 믿지 않는다 (`FILE_INVALID`).
  - 모의 은행은 처리할 때마다 상세 레코드를 일자별 거래 기록(`{bank-files}/journal/yyyyMMdd.log`)에 덧붙이고, 매일 00:05 (KST) 에 전날 기록으로 이 파일을 만든다 (임시 파일 → 원자적 이름 바꾸기, 이미 있으면 덮어쓰지 않음).

  예 (2026-10-28, 처리 1건, 공백은 `_`):
  ```
  H20261028090_______________________________________________
  DT261028ABCDEFGHIJKLM0901002123456789___0000000030000101530
  T000000001000000000000030000_______________________________
  ```
  ````
  (바깥 4중 펜스 안의 내용을 들여쓰기 없이 그대로 붙인다.) 변경 이력에 행: `| v0.3 | 2026-10-29 | 대사 파일(헤더 · 상세 · 트레일러, 59바이트 레코드) 추가 ([ADR-0010](adr/0010-daily-reconciliation.md)) |`.

- [ ] **Step 3: `docs/database.md`**:
  - 머리 인용문의 기준을 `P5-1 (Flyway V1 ~ V4 적용)` 로, 설계 배경에 `[P5-1 설계](superpowers/specs/2026-10-29-p5-1-daily-reconciliation-design.md)` 추가.
  - §2 시간대 정책 표 아래에 한 줄: "**예외**: Spring Batch 메타 표(`BATCH_*`)의 시각은 Spring Batch 가 `LocalDateTime.now()` 로 쓴다 = JVM 기본 시간대. `TimeZone.setDefault` 를 쓰지 않으므로 바꾸지 않는다 — 대사 결과의 기준은 업무 표(`recon_issue` 등, KST)다."
  - §3 ERD 에 `bank_statement_line` · `recon_issue` 를 더한다 (`recon_issue }o--o| transfer : transfer_id`, `recon_issue }o--o| wallet : user_id`, `bank_statement_line }o--o{ transfer : reference` — 모두 FK 없이 논리 관계).
  - 새 절 `### 4-6. Spring Batch 메타 표 (P5-1)`: `BATCH_JOB_INSTANCE`(잡 이름 + 식별 파라미터 해시 `JOB_KEY` 유니크 = 날짜마다 인스턴스 하나) · `BATCH_JOB_EXECUTION`(실행마다 한 줄, `STATUS`) · `…_PARAMS`(`reconDate`) · `BATCH_STEP_EXECUTION`(단계마다, 읽기 · 쓰기 · 커밋 · 롤백 수) · `…_CONTEXT`(재시작 지점, ③ ④ 판정별 건수 `verdict.*`) · `…_SEQ` 3개. "V4 가 spring-batch-core 6.0.5 의 `schema-mysql.sql` 을 그대로 만든다. 직접 고치지 않는다 (Spring Batch 가 쓴다)."
  - 새 절 `### 4-7. bank_statement_line — 은행 대사 파일의 상세 줄 (P5-1)`: 열 표(`bank_date` DATE 파일의 처리일 KST · `line_no` INT 물리 줄 번호(헤더 = 1) · `reference` CHAR(20) ascii_bin · `bank_code` · `account_no` · `amount` · `processed_at` DATETIME(6) KST · `loaded_at`), PK `(bank_date, line_no)` = 같은 파일을 다시 넣으면 무시, `idx_statement_reference` = 거래번호로 모든 날짜의 줄(이중 지급 · 늦은 처리). append-only.
  - 새 절 `### 4-8. recon_issue — 대사가 찾은 어긋남 · 자동 확정 기록 (P5-1)`: 열 표(`type` = `AUTO_COMPLETED` · `AUTO_FAILED` · `AMOUNT_OR_ACCOUNT_MISMATCH` · `COMPLETED_NOT_AT_BANK` · `REFUNDED_BUT_PAID` · `PAID_BUT_RECEIVED`(긴급) · `UNKNOWN_REFERENCE` · `BALANCE_LEDGER_MISMATCH` · `PAID_TWICE`(긴급), `our_value` = 판정 시점 상태 · 금액 · 계좌, `bank_value` = 은행 줄들 `금액 은행-계좌 날짜#줄`, `ref_key` = `COALESCE(reference, user_id)` STORED 생성 컬럼), 유니크 `uk_recon_issue (recon_date, type, ref_key)` = 재시작 · 재실행해도 늘지 않음, `idx_recon_issue_open (resolved_at, recon_date)`. append-only — 사람이 `resolved_at` · `resolution_note` 만 채운다. "DB 에는 계좌 원문이 있다 — 알림으로 나갈 때만 가린다."
  - §5 상태 값의 `bank_response_code` 설명에 `E002` = 일마감 대사로 은행 미처리 확정(돈은 움직이지 않았고 환불됨, `E001` 과 구분) 추가.
  - §6 동시성 표에 행 두 개: `| 일마감 대사 ③ ④ (P5-1) | READ COMMITTED 청크 + 조건부 전이 | 0건이면 다시 읽어 판정(최대 3회). REPEATABLE READ 면 청크 첫 읽기의 스냅숏이라 바뀐 상태를 못 본다 |`, `| 같은 날짜 대사 동시 실행 (P5-1) | 잡 인스턴스 UNIQUE + 실행 중 확인 + SingleRunGuard | 처음 시작 경합은 UNIQUE 에서 진 쪽이 DuplicateKeyException → 409 RECON_RUNNING. 실패한 날짜의 동시 재시작은 가드가 나중 실행을 멈춘다 |`.
  - §9 운영 쿼리에 `### 일마감 대사 (P5-1)` 소절 SQL 세 개:
    ```sql
    -- 아직 처리하지 않은 이슈 (긴급 먼저)
    SELECT id, recon_date, type, reference, our_value, bank_value, created_at
    FROM recon_issue WHERE resolved_at IS NULL
    ORDER BY FIELD(type, 'PAID_TWICE', 'PAID_BUT_RECEIVED') DESC, recon_date, id;

    -- 처리 기록 (append-only — 이 두 열만 바꾼다)
    UPDATE recon_issue SET resolved_at = NOW(6), resolution_note = '은행 확인: 이중 지급 회수 요청 #123' WHERE id = ? AND resolved_at IS NULL;

    -- 대사 잡 실행 이력 (날짜별 상태 · 실패 이유)
    SELECT p.PARAMETER_VALUE AS recon_date, e.JOB_EXECUTION_ID, e.STATUS, e.EXIT_CODE, LEFT(e.EXIT_MESSAGE, 200) AS exit_message, e.START_TIME, e.END_TIME
    FROM BATCH_JOB_INSTANCE i
    JOIN BATCH_JOB_EXECUTION e ON e.JOB_INSTANCE_ID = i.JOB_INSTANCE_ID
    JOIN BATCH_JOB_EXECUTION_PARAMS p ON p.JOB_EXECUTION_ID = e.JOB_EXECUTION_ID AND p.PARAMETER_NAME = 'reconDate'
    WHERE i.JOB_NAME = 'dailyReconciliationJob' ORDER BY e.JOB_EXECUTION_ID DESC LIMIT 20;
    ```
  - §10 검토 중에 두 줄: "`transfer.sending_at` 단독 인덱스 — 대사 ③ 이 하루 범위를 훑는다(지금은 전체 훑기, 하루 한 번)", "대사 보존 · 파티셔닝 — `bank_statement_line` · `recon_issue` 는 매일 늘어난다 (P5-2 보존 배치)".
  - 변경 이력 행: `| 2026-10-29 | V4: Spring Batch 6 메타 표 · bank_statement_line · recon_issue, E002, 대사 동시성 · 운영 쿼리 | P5-1 (ADR-0010) |`.

- [ ] **Step 4: `OPERATION.md`**:
  - §1 표의 모의 은행 행 실행 칸을 `./gradlew :apps:mock-bank:run --args="9090"` (작업 디렉터리 = 저장소 루트, 기록 · 대사 파일은 `./bank-files`) 로 보강하고, 표 아래에 "모의 은행 · API 는 `./bank-files` 를 같이 본다 (Gradle `run` · `bootRun` 이 저장소 루트에서 돈다). `git status` 에 보이지 않는다(`.gitignore`)." 한 줄.
  - §2 설정 표에 행 추가 (`spring.task.scheduling.pool.size` 행은 값을 `3` 으로, 의미에 "일마감 대사(01 ~ 06시)까지 셋이 서로 막지 않게"):
    - `| bank.maintenance.enabled / start / end | true / "23:50" / "00:10" | 은행 점검 시간 (KST, 시작 포함 · 끝 제외, 자정 넘김 가능). yml 에서는 따옴표 필수. 길이는 재발행 포기(E001)까지의 약 26분보다 짧게 |`
    - `| daily-recon.file-dir | ./bank-files | 은행 대사 파일 디렉터리 (recon/RECON_yyyyMMdd.dat) |`
    - `| daily-recon.chunk-size | 100 | ③ 출금 대조 · ④ 은행 줄 대조 청크 (② 적재는 500 고정) |`
    - `| daily-recon.schedule.enabled / cron / zone | true / 0 0 1-6 * * * / Asia/Seoul | 매시 정각 전날 대사. 부하 측정 · 테스트는 false |`
    - `| alert.telegram.bot-token / chat-id | (비어 있음) | 환경변수 ALERT_TELEGRAM_BOT_TOKEN · ALERT_TELEGRAM_CHAT_ID. 둘 다 있을 때만 텔레그램, 아니면 로그(ALERT 로거) |`
    - `| alert.telegram.base-url | https://api.telegram.org | Bot API 주소 |`
    - `| spring.batch.job.enabled / spring.batch.jdbc.initialize-schema | false / never | 기동 때 잡을 돌리지 않는다 / BATCH_* 표는 Flyway V4 |`
  - §3 모의 은행 시나리오 표 아래에 소절 `### 거래 기록 · 대사 파일 (P5-1)`: 처리할 때마다 `bank-files/journal/yyyyMMdd.log` 에 한 줄, 재기동하면 기록으로 처리 목록을 되살린다(결과조회가 그대로 맞다), 매일 00:05 에 전날 `bank-files/recon/RECON_yyyyMMdd.dat`(이미 있으면 그대로), `./gradlew :apps:mock-bank:run --args="--close-day=20261029"` = 서버 없이 그 날짜 파일만 만들고 끝(다시 만들려면 파일을 지우고), `--bank-files=경로` 로 디렉터리 변경. 손 확인 절차: [`http/daily-recon.http`](http/daily-recon.http).
  - §4 오류 코드 표에 행 넷: `| 은행 점검 시간 (23:50 ~ 00:10) 의 출금 접수 · 1원인증 요청 · 확인 | 503 | BANK_MAINTENANCE (+ Retry-After: 점검 종료까지 남은 초) |`, `| 대사: 이미 완료한 날짜 | 409 | RECON_ALREADY_COMPLETED |`, `| 대사: 같은 날짜가 실행 중 | 409 | RECON_RUNNING |`, `| 대사: 내일 이후 날짜 (KST) | 400 | RECON_DATE_IN_FUTURE |`. 표 아래 `E001` 문장 뒤에 "`E002` 는 일마감 대사가 은행 대사 파일로 \"은행이 처리하지 않았다\" 를 확정한 것이다 (돈은 움직이지 않았고 환불됐다)." 추가.
  - §6 장애 대응 표에 행: `| "대사 미완료" 긴급 알림 (06시) | 이유 코드, bank-files/recon/ 에 그날 파일, 모의 은행 로그 [일마감] | FILE_NOT_READY: 모의 은행이 00:05 에 떠 있지 않았다 → --close-day=그날 로 만들고 POST /admin/reconciliations?date=그날. FILE_INVALID: 파일을 확인(헤더 날짜 · 트레일러) — 은행에 재전송 요청(모의는 지우고 --close-day). ERROR: API 로그의 예외 → 고치고 같은 요청으로 멈춘 곳부터 |`, `| "일마감 대사 … 불일치 N건" 긴급 알림 | recon_issue (database.md §9 열린 이슈 쿼리) | 종류별: PAID_TWICE · PAID_BUT_RECEIVED 는 긴급 — 은행에 확인 · 회수 요청, RECEIVED 면 재발행 전에 손으로 COMPLETED 처리. REFUNDED_BUT_PAID 는 환불한 돈이 나갔다. COMPLETED_NOT_AT_BANK 는 은행에 확인. BALANCE_LEDGER_MISMATCH 는 원장을 고치지 말고 원인을 찾는다. 처리 뒤 resolved_at · resolution_note 를 SQL 로 남긴다 (database.md §9) |`, `| 409 RECON_RUNNING 이 계속 (앱이 대사 도중 죽음) | 대사 잡 실행 이력 쿼리의 STATUS = STARTED · END_TIME NULL | 앱이 죽어 실행이 '돌고 있음' 으로 남았다. 다른 인스턴스가 정말 돌고 있지 않은지 확인한 뒤: UPDATE BATCH_STEP_EXECUTION SET STATUS='FAILED', EXIT_CODE='FAILED', END_TIME=NOW(6) WHERE JOB_EXECUTION_ID=? AND STATUS IN ('STARTING','STARTED'); UPDATE BATCH_JOB_EXECUTION SET STATUS='FAILED', EXIT_CODE='FAILED', END_TIME=NOW(6) WHERE JOB_EXECUTION_ID=?; → 다시 실행하면 멈춘 곳부터 |`, `| 503 BANK_MAINTENANCE | 지금 시각(KST), bank.maintenance.* | 점검 시간이다. Retry-After 초 뒤 다시. 점검 중 접수된 건은 없다(웹이 거절), 점검 전 접수된 건은 끝나면 재시도 · 재발행으로 처리된다 |`. 기존 `잔고와 원장 합이 다름` · `CONFLICT 경고` 행의 "(P5 대사)" 를 "(일마감 대사가 매일 BALANCE_LEDGER_MISMATCH 로 알린다)" · "(일마감 대사가 다음 날 REFUNDED_BUT_PAID 등으로 알린다)" 로.
  - 새 절 `## 9. 일마감 대사 · 점검 시간 · 알림 (P5-1)` (§8 뒤): (1) 점검 시간 — 언제 · 무엇이 막히나(웹 503 세 곳, 은행 송신 보류 = "연결 없음" 과 같은 길), 점검 길이 한도 26분, 점검 시간에도 조회 · 충전 · 관리 API 는 된다 (2) 대사 실행 — 자동(01 ~ 06시 매시, 전날), 수동 `curl -X POST "http://localhost:8080/admin/reconciliations?date=2026-10-28"` → `{status, reason, summary}`, 응답별 의미(200 COMPLETED / 200 FAILED + reason / 409 두 가지 / 400), 다시 돌리면 멈춘 단계부터 (3) 대상 — `sending_at` 이 D일(KST)인 출금 + 파일 D 의 T 줄 중 다른 날 보낸 · 우리에게 없는 것, 자동 확정은 `UNKNOWN` · `SENDING` 만 (4) 이슈 처리 — 열린 이슈 쿼리 · `resolved_at` 남기기 (database.md §9), 확정된 건은 사람이 SQL 로 (5) 알림 — 언제 무엇이(완료 INFO/CRITICAL, 실패 CRITICAL, 06시 미완료, 기존 경고 3곳 WARN), 로그만 볼 때는 로거 `ALERT` 를 grep, 텔레그램 켜는 법은 [SETUP 2-6](SETUP.md#2-6-텔레그램-알림-선택-p5-1), 알림 실패는 대사에 영향이 없다 (6) 손으로 오늘을 마감하는 것은 손 확인용이다 — 그 뒤 출금은 오늘 파일에 없다.

- [ ] **Step 5: `SETUP.md`**: 2-2 의 테스트 수를 Step 9 결과로. 2-5 뒤에 새 절 (바깥 4중 펜스 안의 내용을 들여쓰기 없이):
  ````markdown
  ### 2-6. 텔레그램 알림 (선택, P5-1)
  없어도 된다 — 토큰 · chat id 가 없으면 알림은 로그(`ALERT` 로거)로만 남는다. 휴대폰으로 받고 싶을 때만 한다.
  **토큰은 비밀번호다. 저장소 · `.run/*.xml` · `application.yml` · 셸 기록 · 채팅 · 스크린샷 어디에도 남기지 않는다.**

  1. 텔레그램에서 `@BotFather` 와 대화 → `/newbot` → 이름 · 사용자명(`…_bot`)을 정하면 토큰(`123456789:AA…`)을 준다. 이 화면 밖으로 옮기지 않는다.
  2. 만든 봇과 대화를 열고 아무 메시지(`hi`)를 보낸다 — 봇은 먼저 말을 걸 수 없다.
  3. chat id 찾기 (Git Bash, 토큰이 셸 기록에 남지 않게 `read -s` 로 받는다):
     ```bash
     read -rs TG_TOKEN && echo   # 토큰을 붙여 넣고 Enter (화면에 보이지 않는다)
     curl -s "https://api.telegram.org/bot${TG_TOKEN}/getUpdates" | grep -o '"chat":{"id":[-0-9]*'
     unset TG_TOKEN
     ```
     `"chat":{"id":123456789` 의 숫자가 chat id 다 (비어 있으면 2번을 다시).
  4. Windows **사용자 환경 변수**에 넣는다: 시작 → "계정의 환경 변수 편집" → 새로 만들기 → `ALERT_TELEGRAM_BOT_TOKEN` = 토큰, `ALERT_TELEGRAM_CHAT_ID` = chat id. (파일이 아니라 사용자 프로필에 저장된다. `setx` 는 명령 기록에 토큰이 남으니 쓰지 않는다.)
  5. IntelliJ · 터미널을 다시 열고 API 를 띄운다 → 로그 `alerts go to telegram chat 123456789 (and the log)`. 실행 구성(`.run/*.xml`)의 환경 변수 칸에는 넣지 않는다 — 그 파일은 커밋된다.
  6. 확인: [`http/daily-recon.http`](http/daily-recon.http) 의 2) 대사를 돌리면 휴대폰에 요약이 온다.

  토큰이 새었다면 `@BotFather` → `/revoke` 로 바로 폐기하고 새 토큰으로 4번을 다시 한다. 끄려면 두 환경 변수를 지운다.
  ````
  3절 표에 행: `| 대사 파일 · 거래 기록이 apps/mock-bank/bank-files 처럼 모듈 폴더에 생김 | Gradle 태스크가 아닌 다른 방법(직접 java -jar 등)으로 띄웠다. 저장소 루트에서 실행하거나 --bank-files= / --daily-recon.file-dir= 로 같은 경로를 준다 |`.

- [ ] **Step 6: `ARCHITECTURE.md`**:
  - 머리 인용문 기준을 `P5-1 일마감 대사 · 점검 시간 · 알림 (2026-10-29, 리뷰 중)` 으로.
  - §1 그림의 api 서브그래프에 `RECON[일마감 대사 잡<br/>api/dailyrecon, 01~06시] -- "TransferStore (미확정만 확정)" --> REPO`, `RECON -- "RECON_yyyyMMdd.dat" --> FILES[(bank-files)]`, `BANK -- "00:05 일마감" --> FILES`, `RECON -. "알림" .-> TG[텔레그램 / 로그]` 네 줄. 그림 아래 bullet: "**은행 점검 시간**(23:50 ~ 00:10 KST)에는 은행을 건드리는 웹 요청이 503 이고 게이트웨이가 송신을 보류한다 — 날짜 경계를 은행 파일과 맞춘다 (ADR-0010)".
  - 소스 지도에 `dailyrecon/` (`DailyRecon.kt` 이름 · 설정, `DailyReconJobConfiguration.kt` 잡 · 단계, `DailyReconSteps.kt` 태스클릿 · 리더 · 판정 쓰기, `ReconRepository.kt` 대사 SQL, `ReconSummary.kt` 요약 · 완료 알림, `DailyReconLauncher.kt` 실행 · 동시 실행 가드, `DailyReconScheduler.kt` 매시 회차), `alert/` (`Alert.kt` 포트 · 본문, `TelegramNotifier.kt`, `LogNotifier.kt`, `AlertConfiguration.kt`), `web/BankMaintenanceGuard.kt` · `web/ReconciliationController.kt`, `modules/transfer/StatementMatching.kt`, `modules/bank-gateway/BankMaintenanceWindow.kt`, `modules/bank-protocol/ReconFile.kt`, `apps/mock-bank/BankJournal.kt` · `MockBankOptions.kt`.
  - §6 진행 상황 끝(§7 앞)에 소절 `### P5-1 일마감 대사 흐름`:
  ```mermaid
  flowchart TB
      MB["모의 은행: 처리할 때마다 journal/yyyyMMdd.log"] -- "00:05 (KST)" --> F["recon/RECON_D.dat<br/>H · D… · T"]
      S["스케줄 01~06시 매시 / POST /admin/reconciliations"] --> L["DailyReconLauncher<br/>날짜마다 잡 인스턴스 하나"]
      L --> V["① validateFile<br/>없으면 FILE_NOT_READY · 틀리면 FILE_INVALID"]
      F --> V
      V --> LD["② loadStatement (500)<br/>bank_statement_line"]
      LD --> M3["③ matchTransfers (100, READ COMMITTED)<br/>sending_at = D 인 출금"]
      M3 --> M4["④ matchBankLines (100)<br/>파일 D 의 T 줄 중 ③ 밖"]
      M4 --> C5["⑤ checkLedger<br/>잔고 ≠ 원장 합"]
      C5 --> R6["⑥ report → AlertNotifier"]
      M3 -- "UNKNOWN · SENDING 만" --> X["COMPLETED(0000) / fail(E002) + 환불"]
      M3 -- "그 밖 어긋남" --> I[("recon_issue")]
      M4 --> I
      C5 --> I
  ```
    그 아래 bullet: 판정은 `StatementMatcher`(순수 함수) · 확정은 `StatementSettler`(기존 조건부 전이 · `fail`), 실패하면 멈춘 단계 · 청크부터 다시(결과 같음), 점검 시간 덕분에 "D일 송신" = "파일 D", 근거 ADR-0010.
  - §8 색인에 `| [0010](docs/adr/0010-daily-reconciliation.md) | 일마감 대사는 은행 대사 파일과 맞추고 결과를 모르는 출금만 자동 확정, 점검 시간으로 날짜 경계, API 앱 안의 Spring Batch |`.

- [ ] **Step 7: 손 확인 자료**
  - `.run/6. API (대사 손 확인).run.xml` (신규 — 결과를 모르는 출금을 정리 작업이 확정하지 않고 남겨 두게):
  ```xml
  <component name="ProjectRunConfigurationManager">
    <configuration default="false" name="6. API (대사 손 확인)" type="GradleRunConfiguration" factoryName="Gradle">
      <ExternalSystemSettings>
        <option name="executionName" />
        <option name="externalProjectPath" value="$PROJECT_DIR$" />
        <option name="externalSystemIdString" value="GRADLE" />
        <option name="scriptParameters" value="--args=&quot;--transfer.reconcile.enabled=false&quot;" />
        <option name="taskDescriptions">
          <list />
        </option>
        <option name="taskNames">
          <list>
            <option value=":apps:api:bootRun" />
          </list>
        </option>
        <option name="vmOptions" />
      </ExternalSystemSettings>
      <ExternalSystemDebugServerProcess>true</ExternalSystemDebugServerProcess>
      <ExternalSystemReattachDebugProcess>true</ExternalSystemReattachDebugProcess>
      <DebugAllEnabled>false</DebugAllEnabled>
      <RunAsTest>false</RunAsTest>
      <method v="2" />
    </configuration>
  </component>
  ```
  - `http/daily-recon.http` (신규):
  ```http
  ### IntelliJ HTTP Client 로 일마감 대사 · 점검 시간 확인 (P5-1)
  ### 준비: infra up, 모의 은행은 "1. Mock Bank (9090)" (기록 · 대사 파일 = 저장소 루트 bank-files/).
  ### 출금은 user-1 이 1원인증한 계좌로 — account-verification.http 1)·2), 충전은 transfer.http 0).

  ### ============================================================
  ### 1) 점검 시간 — 지금 시각을 포함하는 점검으로 API 를 띄운다 (예: 지금이 14:20 이면)
  ###    ./gradlew :apps:api:bootRun --args="--bank.maintenance.start=14:00 --bank.maintenance.end=14:40"
  ### 1-1) 출금 접수 → 503 BANK_MAINTENANCE, 응답 헤더 Retry-After = 점검 끝까지 남은 초
  POST http://localhost:8080/api/v1/transfers
  Content-Type: application/json
  X-User-Id: user-1
  Idempotency-Key: {{$random.uuid}}

  {
    "bankCode": "090",
    "accountNo": "1002123456789",
    "amount": 1000,
    "memo": "점검"
  }

  ### 1-2) 잔고 조회는 그대로 200
  GET http://localhost:8080/api/v1/wallet
  X-User-Id: user-1

  ### ============================================================
  ### 2) 대사 — API 를 실행 구성 "6. API (대사 손 확인)" 으로 띄운다 (정리 작업이 꺼져 결과를 모르는 출금이 그대로 남는다)
  ###    9990000000002(처리 후 무응답)는 1원인증으로 user-1 계좌로 만든다 (1원 이체는 정상 응답 — OPERATION §3)
  ### 2-1) 정상 출금 → 몇 초 뒤 COMPLETED
  POST http://localhost:8080/api/v1/transfers
  Content-Type: application/json
  X-User-Id: user-1
  Idempotency-Key: {{$random.uuid}}

  {
    "bankCode": "090",
    "accountNo": "1002123456789",
    "amount": 1000,
    "memo": "대사정상"
  }

  ### 2-2) 처리 후 무응답 계좌 → UNKNOWN 으로 남는다 (은행은 처리했다 → 대사가 자동 완료)
  POST http://localhost:8080/api/v1/transfers
  Content-Type: application/json
  X-User-Id: user-1
  Idempotency-Key: {{$random.uuid}}

  {
    "bankCode": "090",
    "accountNo": "9990000000002",
    "amount": 2000,
    "memo": "대사무응답"
  }

  ### 2-3) 모의 은행에서 오늘 파일을 만든다 (Git Bash, 저장소 루트): ./gradlew :apps:mock-bank:run --args="--close-day=$(date +%Y%m%d)"
  ###      → 콘솔 "Recon file for …: …\bank-files\recon\RECON_오늘.dat". 오늘을 미리 마감하는 것은 손 확인용이다 (그 뒤 출금은 이 파일에 없다)
  ### 2-4) 대사 실행 (date = 오늘, KST) → 200 {"status":"COMPLETED", "summary":{…"autoCompleted":1…}}. 봇을 설정했으면 휴대폰으로 요약
  POST http://localhost:8080/admin/reconciliations?date={{$date 'YYYY-MM-DD'}}

  ### 2-5) 다시 보내면 409 RECON_ALREADY_COMPLETED
  POST http://localhost:8080/admin/reconciliations?date={{$date 'YYYY-MM-DD'}}

  ### 2-6) DB 확인 (MySQL): 2-2 의 출금이 COMPLETED(0000), 이슈 AUTO_COMPLETED 한 줄
  ###      SELECT status, bank_response_code FROM transfer WHERE memo = '대사무응답' ORDER BY created_at DESC LIMIT 1;
  ###      SELECT type, reference, our_value, bank_value FROM recon_issue WHERE recon_date = CURDATE();
  ```
  (`{{$date 'YYYY-MM-DD'}}` 는 IntelliJ HTTP Client 의 동적 변수다 — PC 시각 기준이라 PC 시간대가 KST 여야 한다. 아니면 날짜를 직접 쓴다.)
  - `postman/collections/bank-transfer/6-daily-recon/.resources/definition.yaml`:
  ```yaml
  $kind: collection
  name: 6. 일마감 대사 · 점검 시간 (P5-1)
  order: 6000
  ```
  - `6-daily-recon/01-maintenance-transfer.request.yaml`:
  ```yaml
  $kind: http-request
  name: 6-1 (수동) 점검 시간 출금 → 503 BANK_MAINTENANCE + Retry-After
  method: POST
  url: '{{baseUrl}}/api/v1/transfers'
  order: 1000
  description: |-
    수동 확인 전용 — 컬렉션 변수 manualMaintenance 가 true 일 때만 나간다 (평소 시각에 보내면 202 가 나 잔고가 빠진다).
    준비: 지금 시각을 포함하는 점검으로 API 를 띄운다 — ./gradlew :apps:api:bootRun --args="--bank.maintenance.start=14:00 --bank.maintenance.end=14:40"
    (http/daily-recon.http 1-1). 끝나면 manualMaintenance 를 비운다.
  headers:
    - key: Content-Type
      value: application/json
    - key: X-User-Id
      value: '{{userId}}'
    - key: Idempotency-Key
      value: '{{$guid}}'
  body:
    type: json
    content: |-
      {
        "bankCode": "090",
        "accountNo": "{{accountNo}}",
        "amount": 1000,
        "memo": "점검"
      }
  scripts:
    - type: beforeRequest
      language: text/javascript
      code: |-
        if (pm.variables.get("manualMaintenance") !== "true") {
          pm.execution.skipRequest();
        }
    - type: afterResponse
      language: text/javascript
      code: |-
        pm.test("503 BANK_MAINTENANCE", () => {
          pm.response.to.have.status(503);
          pm.expect(pm.response.json().code).to.eql("BANK_MAINTENANCE");
        });
        pm.test("Retry-After = 점검 끝까지 남은 초 (양수)", () => {
          pm.expect(Number(pm.response.headers.get("Retry-After"))).to.be.above(0);
        });
  ```
  - `6-daily-recon/02-maintenance-wallet.request.yaml`:
  ```yaml
  $kind: http-request
  name: 6-2 점검 시간에도 잔고 조회는 200
  method: GET
  url: '{{baseUrl}}/api/v1/wallet'
  order: 2000
  headers:
    - key: X-User-Id
      value: '{{userId}}'
  scripts:
    - type: afterResponse
      language: text/javascript
      code: |-
        pm.test("200 (조회는 은행을 건드리지 않는다)", () => {
          pm.response.to.have.status(200);
        });
  ```
  - `6-daily-recon/03-run-recon.request.yaml`:
  ```yaml
  $kind: http-request
  name: 6-3 (수동) 오늘 대사 실행 → 200 COMPLETED + 요약
  method: POST
  url: '{{baseUrl}}/admin/reconciliations?date={{reconDate}}'
  order: 3000
  description: |-
    수동 확인 전용 — manualRecon 이 true 일 때만 나간다. 준비: 실행 구성 "6. API (대사 손 확인)", 출금(정상 · 9990000000002) 뒤
    ./gradlew :apps:mock-bank:run --args="--close-day=오늘(yyyyMMdd)" 로 오늘 파일을 만든다 (http/daily-recon.http 2).
    reconDate 가 비어 있으면 오늘(KST)을 넣는다.
  scripts:
    - type: beforeRequest
      language: text/javascript
      code: |-
        if (pm.variables.get("manualRecon") !== "true") {
          pm.execution.skipRequest();
        }
        if (!pm.collectionVariables.get("reconDate")) {
          pm.collectionVariables.set("reconDate", new Date(Date.now() + 9 * 3600 * 1000).toISOString().slice(0, 10));
        }
    - type: afterResponse
      language: text/javascript
      code: |-
        const body = pm.response.json();
        pm.test("200 COMPLETED", () => {
          pm.response.to.have.status(200);
          pm.expect(body.status).to.eql("COMPLETED");
        });
        console.log("summary = " + JSON.stringify(body.summary));
  ```
  - `6-daily-recon/04-run-again.request.yaml`:
  ```yaml
  $kind: http-request
  name: 6-4 (수동) 같은 날짜 다시 → 409 RECON_ALREADY_COMPLETED
  method: POST
  url: '{{baseUrl}}/admin/reconciliations?date={{reconDate}}'
  order: 4000
  scripts:
    - type: beforeRequest
      language: text/javascript
      code: |-
        if (pm.variables.get("manualRecon") !== "true") {
          pm.execution.skipRequest();
        }
    - type: afterResponse
      language: text/javascript
      code: |-
        pm.test("409 RECON_ALREADY_COMPLETED (끝난 날짜는 다시 돌리지 않는다)", () => {
          pm.response.to.have.status(409);
          pm.expect(pm.response.json().code).to.eql("RECON_ALREADY_COMPLETED");
        });
  ```
  - `6-daily-recon/05-tomorrow.request.yaml`:
  ```yaml
  $kind: http-request
  name: 6-5 내일 날짜 → 400 RECON_DATE_IN_FUTURE
  method: POST
  url: '{{baseUrl}}/admin/reconciliations?date={{tomorrow}}'
  order: 5000
  scripts:
    - type: beforeRequest
      language: text/javascript
      code: |-
        pm.collectionVariables.set("tomorrow", new Date(Date.now() + 33 * 3600 * 1000).toISOString().slice(0, 10));   // KST 내일
    - type: afterResponse
      language: text/javascript
      code: |-
        pm.test("400 RECON_DATE_IN_FUTURE (오늘까지만)", () => {
          pm.response.to.have.status(400);
          pm.expect(pm.response.json().code).to.eql("RECON_DATE_IN_FUTURE");
        });
  ```
  - `postman/collections/bank-transfer/.resources/definition.yaml`: description 의 `7) (P4-2) …` 줄 뒤에 줄 추가: `  8) (P5-1) "6. 일마감 대사 · 점검 시간" 폴더: 6-2 · 6-5 는 언제나 안전. 6-1 은 점검 시간으로 띄운 API 에서 manualMaintenance=true 로, 6-3 · 6-4 는 실행 구성 "6. API (대사 손 확인)" + 모의 은행 --close-day=오늘 뒤 manualRecon=true 로 (끝나면 비운다). 대응: http/daily-recon.http.` variables 에 `manualMaintenance: ""`, `manualRecon: ""`, `reconDate: ""`, `tomorrow: ""` 추가.

- [ ] **Step 8: 나머지 문서**
  - `README.md`: "지금 되는 것" 의 P4-2 줄 아래에 `- **일마감 대사 · 은행 점검 시간 · 알림 (P5-1)**: 모의 은행이 매일 00:05 에 만드는 대사 파일(고정길이 EUC-KR, 규격 v0.3)과 하루치 출금을 Spring Batch 잡으로 맞춘다 — 결과를 모르는 출금(UNKNOWN · SENDING)만 자동 확정(완료 / FAILED E002 + 환불), 확정된 건의 어긋남(이중 지급 · 환불했는데 나감 등)은 기록 · 알림. 끝난 날짜 거부 · 실패하면 멈춘 곳부터 · 동시 실행 하나. 23:50 ~ 00:10 은행 점검 시간에는 출금 · 1원인증 503 + Retry-After. 알림은 텔레그램(선택) / 로그 ([ADR-0010](docs/adr/0010-daily-reconciliation.md))`. 테스트 수(지금 되는 것 · IntelliJ · 터미널)를 Step 9 결과로, 테스트 목록 괄호에 "대사 전 칸 · 재시작(이중 환불 없음) · 동시 실행 하나 · 점검 시간 503 · UTC 시계 → KST 판정" 추가. 로드맵 `- [ ] **P5** Spring Batch 일마감 대사 + 텔레그램 알림` 아래에 하위 항목 `  - [x] P5-1 일마감 대사 · 점검 시간 · 알림 → [ADR-0010](docs/adr/0010-daily-reconciliation.md)` 과 `  - [ ] P5-2 보존 배치 (발행된 아웃박스 · 만료된 1원인증 정리)`. 실행 절(터미널)에 `./gradlew :apps:mock-bank:run --args="--close-day=20261029"   # 서버 없이 그 날짜 대사 파일만` 한 줄. 기술 스택에 `Spring Batch 6 (일마감 대사)` 추가.
  - `CHANGELOG.md`: `## [P4-2] …` 위에 `## [P5-1] 일마감 대사 · 은행 점검 시간 · 알림 — 2026-10-29` — Added: 대사 파일 형식 v0.3(`ReconFileFormat`), 모의 은행 거래 기록 · 재기동 복원 · 00:05 일마감 · `--close-day` · `--bank-files`, `BankMaintenanceWindow` · `SendGate`, 웹 503 `BANK_MAINTENANCE` + `Retry-After`, Spring Batch 6 잡 `dailyReconciliationJob`(6단계) · `StatementMatcher` · `StatementSettler` · `E002`, Flyway V4(Batch 메타 · `bank_statement_line` · `recon_issue`), 스케줄 01 ~ 06시 · `POST /admin/reconciliations`(200 / 409 `RECON_ALREADY_COMPLETED` · `RECON_RUNNING` / 400 `RECON_DATE_IN_FUTURE`), `AlertNotifier` · `TelegramNotifier` · `LogNotifier`, 설정 `bank.maintenance.*` · `daily-recon.*` · `alert.telegram.*`, 실행 구성 `6. API (대사 손 확인)`, `http/daily-recon.http`, Postman 6 폴더, ADR-0010. Changed: `Clock.systemUTC()` 배선을 `Clock` 빈 하나로, 기존 경고 3곳(CONFLICT · DLT · 결과조회 한도)에 WARN 알림, Gradle `run` · `bootRun` 작업 디렉터리 = 저장소 루트, 스케줄 풀 2 → 3, `spring-boot-starter-batch-jdbc` 추가, 부하 측정 실행 구성에 `--daily-recon.schedule.enabled=false`.
  - `HANDOVER.md`: 마지막 갱신일 2026-10-29. 표의 `P5 ~ P9` 행 위에 `| P5-1 일마감 대사 | 🟡 리뷰 중 | ADR-0010 제안, 테스트 N개(Step 9). 대사 파일 v0.3 · 점검 시간 · Spring Batch 잡 · 알림 |` 행, `P5 ~ P9` 를 `P5-2 ~ P9` 로. §2 에 `### P5-1 설계 (합의된 것)` — [설계 문서](docs/superpowers/specs/2026-10-29-p5-1-daily-reconciliation-design.md) 링크와 스펙 §2 결정 요약 표 7줄(은행 쪽 기록 · 자동 보정 범위 · 실행 위치 · 중복 실행 · 재시작 · 날짜 경계 · 알림 · 이름). §3 다음 할 일: 1) P5-1 리뷰 · 손 확인(http/daily-recon.http 1 · 2, 봇이 있으면 휴대폰 요약) · ADR-0010 채택 2) velog `#9` (P5-1: 대사는 왜 미확정만 고치나 · 점검 시간으로 날짜 경계 맞추기) 3) P5-2 설계(보존 배치). 기존 2)(P5 설계)는 지운다. 새 소절 `### P5-1 범위 밖으로 남긴 것`: 보존 배치(발행된 `outbox_event`, 만료된 `account_verification`), 원장 파티셔닝 · 보관, 불일치 해결 API(지금은 SQL), 알림 재전송 큐, 관리 API 인증, 별도 배치 서버(P8), `transfer.sending_at` 인덱스. §4 에 `(P5-1)` 줄: 설계 §11 의 한계 다섯 개 + Spring Batch 메타 표 시각은 JVM 기본 시간대 + 앱이 대사 도중 죽으면 실행이 STARTED 로 남아 409 `RECON_RUNNING` 이 계속된다(OPERATION §6 의 SQL 로 FAILED 처리) + 텔레그램 호출이 컨슈머 · 정리 작업 스레드에서 최대 8초 걸릴 수 있다(CONFLICT · DLT · 한도 소진 때만). 기존 (P4-1) 줄의 "자동 보정하지 않는다 (P5 대사)" 를 "자동 보정하지 않는다 — 일마감 대사가 다음 날 기록 · 알린다 (P5-1)" 로, (P3-3a) 줄의 "(최종 안전망은 P5 대사)" 를 "(최종 안전망은 일마감 대사 — P5-1)" 로. §5 최근 배운 것에 "시각 판단은 Instant 를 업무 시간대(KST)로 바꿔서 한다 — `LocalTime.now(clock)` 은 시계의 시간대(서버는 UTC)를 쓴다", "Spring Batch 6 의 `chunk(size)` 는 트랜잭션 관리자를 넘기지 않으면 트랜잭션이 없다(Resourceless) — 단계마다 명시한다", "같은 트랜잭션 안의 '다시 읽기' 는 REPEATABLE READ 에서 첫 읽기의 스냅숏을 본다 — 조건부 UPDATE 0건 뒤 다시 판정하려면 READ COMMITTED", "YAML 1.1 은 `23:50` 을 60진수 정수로 읽는다 — 시각은 따옴표로" 네 줄.

- [ ] **Step 9: 전체 빌드 · 테스트 수** — `./gradlew build --rerun-tasks` 후 테스트 수:
```bash
grep -ho 'testsuite name="[^"]*" tests="[0-9]*"' apps/*/build/test-results/test/*.xml modules/*/build/test-results/test/*.xml \
  | awk -F'tests="' '{ split($2, a, "\""); s += a[1] } END { print s }'
```
  이 수를 README(세 곳) · SETUP 2-2 · HANDOVER(P5-1 행) 에 넣는다. (예상: 240 + 이 계획의 새 테스트 약 110 — 실제 값으로 쓴다.)

- [ ] **Step 10: 커밋**

```bash
git add docs http postman .run README.md SETUP.md CHANGELOG.md HANDOVER.md OPERATION.md ARCHITECTURE.md
git commit -m "docs(p5-1): ADR-0010 일마감 대사 · 규격 v0.3 대사 파일 · DB(V4 · 운영 쿼리) · 운영(점검 · 대사 · 알림 · 이슈 처리) · 손 확인

왜 미확정만 자동 확정하는지(확정된 건을 뒤집으면 보정이 새 사고), 왜 점검 시간으로 날짜 경계를 맞추는지,
왜 API 앱 안의 Spring Batch(JDBC 잡 저장소)인지를 남긴다. 텔레그램 봇은 토큰을 저장소 · 셸 기록에 남기지 않고 만든다 (SETUP 2-6).

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```
(push · PR 없음.)

---

## 스펙 대응표 (계획 자체 점검)

| 스펙 | 내용 | Task |
|---|---|---|
| §3-1 | 대사 파일 형식(H · D · T, AN/N, EUC-KR, 같은 길이), 처리한 이체만, V 는 건수만, 형식 코드는 bank-protocol | 1 (형식), 10 (V 건수 · T 만 대조) |
| §3-2 | 모의 은행 일자별 기록 · 기동 복원 · 00:05 일마감(빈 날 · 원자적 · 덮어쓰지 않음) · `--close-day` · `--bank-files` · `./bank-files` · `.gitignore` | 2 |
| §4 | 설정 3키 · 시작 포함 끝 제외 · 자정 넘김, `BankMaintenanceWindow(isActive, remaining)`, `Clock` 빈, 웹 503 `BANK_MAINTENANCE` + `Retry-After`(올림), 조회 · 충전 · 관리 그대로, 어댑터 "연결 없음"(이체 RECEIVED · 결과조회 세지 않음 · 1원인증도), 26분 > 20분 명시 | 3, 4, 5, 11 (관리 API 점검 중 테스트), 12 (OPERATION) |
| §5-1 | 6단계 · 청크 500/100 · `FILE_NOT_READY` / `FILE_INVALID` · 유니크 무시 · 모든 날짜의 줄 · ④ ③ 밖의 T 줄(다른 날 보낸 출금 같은 판정) · 순수 함수 판정 · 조건부 전이 · `fail(E002)` · 0건 다시 읽기 | 8, 9, 10 |
| §5-2 | 01 ~ 06시 매시 Asia/Seoul · `FILE_NOT_READY` 다음 정각 · 완료면 INFO · 06시 미완료 긴급 · 관리 API 200/409/409/400(오늘 허용) · 끝난 날짜 거부 · 이어서 · 하나만 · 어디서 다시 해도 같음 · 부하 측정 · 테스트 스케줄 끔 | 11 |
| §6 | 판정 표 1 ~ 11 + RECEIVED/없음, `PAID_TWICE`, V 줄, `E002` | 8 (표), 10 (전 칸) |
| §7 | V4: Batch 6 MySQL 스키마 그대로 · `initialize-schema=never` · `bank_statement_line`(PK · 인덱스) · `recon_issue`(유니크 `ref_key`) · append-only | 9 |
| §8 | `AlertNotifier`, Telegram(둘 다 있을 때)/Log, 설정 · 환경변수, `POST …/sendMessage` 일반 텍스트, 3초/5초, 실패 삼킴, 가림 · 사용자 id 없음 · 20건 · 4,096자, 완료 INFO/CRITICAL, 실패 CRITICAL(FILE_NOT_READY 는 06시만), 기존 경고 3곳 WARN | 6, 7, 10 (완료), 11 (실패) |
| §9 | 설정 키 · 기본값 전부 | 4 (`bank.maintenance`), 6 (`alert.telegram`), 9 (`spring.batch.*`), 10 (`file-dir` · `chunk-size`), 11 (`schedule.*`) |
| §10 단위 | `StatementMatcher` 표 + 변이 검사(커밋 본문), `BankMaintenanceWindow`(포함 · 제외 · 자정 · 올림 · 꺼짐 · UTC 시계 → KST), 파일 형식(왕복 · 건수 · 합계 · 날짜 · 길이), `TelegramNotifier`(가짜 서버 · 가림 · 자르기 · 500 · 시간 초과) | 8, 3, 1, 6 |
| §10 통합 | 전 칸 · 재실행 거부 · 재시작(③ 중간 청크, 이중 환불 없음) · 동시 실행 · 경합 · `FILE_NOT_READY` → 재시작 · 점검 시간(503 + 조회 · 충전 200, 흐름 테스트) · 모의 은행(재기동 · `--close-day`) · 테스트마다 다른 대사일 | 10, 11, 5, 4, 2, 9 (`ReconTestDates`) |
| §10 손 확인 | Postman 6 폴더 · `http/daily-recon.http` (점검 · 대사 · 409 · 오늘 마감 주의) | 12 |
| §11 | 위험과 한계 | 12 (ADR · HANDOVER · OPERATION) |
| §12 | ADR-0010, protocol v0.3, database.md, OPERATION, SETUP(텔레그램 · 환경변수), ARCHITECTURE, README, HANDOVER, CHANGELOG, Postman 6 폴더 (velog #9 는 HANDOVER 의 다음 할 일) | 12 |
