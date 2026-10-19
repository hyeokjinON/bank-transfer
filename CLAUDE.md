# CLAUDE.md — 이 저장소에서 작업하는 AI 에이전트(와 사람)를 위한 안내

> 처음 보는 사람은 이 순서로 읽는다: **README → [ARCHITECTURE](ARCHITECTURE.md) → [HANDOVER](HANDOVER.md)**.
> 개발 환경은 [SETUP](SETUP.md), 실행·장애 대응은 [OPERATION](OPERATION.md), 변경 이력은 [CHANGELOG](CHANGELOG.md).

## 프로젝트 한 줄
은행 FEP 와 고정길이 전문(TCP)으로 통신하는 송금·계좌인증 서비스를 Kotlin · Spring Boot 4 · Netty 4.2 로 단계별(P0~P9) 재구현한다.
예전에 운영한 입출금 미들웨어(Java 8, Boot 1.5)의 문제를 "지금이라면 어떻게 푸는가"로 검증하는 학습·포트폴리오 저장소다.

## 절대 규칙
- **예전 회사 코드·설정·식별자를 한 줄도 넣지 않는다.** 전문 규격·은행은 모두 이 저장소에서 새로 정의한 모의(Mock)다 ([docs/protocol.md](docs/protocol.md)).
- 비밀번호·키·실제 IP·계정을 커밋하지 않는다. 로컬 값은 `infra/.env` (git 제외), 예시는 `infra/.env.example`.
- 도메인 모듈(`modules/account`, 앞으로 `modules/transfer`)은 **Spring · Netty · JPA · Kafka 를 의존하지 않는다.** 외부와는 포트(인터페이스)로만 말하고, 어댑터는 `apps/api` 에서 조립한다 ([ADR-0001](docs/adr/0001-modular-monolith-ports.md)).

## 작업 방식
1. **설계는 사람이 정한다.** 선택지가 있으면 장단점과 추천을 제시하고 고르게 한다. 정한 내용은 `docs/adr/NNNN-*.md` 로 남긴다 (상태: 제안 → 머지 시 채택).
2. 구현은 **단계별 브랜치**(`feat/p3-1-...`, `docs/...`, `fix/...`) → 테스트 → PR. `main` 에 직접 커밋하지 않는다.
3. PR 본문: 무엇을 / 왜(리뷰어가 설명할 수 있어야 하는 것) / 검증 / 리뷰 체크리스트. 머지는 사람이 한다. 자동 머지를 켜지 않는다.
4. 단계가 끝나면 [CHANGELOG](CHANGELOG.md) 와 [HANDOVER](HANDOVER.md) 를 갱신한다. README 로드맵 체크도 같이.
5. 커밋 메시지: `type(scope): 한국어 요약` (`feat`, `fix`, `infra`, `docs`, `ci`, `chore`, `test`). AI 가 만든 커밋은 끝에 `Co-Authored-By:` 줄을 붙인다.

## 자주 쓰는 명령
```bash
./gradlew build                                   # 컴파일 + 전체 테스트
./gradlew :modules:account:test                   # 모듈 하나만
./gradlew :apps:mock-bank:run --args="9090"       # 모의 은행
./gradlew :apps:api:bootRun                       # API (8080)
docker compose -f infra/docker-compose.yml --env-file infra/.env up -d --wait   # MySQL·Kafka·Redis
bash infra/verify.sh                              # 인프라 읽기/쓰기 확인
```
- Windows 에서 셸 스크립트는 **Git Bash** 로 실행한다. PowerShell 의 `bash` 는 WSL 실행기라 동작하지 않는다 ([OPERATION](OPERATION.md#자주-막히는-곳)).
- `*.sh`, `*.yml`, `gradlew` 는 LF 고정(`.gitattributes`). 설정 파일을 고칠 때는 **먼저 읽고 합친다** (덮어쓰기 금지).

## 코드 규칙
- Kotlin 2.3, Java 21 toolchain, 패키지 루트 `io.github.hyeokjinon.banktransfer.<모듈>`.
- 주석·문서·커밋은 한국어. 주석은 "무엇"보다 **"왜"** 를 쓴다. 주변 코드의 밀도와 말투를 따른다.
- 비동기 결과는 `CompletableFuture` (P6 이후 코루틴 비교). 예외는 `CompletionException` 을 벗겨서 처리한다.
- 금액은 `Long`(원 단위), 시간은 `Instant` + 주입한 `Clock`. 테스트에서 시간을 고정할 수 있어야 한다.
- 새 기능은 테스트 먼저(TDD). 도메인은 가짜 포트로 단위 테스트, 어댑터는 Testcontainers/모의 은행으로 통합 테스트.
- 버전은 정확히 고정한다 (`latest` 금지, 예외: 선택 도구인 kafka-ui).

## 문서 위치
| 문서 | 내용 |
|---|---|
| [ARCHITECTURE.md](ARCHITECTURE.md) | 모듈 구조, 의존 방향, 런타임 흐름, 설계 결정 색인 |
| [SETUP.md](SETUP.md) | 처음 개발 환경 만들기 (JDK, IntelliJ, Docker, Git Bash) |
| [OPERATION.md](OPERATION.md) | 실행, 포트, 상태 확인, 모의 은행 시나리오, 장애 대응 |
| [HANDOVER.md](HANDOVER.md) | 지금 어디까지 왔고 다음에 무엇을 하는지 (세션마다 갱신) |
| [CHANGELOG.md](CHANGELOG.md) | 단계별 변경 이력 |
| [docs/adr/](docs/adr) | 설계 결정 기록 |
| [docs/protocol.md](docs/protocol.md) | 모의 은행 전문 규격 |
| [infra/README.md](infra/README.md) | 로컬 인프라 상세 |
