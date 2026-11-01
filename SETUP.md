# SETUP — 처음 개발 환경 만들기

> 목표: 새 PC 에서 30분 안에 `./gradlew build` 통과 + 1원인증 API 호출 + 인프라 `verify.sh` 통과.
> 기준 환경: Windows 11. macOS·Linux 는 Docker Desktop/Engine 만 설치하면 나머지는 같다.

## 1. 필요한 것
| 도구 | 버전 | 용도 | 확인 |
|---|---|---|---|
| JDK | **21** (Temurin 권장) | 빌드·실행 (Gradle toolchain 21) | `java -version` |
| Git for Windows | 최신 | 소스 + **Git Bash** (셸 스크립트 실행) | `git --version` |
| IntelliJ IDEA | 2025.x 이상 | 개발, `.run/` 실행 구성, HTTP Client | |
| Docker Desktop | 4.x (WSL2 엔진) | MySQL · Kafka · Redis (P3 부터) | `docker version` (Client·Server 둘 다) |

Gradle 은 설치하지 않는다. 저장소의 래퍼(`./gradlew`)가 맞는 버전(9.x)을 내려받는다.

## 2. 순서
### 2-1. 소스
```bash
git clone https://github.com/hyeokjinON/bank-transfer.git
cd bank-transfer
```
체크아웃 후 `gradlew`·`*.sh` 가 LF 인지 신경 쓸 필요는 없다 (`.gitattributes` 가 고정).

### 2-2. 빌드 · 테스트
```bash
./gradlew build      # 첫 실행은 의존성 다운로드로 몇 분 걸린다. 테스트 370개 통과해야 정상
```
테스트는 Testcontainers 로 MySQL · Kafka · Redis 를 띄우므로 Docker Desktop 이 켜져 있어야 한다 (설치는 2-4).

### 2-3. IntelliJ
1. 폴더 열기 → Gradle 프로젝트로 가져오기.
2. `Settings > Build, Execution, Deployment > Build Tools > Gradle > Gradle JVM` = 21, `File > Project Structure > SDK` = 21.
3. (권장) `Settings > Tools > Terminal > Shell path` = `C:\Program Files\Git\bin\bash.exe` → 터미널에서 `bash infra/verify.sh` 가 그대로 동작.
3-1. 인프라 먼저: `docker compose -f infra/docker-compose.yml --env-file infra/.env up -d --wait` (API 는 MySQL 과 Kafka 를 쓴다)
4. 실행 목록의 `0. Mock Bank + API` 실행 → `http/account-verification.http` 의 ▶ 로 요청 (Ultimate).
   Community Edition 은 `.http` 실행이 없다 → Postman 에서 저장소 폴더를 열면 `postman/collections/bank-transfer` 컬렉션이 보인다 (요청마다 Test Results 로 통과/실패 표시).

### 2-4. Docker Desktop (P3 부터 필요)
1. **관리자 PowerShell**: `wsl --install` → 재부팅.
2. Docker Desktop 설치 (WSL 2 엔진 체크) → 실행 → 엔진이 Running 이 될 때까지 대기.
3. 확인: `docker version`, `docker compose version`.
4. 인프라:
   ```bash
   cp infra/.env.example infra/.env
   docker compose -f infra/docker-compose.yml --env-file infra/.env up -d --wait
   bash infra/verify.sh        # → "모두 정상입니다."
   ```
5. (선택) 메모리 제한: `%UserProfile%\.wslconfig` 에 `[wsl2]` / `memory=4GB`.

### 2-5. 부하 측정 도구 (선택, P4-2)
설치할 것이 없다. k6 는 Docker 이미지(`grafana/k6:2.3.0`)로 돌고, 측정 스크립트는 `load/` 에 있다.
- 확인: `docker compose -f infra/docker-compose.yml --env-file infra/.env --profile load run --rm k6 version` → `k6 v2.3.0` (첫 실행은 이미지를 받는다).
- 실행은 [OPERATION §8](OPERATION.md#8-부하-측정-p4-2-k6).
- Windows: `load/run.sh` 는 Git Bash 로 실행한다 (PowerShell 의 `bash` 는 WSL 실행기다). Git Bash 의 경로 변환을 막는 `MSYS_NO_PATHCONV=1` 은 스크립트가 알아서 설정한다.

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
5. **IntelliJ 를 완전히 종료(File → Exit)했다가 다시 켜고** API 를 띄운다 → 로그 `alerts go to telegram chat 123456789 (and the log)`. 환경 변수는 프로그램이 시작될 때만 읽는다 — 창 · 프로젝트만 닫았다 열거나 실행 중이던 터미널에서 띄우면 예전 값(없음)이라 `alerts go to the log only` 가 나온다. 실행 구성(`.run/*.xml`)의 환경 변수 칸에는 넣지 않는다 — 그 파일은 커밋된다.
6. 확인: [`http/daily-recon.http`](http/daily-recon.http) 의 2) 대사를 돌리면 휴대폰에 요약이 온다. 알림은 대사가 끝나거나 실패했을 때 · `CONFLICT` · DLT · 결과조회 한도 소진 때만 간다 — API 기동, 409 `RECON_ALREADY_COMPLETED`, 400 `RECON_DATE_NOT_CLOSED` 로는 오지 않는다 (API 는 실행 구성 `6. API (대사 손 확인)` 으로 — 오늘 날짜 대사는 그 구성의 `daily-recon.allow-today=true` 일 때만 받는다).

토큰이 새었다면 `@BotFather` → `/revoke` 로 바로 폐기하고 새 토큰으로 4번을 다시 한다. 끄려면 두 환경 변수를 지운다.

## 3. Windows 에서 알아둘 것
| 상황 | 해결 |
|---|---|
| PowerShell 에서 `bash infra/verify.sh` → `execvpe(/bin/bash) failed` | PowerShell 의 `bash` 는 WSL 실행기다. Git Bash 로: `& "C:\Program Files\Git\bin\bash.exe" infra/verify.sh` |
| Git Bash 에서 `docker compose exec kafka /opt/...` 가 파일 없음 | Git Bash 의 경로 자동 변환. `MSYS_NO_PATHCONV=1` 을 앞에 붙인다 |
| 콘솔 한글 깨짐 | 실행 구성·Gradle 태스크에 `-Dstdout.encoding=UTF-8` 이 이미 들어 있다. 직접 실행할 때도 같은 옵션 |
| `curl` 로 한글 JSON 이 깨짐 | 본문을 파일로 만들고 `--data-binary @req.json` |
| 포트 3306·6379 사용 중 | `infra/.env` 에서 `MYSQL_PORT=3307` 처럼 변경 |
| 대사 파일 · 거래 기록이 `apps/mock-bank/bank-files` 처럼 모듈 폴더에 생김 | Gradle 태스크가 아닌 다른 방법(직접 `java -jar` 등)으로 띄웠다. 저장소 루트에서 실행하거나 `--bank-files=` / `--daily-recon.file-dir=` 로 같은 경로를 준다 |

## 4. 다 됐는지 확인
- [ ] `./gradlew build` 통과
- [ ] `0. Mock Bank + API` 실행 후 `GET /actuator/health` → `UP`
- [ ] 1원인증 요청 → 모의 은행 콘솔의 적요 코드로 confirm → `VERIFIED`
- [ ] `bash infra/verify.sh` → "모두 정상입니다."
- [ ] `http/transfer.http` 1) → 202, 2-1) 조회 → 몇 초 뒤 `COMPLETED` (아웃박스 → Kafka → 모의 은행), API 재시작 후 조회 → 같은 건 (DB 에 남음)

다음: 실행·장애 대응은 [OPERATION.md](OPERATION.md), 구조는 [ARCHITECTURE.md](ARCHITECTURE.md).
