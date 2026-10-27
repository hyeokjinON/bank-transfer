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
./gradlew build      # 첫 실행은 의존성 다운로드로 몇 분 걸린다. 테스트 239개 통과해야 정상
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

## 3. Windows 에서 알아둘 것
| 상황 | 해결 |
|---|---|
| PowerShell 에서 `bash infra/verify.sh` → `execvpe(/bin/bash) failed` | PowerShell 의 `bash` 는 WSL 실행기다. Git Bash 로: `& "C:\Program Files\Git\bin\bash.exe" infra/verify.sh` |
| Git Bash 에서 `docker compose exec kafka /opt/...` 가 파일 없음 | Git Bash 의 경로 자동 변환. `MSYS_NO_PATHCONV=1` 을 앞에 붙인다 |
| 콘솔 한글 깨짐 | 실행 구성·Gradle 태스크에 `-Dstdout.encoding=UTF-8` 이 이미 들어 있다. 직접 실행할 때도 같은 옵션 |
| `curl` 로 한글 JSON 이 깨짐 | 본문을 파일로 만들고 `--data-binary @req.json` |
| 포트 3306·6379 사용 중 | `infra/.env` 에서 `MYSQL_PORT=3307` 처럼 변경 |

## 4. 다 됐는지 확인
- [ ] `./gradlew build` 통과
- [ ] `0. Mock Bank + API` 실행 후 `GET /actuator/health` → `UP`
- [ ] 1원인증 요청 → 모의 은행 콘솔의 적요 코드로 confirm → `VERIFIED`
- [ ] `bash infra/verify.sh` → "모두 정상입니다."
- [ ] `http/transfer.http` 1) → 202, 2-1) 조회 → 몇 초 뒤 `COMPLETED` (아웃박스 → Kafka → 모의 은행), API 재시작 후 조회 → 같은 건 (DB 에 남음)

다음: 실행·장애 대응은 [OPERATION.md](OPERATION.md), 구조는 [ARCHITECTURE.md](ARCHITECTURE.md).
