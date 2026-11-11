# syntax=docker/dockerfile:1.7
# 앱 이미지 세 개를 한 파일에서 만든다 (P8-2, ADR-0016). 줄마다 이유는 ADR-0016 "이미지" 절.
#   docker build --target api          -t bank-transfer/api:dev .
#   docker build --target bank-gateway -t bank-transfer/bank-gateway:dev .
#   docker build --target mock-bank    -t bank-transfer/mock-bank:dev .
# 보통은 Compose 가 부른다: docker compose -f infra/docker-compose.yml --env-file infra/.env --profile app build

# ---- 1. 빌드: JDK + Gradle 로 세 앱을 한 번에. 이 단계(JDK · Gradle · 소스)는 최종 이미지에 들어가지 않는다 ----
FROM eclipse-temurin:21-jdk AS build
WORKDIR /src
# 덜 바뀌는 것 먼저: 래퍼 · 빌드 스크립트가 그대로면 Gradle 배포판을 내려받는 이 층을 다시 쓴다.
# sh 로 부른다 — Windows 에서 보낸 빌드 컨텍스트의 실행 비트에 기대지 않게
COPY gradlew settings.gradle.kts build.gradle.kts ./
COPY gradle ./gradle
RUN sh ./gradlew --version --no-daemon
COPY . .
# 의존성 캐시는 캐시 마운트(이미지에 안 들어감)에. /root/.gradle 전체가 아니라 caches 만 — 위 층의 래퍼 배포판(/root/.gradle/wrapper)을 가리지 않게.
# 테스트는 CI build 잡이 돌린다 (-x test). Kotlin 컴파일은 Gradle 프로세스 안에서 (컴파일 데몬을 따로 띄우지 않게)
RUN --mount=type=cache,id=gradle-caches,target=/root/.gradle/caches,sharing=locked \
    sh ./gradlew :apps:api:bootJar :apps:bank-gateway:bootJar :apps:mock-bank:installDist \
      -x test --no-daemon -Pkotlin.compiler.execution.strategy=in-process
# Boot jar 를 레이어로 푼다 → dependencies · spring-boot-loader · snapshot-dependencies(지금은 비어 있음) · application.
# 이름을 정확히 쓴다 — 같은 폴더의 -plain.jar(라이브러리용)와 섞이지 않게
RUN java -Djarmode=tools -jar apps/api/build/libs/api-0.1.0-SNAPSHOT.jar extract --layers --launcher --destination /out/api \
 && java -Djarmode=tools -jar apps/bank-gateway/build/libs/bank-gateway-0.1.0-SNAPSHOT.jar extract --layers --launcher --destination /out/bank-gateway \
 && cp -r apps/mock-bank/build/install/mock-bank /out/mock-bank

# ---- 2. 실행 공통: JRE 만 (Alpine — 작고 헬스체크용 wget · nc 가 busybox 에 있다), root 아닌 사용자 ----
FROM eclipse-temurin:21-jre-alpine AS runtime-base
# /data/bank-files: 모의 은행이 쓰고 api 가 읽는 대사 파일 자리. 이미지에서 app 소유로 만들어 두면 처음 붙는 이름 있는 볼륨이 이 소유자를 물려받는다
RUN addgroup -S app && adduser -S app -G app \
 && mkdir -p /data/bank-files && chown -R app:app /data
# 로그 · Spring Batch 메타 시각 · 모의 은행 파일 날짜가 KST (JVM 이 TZ 를 읽는다 — 앱 코드는 시간대를 명시해 이 값에 기대지 않는다)
ENV TZ=Asia/Seoul
WORKDIR /app
USER app

# ---- 3-1. api: 덜 바뀌는 층부터 — 코드 한 줄을 고치면 맨 위 application 층만 다시 만든다 ----
FROM runtime-base AS api
COPY --from=build /out/api/dependencies/ ./
COPY --from=build /out/api/spring-boot-loader/ ./
COPY --from=build /out/api/snapshot-dependencies/ ./
COPY --from=build /out/api/application/ ./
EXPOSE 8080
# exec 형식: PID 1 = java → docker stop 의 SIGTERM 이 바로 가 graceful shutdown. 힙은 컨테이너 메모리 한도의 75% (나머지는 스택 · 메타스페이스 · 네이티브).
# OOM 이면 반쯤 죽은 채 버티지 않고 끝낸다 → restart 정책이 새로 띄운다
ENTRYPOINT ["java", "-XX:MaxRAMPercentage=75", "-XX:+ExitOnOutOfMemoryError", "-Dstdout.encoding=UTF-8", "-Dstderr.encoding=UTF-8", "org.springframework.boot.loader.launch.JarLauncher"]

# ---- 3-2. bank-gateway: api 와 같은 모양 ----
FROM runtime-base AS bank-gateway
COPY --from=build /out/bank-gateway/dependencies/ ./
COPY --from=build /out/bank-gateway/spring-boot-loader/ ./
COPY --from=build /out/bank-gateway/snapshot-dependencies/ ./
COPY --from=build /out/bank-gateway/application/ ./
EXPOSE 8082
ENTRYPOINT ["java", "-XX:MaxRAMPercentage=75", "-XX:+ExitOnOutOfMemoryError", "-Dstdout.encoding=UTF-8", "-Dstderr.encoding=UTF-8", "org.springframework.boot.loader.launch.JarLauncher"]

# ---- 3-3. mock-bank: Boot 앱이 아니다 — installDist 의 lib/ 를 클래스패스로 (lib/* 는 java 가 펼친다) ----
FROM runtime-base AS mock-bank
COPY --from=build /out/mock-bank/lib/ ./lib/
EXPOSE 9090
ENTRYPOINT ["java", "-XX:MaxRAMPercentage=75", "-XX:+ExitOnOutOfMemoryError", "-Dstdout.encoding=UTF-8", "-Dstderr.encoding=UTF-8", "-cp", "lib/*", "io.github.hyeokjinon.banktransfer.mockbank.MockBankServerKt"]
CMD ["9090", "--bank-files=/data/bank-files"]
