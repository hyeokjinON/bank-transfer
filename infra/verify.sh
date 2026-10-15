#!/usr/bin/env bash
# 로컬 인프라가 "실제로 쓸 수 있는 상태"인지 확인한다 (healthcheck 보다 한 단계 더).
#   MySQL : 앱 계정으로 접속 → 테이블 생성 · 삽입 · 조회
#   Kafka : 토픽 생성 → 메시지 1건 보내기 → 처음부터 읽어 같은 값인지 확인
#   Redis : SET · GET · TTL
# 사용: bash infra/verify.sh   (Git Bash · WSL · macOS · Linux · GitHub Actions)
set -euo pipefail

cd "$(dirname "$0")"
ENV_FILE=.env
[ -f "$ENV_FILE" ] || ENV_FILE=.env.example
set -a; . "./$ENV_FILE"; set +a
dc() { docker compose -f docker-compose.yml --env-file "$ENV_FILE" "$@"; }
ok() { printf '  \033[32mOK\033[0m %s\n' "$1"; }

echo "[1/3] MySQL"
dc exec -T mysql mysql -u"$MYSQL_USER" -p"$MYSQL_PASSWORD" "$MYSQL_DATABASE" -N -e "
  CREATE TABLE IF NOT EXISTS infra_check (id INT PRIMARY KEY, note VARCHAR(20));
  REPLACE INTO infra_check VALUES (1, '한글확인');
  SELECT note FROM infra_check WHERE id = 1;" 2>/dev/null | grep -q '한글확인'
ok "앱 계정 접속 · 쓰기 · 읽기 · utf8mb4 한글"

echo "[2/3] Kafka (KRaft)"
TOPIC=infra.check
dc exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --if-not-exists --topic "$TOPIC" --partitions 1 --replication-factor 1 >/dev/null
MSG="ping-$(date +%s)"
echo "$MSG" | dc exec -T kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic "$TOPIC" >/dev/null
# 소비자는 시간 초과로 끝나면 0 이 아닌 코드로 끝나므로, 출력만 받아서 확인한다
OUT=$(dc exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic "$TOPIC" \
  --from-beginning --timeout-ms 10000 2>/dev/null || true)
grep -q "$MSG" <<<"$OUT"
ok "토픽 생성 · 메시지 보내기 · 처음부터 읽기"

echo "[3/3] Redis"
dc exec -T redis redis-cli SET infra:check ok EX 60 >/dev/null
[ "$(dc exec -T redis redis-cli GET infra:check | tr -d '\r')" = "ok" ]
TTL=$(dc exec -T redis redis-cli TTL infra:check | tr -d '\r')
[ "$TTL" -gt 0 ]
ok "SET · GET · 만료시간(TTL ${TTL}s)"

echo "모두 정상입니다."
