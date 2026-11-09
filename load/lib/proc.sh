# shellcheck shell=bash
# 측정 도구 공통 — 포트 · JVM PID (load/verification/run.sh · load/consumer/run.sh 가 source 한다, lib/vt.sh 도 쓴다).
# 이 PC 의 netstat 은 0.0.0.0 에 묶인 대기 포트를 보여 주지 않아 쓰지 않는다 (P7 에서 확인).
port_busy() { (exec 3<>"/dev/tcp/127.0.0.1/$1") 2>/dev/null; }   # 그 포트에 접속되면 누군가 쥐고 있다
# jcmd -l 에서 고정 문자열 $1 을 명령행에 가진 JVM 의 PID (없으면 빈 출력). 앱은 "--server.port=포트", 모의 은행은 "MockBankServerKt 포트"
jvm_pid() { "$(dirname "$(command -v java)")/jcmd" -l 2>/dev/null | tr -d '\r' | grep -a -F -- "$1" | awk '{ print $1 }' | head -1; }
# 포트 $1 을 쥔 측정 JVM(앱 · 모의 은행)의 PID
port_pid() { local p; p=$(jvm_pid "--server.port=$1"); [ -n "$p" ] || p=$(jvm_pid "MockBankServerKt $1"); echo "$p"; }
