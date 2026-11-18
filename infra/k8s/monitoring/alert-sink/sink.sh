#!/bin/sh
# 웹훅 본문(JSON 한 줄)을 stderr → 컨테이너 로그. 한 번의 write 로 찍는다 (동시 수신 때 다른 본문과 섞이지 않게). 응답은 200 빈 본문
body=$(head -c "${CONTENT_LENGTH:-0}")
printf '%s\n' "$body" >&2
printf 'Content-Type: text/plain\r\n\r\n'
