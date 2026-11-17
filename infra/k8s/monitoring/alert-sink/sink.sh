#!/bin/sh
# 웹훅 본문(JSON 한 줄)을 그대로 stderr → 컨테이너 로그. 응답은 200 빈 본문
head -c "${CONTENT_LENGTH:-0}" >&2; echo >&2
printf 'Content-Type: text/plain\r\n\r\n'
