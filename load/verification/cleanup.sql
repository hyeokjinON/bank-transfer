-- P6-2 1원인증 측정 데이터 지우기 (측정 사용자 100명만: p62-u001 ~ p62-u100). load/verification/run.sh 가 회차 앞에 부르고, 측정이 끝나면 한 번 더 부른다.
-- 사용자 조건: 접두사(LIKE)로 인덱스 범위를 줄이고 REGEXP 가 정확한 100명만 남긴다 (load/consumer/cleanup.sql 과 같은 방식 — 다른 행은 지우지 않는다).
-- 1원인증만 만든다 (지갑 · 출금 · 아웃박스 없음).
DELETE FROM account_verification
 WHERE user_id LIKE 'p62-u%' AND user_id REGEXP '^p62-u(00[1-9]|0[1-9][0-9]|100)$';
