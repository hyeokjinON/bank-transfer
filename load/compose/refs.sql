-- 측정 출금의 거래번호 (은행 거래 기록과 맞춰 본다 — 하나도 빠지지 않고 두 번 나오지 않아야 한다)
SELECT reference FROM transfer
 WHERE user_id LIKE 'load-p82-%' AND user_id REGEXP '^load-p82-u0(0[1-9]|1[0-9]|20)$';
