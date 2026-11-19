package io.github.hyeokjinon.banktransfer.api.alert

/**
 * 앱 알림의 **로그 사본**에서만 금액 · 잔고를 가린다 (P9c, 스펙 F6 ① — 앱이 금액을 남기지 않는다). 텔레그램 본문은 그대로 — 운영자가 조치에 쓴다.
 * 알림 본문의 금액에는 '원' 이 붙지 않아 Alloy 의 `***원` 규칙에도 걸리지 않는다 → 앱에서 가린다.
 *
 * 가리는 것: `amount=` · `balance=` · `ledgerSum=` 뒤 숫자(음수 포함), 일마감 대사 항목(`우리=… 은행=…`)의 값 안에 따로 선 숫자(= 금액).
 * 남기는 것 (찾기 키): 거래번호 · 출금 id · `issue #id` · 날짜 · 은행 줄 번호(`2026-10-28#3`) · 은행코드-계좌(계좌는 [AlertText] 가 이미 가렸다) · 건수.
 */
internal object AlertLogMask {
    private const val HIDDEN = "***"

    /** 이름 붙은 금액 · 잔고 (give-up 알림의 `amount=`, 잔고 원장 불일치의 `balance=` · `ledgerSum=`). */
    private val LABELED = Regex("""\b(amount|balance|ledgerSum)=-?\d[\d,]*""")

    /** 대사 항목에서 값이 시작하는 곳 (`ReconIssueRow.alertLine` — `<종류> <거래번호 | 지갑(issue #id)> 우리=<값> 은행=<값>`). */
    private const val RECON_VALUES = "우리="

    /**
     * 값 안의 따로 선 숫자: 앞은 공백 · `=`, 뒤는 공백 · `;` · 줄 끝 · `…`(4096자 자르기).
     * our_value `<상태> <금액> <은행코드>-<계좌>`, bank_value `<금액> <은행코드>-<계좌> <날짜>#<줄>; …` 에서 금액만 걸린다 —
     * 은행코드 · 날짜 · 줄 번호는 `-` · `#` 에 붙어 있다.
     */
    private val STANDALONE_NUMBER = Regex("""(?<=[\s=])-?\d[\d,]*(?=$|[\s;…])""")

    fun mask(text: String): String = text.split('\n').joinToString("\n") { maskLine(it) }

    private fun maskLine(line: String): String {
        val labeled = LABELED.replace(line) { "${it.groupValues[1]}=$HIDDEN" }
        val start = labeled.indexOf(RECON_VALUES)
        if (start < 0) return labeled
        return labeled.substring(0, start) + STANDALONE_NUMBER.replace(labeled.substring(start), HIDDEN)
    }
}
