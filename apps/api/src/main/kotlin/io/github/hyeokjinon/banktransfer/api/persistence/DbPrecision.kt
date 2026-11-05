package io.github.hyeokjinon.banktransfer.api.persistence

import java.time.Instant
import java.time.temporal.ChronoUnit

/** DATETIME(6) 은 마이크로초까지만 담는다. 저장 전에 맞춰 잘라야 저장한 값과 다시 읽은 값이 같다. (1원인증 저장소는 account-infra 에 같은 함수가 있다) */
internal fun Instant.toDbPrecision(): Instant = truncatedTo(ChronoUnit.MICROS)
