package com.db.serna.utils.spark.cache

import org.scalatest.funsuite.AnyFunSuite

class MemorySizingUtilsFormattingSpec extends AnyFunSuite {

  test("bytesToMB performs integer division and floors the result") {
    val cases = Seq(
      0L -> 0L,
      1L -> 0L,
      1024L -> 0L,                    // < 1 MB
      (512L * 1024L) -> 0L,           // 512 KB => 0 MB
      (1024L * 1024L) -> 1L,          // 1 MB
      (1536L * 1024L) -> 1L,          // 1.5 MB => floors to 1 MB
      (2L * 1024L * 1024L) -> 2L      // 2 MB
    )

    cases.foreach { case (bytes, expectedMB) =>
      val actual = MemorySizingUtils.bytesToMB(bytes)
      println(s"[MSU-Format] bytesToMB: bytes=$bytes => MB=$actual (expected=$expectedMB)")
      assert(actual === expectedMB)
    }
  }

  test("fmtBytes formats bytes into human-readable units (base 1024) truncated to 2 decimals (no rounding)") {
    val cases = Seq(
      -1L -> "0 B",
      0L -> "0 B",
      1L -> "1.00 B",
      1023L -> "1023.00 B",
      1024L -> "1.00 KB",
      1536L -> "1.50 KB",                           // exact 1.5 KB
      (1024L * 1024L) -> "1.00 MB",
      (1536L * 1024L) -> "1.50 MB",                 // exact 1.5 MB
      (1024L * 1024L * 1024L) -> "1.00 GB",
      (1536L * 1024L * 1024L) -> "1.50 GB",         // exact 1.5 GB
      (1024L * 1024L * 1024L * 1024L) -> "1.00 TB",
      (1536L * 1024L * 1024L * 1024L) -> "1.50 TB", // exact 1.5 TB
      2047L -> "1.99 KB"                            // no rounding up; truncated from ~1.999 KB
    )

    cases.foreach { case (bytes, expectedStr) =>
      val actual = MemorySizingUtils.fmtBytes(bytes)
      println(s"[MSU-Format] fmtBytes: bytes=$bytes => '$actual' (expected='$expectedStr')")
      assert(actual === expectedStr)
    }
  }

  test("fmtBytes boundary behavior without rounding") {
    val kbMinusOne = 1024L - 1                    // 1023 B -> stays in B
    val kbExact = 1024L                           // 1.00 KB
    val kbAlmostTwo = 2047L                       // ~1.999 KB -> 1.99 KB (truncate)

    val mbMinusOne = (1024L * 1024L) - 1          // 1 MB - 1B -> 1023.99 KB (truncate)
    val mbExact = 1024L * 1024L                   // 1.00 MB
    val gbMinusOne = (1024L * 1024L * 1024L) - 1  // -> 1023.99 MB (truncate)
    val gbExact = 1024L * 1024L * 1024L           // 1.00 GB

    val cases = Seq(
      kbMinusOne -> "1023.00 B",
      kbExact -> "1.00 KB",
      kbAlmostTwo -> "1.99 KB",
      mbMinusOne -> "1023.99 KB",  // updated: truncation, not rounding
      mbExact -> "1.00 MB",
      gbMinusOne -> "1023.99 MB",  // updated: truncation, not rounding
      gbExact -> "1.00 GB"
    )

    cases.foreach { case (bytes, expected) =>
      val actual = MemorySizingUtils.fmtBytes(bytes)
      println(s"[MSU-Format] boundary fmtBytes: bytes=$bytes => '$actual' (expected='$expected')")
      assert(actual === expected)
    }
  }

  test("fmtBytes output always includes two decimal places for non-zero values (without rounding)") {
    val values = Seq(1L, 10L, 1024L, 10 * 1024L, 1024L * 1024L, 10L * 1024L * 1024L)
    values.foreach { b =>
      val s = MemorySizingUtils.fmtBytes(b)
      println(s"[MSU-Format] two-decimals check: bytes=$b => '$s'")
      if (b > 0) {
        val parts = s.split(" ")
        assert(parts.length == 2, "Expected '<value> <unit>' format")
        val value = parts(0)
        assert(value.matches("""\d+\.\d{2}"""), s"Expected two decimal places, got '$value'")
      } else {
        assert(s == "0 B")
      }
    }
  }

  test("fmtBytes does not round up at .999 thresholds (uses largest unit)") {
    val almostTwoKB = 2047L // ~1.999 KB
    val s1 = MemorySizingUtils.fmtBytes(almostTwoKB)
    println(s"[MSU-Format] no-rounding: 2047 => '$s1'")
    assert(s1 == "1.99 KB")

    val almostTwoMB = (2L * 1024L * 1024L) - (1L * 1024L) // 2MB - 1KB ≈ 1.999 MB
    val s2 = MemorySizingUtils.fmtBytes(almostTwoMB)
    println(s"[MSU-Format] no-rounding: (2MB-1KB) => '$s2'")
    assert(s2 == "1.99 MB") // updated: use MB unit with truncation
  }
}