package com.db.serna.utils.spark

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

/**
 * Shared SparkSession for tests in a suite.
 * - Creates SparkSession lazily when first used
 * - Stops it after all tests
 * - Clears active/default to avoid reuse of a stopped context
 */
trait SparkTestSession extends BeforeAndAfterAll { self: AnyFunSuite =>
  @transient private var _spark: SparkSession = _

  protected def spark: SparkSession = {
    if (_spark == null) {
      _spark = SparkSession.builder()
        .appName(self.getClass.getSimpleName.stripSuffix("$"))
        .master("local[2]")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
      SparkSession.setActiveSession(_spark)
      SparkSession.setDefaultSession(_spark)
    }
    _spark
  }

  override protected def afterAll(): Unit = {
    try {
      if (_spark != null) {
        _spark.stop()
        SparkSession.clearActiveSession()
        SparkSession.clearDefaultSession()
        System.clearProperty("spark.driver.port")
      }
    } finally {
      super.afterAll()
    }
  }
}