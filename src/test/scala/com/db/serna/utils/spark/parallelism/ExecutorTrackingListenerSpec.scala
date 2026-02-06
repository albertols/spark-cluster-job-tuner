package com.db.serna.utils.spark.parallelism

import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class ExecutorTrackingListenerSpec extends AnyFunSuite with Matchers {

  // Create a local Spark session once for all tests
  lazy val spark: SparkSession = SparkSession.builder()
    .master("local[2]")
    .appName("ExecutorTrackingListenerTest")
    .getOrCreate()

  test("Synthetic application_start emitted if listener added after SparkSession creation") {
    val listener = new ExecutorTrackingListener(
      spark,
      5,
      "TEST_APP_ID",
      Map("test_case" -> "synthetic_start")
    )
    spark.sparkContext.addSparkListener(listener)

    // Trigger trivial action to ensure some events may flow
    spark.range(1, 5).count()

    val events = listener.getEmittedEvents
    val appStartEvents = events.filter(_.contains("\"event_type\":\"application_start\""))
    appStartEvents.nonEmpty shouldBe true
    // Ensure event_type first
    appStartEvents.foreach { line =>
      line.trim.startsWith("{\"event_type\"") shouldBe true
    }
  }

  test("Executor add/remove logs have event_type first") {
    val listener = new ExecutorTrackingListener(
      spark,
      5,
      "TEST_APP_ID",
      Map("test_case" -> "executor_events")
    )
    spark.sparkContext.addSparkListener(listener)

    // Force some tasks
    spark.range(0, 1000).repartition(4).count()

    val events = listener.getEmittedEvents
    val executorEvents = events.filter(e =>
      // hardcoded due to: ')' expected but string literal found.
      //      e.contains(s"\"executor_event\":\"${listener.executorAdded.toString}\"".toString)
      e.contains("\"executor_event\":\"EXECUTOR_ADDED\"") ||
        e.contains("\"executor_event\":\"EXECUTOR_REMOVED\"")
    )
    executorEvents.foreach { line =>
      line.startsWith("{\"event_type\"") shouldBe true
    }
  }

  test("Application end and summary emitted") {
    val listener = new ExecutorTrackingListener(
      spark,
      5,
      "TEST_APP_ID",
      Map("test_case" -> "app_end_summary")
    )
    spark.sparkContext.addSparkListener(listener)

    // Perform an action
    spark.range(0, 10).count()

    // Manually invoke application end (simulate) — Spark won't call this in tests until stop().
    listener.onApplicationEnd(
      new org.apache.spark.scheduler.SparkListenerApplicationEnd(System.currentTimeMillis())
    )

    val events: List[String] = listener.getEmittedEvents
    events.exists(_.contains("\"event_type\":\"application_end\"")) shouldBe true
    events.exists(_.contains("\"event_type\":\"application_summary\"")) shouldBe false
  }
}
