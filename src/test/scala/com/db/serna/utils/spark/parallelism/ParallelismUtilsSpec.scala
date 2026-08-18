package com.db.serna.utils.spark.parallelism

import com.db.serna.utils.spark.{SparkTestSession, TestSparkSessionSupport}
import org.apache.spark.sql.DataFrame
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class ParallelismUtilsSpec extends AnyFunSuite with Matchers with SparkTestSession {

  private def sampleDF(initialPartitions: Int = 2): DataFrame = {
    val ssStable = spark;
    import ssStable.implicits._

    val data = (1 to 100).map(i => (i, s"cat_${i % 5}", s"group_${i % 3}"))
    val base = data.toDF("id", "category", "grp")
    base.repartition(initialPartitions)
  }

  test("getClusterParallelismNum should correctly calculate parallelism or use fallback") {
    val conf = TestSparkSessionSupport.MinimalLocalConf + ("spark.executor.cores" -> "8")
    TestSparkSessionSupport.withSession(conf) { spark =>
      val result1 = ParallelismUtils.getClusterParallelismNum()(spark)
      println(s"[PUS] result1=$result1 (expected 1 * 8 * 5 = 40)")
      result1 shouldBe 40

      val result2 = ParallelismUtils.getClusterParallelismNum(3)(spark)
      println(s"[PUS] result2=$result2 (expected 1 * 8 * 3 = 24)")
      result2 shouldBe 24
    }
  }

  test("rebalancePartitions should repartition by partitionColumn when partitions need changing") {
    TestSparkSessionSupport.withSession { spark =>
      val ssStable = spark;
      import ssStable.implicits._

      val testDF: DataFrame = Seq(("A", 1), ("B", 2), ("C", 3), ("D", 4)).toDF("col1", "col2")
      val result = ParallelismUtils.rebalancePartitions(testDF, 3, "col1")
      println(s"[PUS] repartition by col1 -> numPartitions=${result.rdd.getNumPartitions}")
      assert(result.rdd.getNumPartitions == 3)
    }
  }

  test("rebalancePartitions should reducing partitions (coalesce) when no partition column") {
    val ssStable = spark;
    import ssStable.implicits._

    val df = Seq(("A", 1), ("B", 2), ("C", 3), ("D", 4)).toDF("col1", "col2").repartition(4)
    val result = ParallelismUtils.rebalancePartitions(df, 2)
    println(s"[PUS] coalesce -> numPartitions=${result.rdd.getNumPartitions}")
    assert(result.rdd.getNumPartitions == 2)
  }

  test("rebalancePartitions should increasing partitions (repartition) when no partition column") {
    val ssStable = spark;
    import ssStable.implicits._

    val df = Seq(("A", 1), ("B", 2), ("C", 3), ("D", 4)).toDF("col1", "col2").repartition(2)
    val result = ParallelismUtils.rebalancePartitions(df, 4)
    println(s"[PUS] repartition grow -> numPartitions=${result.rdd.getNumPartitions}")
    assert(result.rdd.getNumPartitions == 4)
  }

  test("rebalancePartitions should skip when initial partition = desired partition") {
    val ssStable = spark;
    import ssStable.implicits._

    val df = Seq(("A", 1), ("B", 2), ("C", 3), ("D", 4)).toDF("col1", "col2").repartition(2)
    val result = ParallelismUtils.rebalancePartitions(df, 2)
    println(s"[PUS] no-op -> numPartitions=${result.rdd.getNumPartitions}")
    assert(result.rdd.getNumPartitions == 2)
    assert(result.collect().sameElements(df.collect()))
  }

  test("rebalancePartitions multi-column repartition increases partitions") {
    val df = sampleDF(initialPartitions = 2)
    val result = ParallelismUtils.rebalancePartitions(df, numPartitions = 5, partitionColumns = Seq("category", "grp"))
    println(s"[PUS] multi repartition -> numPartitions=${result.rdd.getNumPartitions}")
    assert(result.rdd.getNumPartitions === 5)
  }

  test("rebalancePartitions multi-column ignores invalid columns but uses valid ones") {
    val df = sampleDF(initialPartitions = 2)
    val result = ParallelismUtils.rebalancePartitions(
      df,
      numPartitions = 6,
      partitionColumns = Seq("does_not_exist", "grp", "also_missing")
    )
    println(s"[PUS] multi with invalid cols -> numPartitions=${result.rdd.getNumPartitions}")
    assert(result.rdd.getNumPartitions === 6)
  }

  test("rebalancePartitions multi-column fallback when no valid columns -> repartition for growth") {
    val df = sampleDF(initialPartitions = 2)
    val result = ParallelismUtils.rebalancePartitions(df, numPartitions = 4, partitionColumns = Seq("nope1", "nope2"))
    println(s"[PUS] multi fallback grow -> numPartitions=${result.rdd.getNumPartitions}")
    assert(result.rdd.getNumPartitions === 4)
  }

  test("rebalancePartitions multi-column fallback to coalesce when shrinking and no valid columns") {
    val df = sampleDF(initialPartitions = 4)
    val result = ParallelismUtils.rebalancePartitions(df, numPartitions = 2, partitionColumns = Seq("bad_col"))
    println(s"[PUS] multi fallback shrink -> numPartitions=${result.rdd.getNumPartitions}")
    assert(result.rdd.getNumPartitions === 2)
  }

  test("rebalancePartitions multi-column no-op when partition count matches") {
    val df = sampleDF(initialPartitions = 3)
    val same = ParallelismUtils.rebalancePartitions(df, numPartitions = 3, partitionColumns = Seq("category", "grp"))
    println(s"[PUS] multi no-op -> rdd.id=${same.rdd.id}, original.id=${df.rdd.id}")
    assert(same.rdd.id === df.rdd.id)
  }

  test("original single-column method still works for valid column") {
    val df = sampleDF(initialPartitions = 2)
    val result = ParallelismUtils.rebalancePartitions(df, numPartitions = 5, partitionColumn = "category")
    println(s"[PUS] single valid col -> numPartitions=${result.rdd.getNumPartitions}")
    assert(result.rdd.getNumPartitions === 5)
  }

  test("original single-column method coalesce when shrinking without column") {
    val df = sampleDF(initialPartitions = 5)
    val result = ParallelismUtils.rebalancePartitions(df, numPartitions = 2)
    println(s"[PUS] single shrink without col -> numPartitions=${result.rdd.getNumPartitions}")
    assert(result.rdd.getNumPartitions === 2)
  }

  test("original single-column method no-op when counts match") {
    val df = sampleDF(initialPartitions = 4)
    val same = ParallelismUtils.rebalancePartitions(df, numPartitions = 4)
    println(s"[PUS] single no-op -> rdd.id=${same.rdd.id}, original.id=${df.rdd.id}")
    assert(same.rdd.id === df.rdd.id)
  }
}
