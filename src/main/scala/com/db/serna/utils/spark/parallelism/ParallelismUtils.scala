package com.db.serna.utils.spark.parallelism

import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

object ParallelismUtils {

  private val log = Logger.getLogger(this.getClass.getName)

  /**
   * * Calculates estimated main.scala.com.db.serna.utils.spark.parallelism for a Spark cluster.
   *
   * @param tasksPerCore
   *   Number of tasks per core (default: 5)
   * @param fallback
   *   Value to use if calculation fails (default: 50)
   * @implict
   *   spark The implicit SparkSession
   * @return
   *   Estimated cluster main.scala.com.db.serna.utils.spark.parallelism as Int
   */
  def getClusterParallelismNum(tasksPerCore: Int = 5, fallback: Int = 50)(implicit spark: SparkSession): Int = {
    Try {
      val executorCount = math.max(1, currentExecutorCount(spark))
      val coresPerExecutor = spark.conf.get("spark.executor.cores").toInt

      val totalParallelism = executorCount * coresPerExecutor * tasksPerCore
      log.info(s"Executors: $executorCount, Cores per executor: $coresPerExecutor, Tasks/core: $tasksPerCore")
      log.info(s"Calculated cluster main.scala.com.db.serna.utils.spark.parallelism: $totalParallelism")
      totalParallelism
    } match {
      case Success(parallelismNum) => parallelismNum
      case Failure(e) =>
        log.info(
          s"Failed to calculate cluster main.scala.com.db.serna.utils.spark.parallelism, defaulting to $fallback. Error: ${e.getMessage}"
        )
        fallback
    }
  }

  /**
   * Repartitions or coalesces a DataFrame based on the provided number of partitions and optional single partition
   * column.
   *
   *   - If a valid partition column is provided and exists in the DataFrame, repartitions by that column.
   *   - If no partition column is provided and reducing the number of partitions, uses coalesce for efficiency.
   *   - If no partition column is provided and increasing the number of partitions, uses repartition.
   *   - If the current number of partitions matches the requested number, returns the DataFrame unchanged.
   *
   * @param df
   *   The input DataFrame to repartition or coalesce.
   * @param numPartitions
   *   The desired number of partitions in the output DataFrame.
   * @param partitionColumn
   *   Optional column name to repartition by. Default is empty string (no column).
   * @return
   *   DataFrame with the adjusted number of partitions.
   */
  def rebalancePartitions(df: DataFrame, numPartitions: Int, partitionColumn: String = ""): DataFrame = {
    val initialPartitions = df.rdd.getNumPartitions

    (partitionColumn.trim, df.columns.contains(partitionColumn.trim), initialPartitions.compare(numPartitions)) match {
      case (colName, true, cmp) if colName.nonEmpty && cmp != 0 =>
        log.info(s"Repartitioning DataFrame from $initialPartitions to $numPartitions partitions by column '$colName'")
        df.repartition(numPartitions, col(colName))
      case (_, _, 1) =>
        log.info(s"Coalescing DataFrame from $initialPartitions to $numPartitions partitions")
        df.coalesce(numPartitions)
      case (_, _, -1) =>
        log.info(s"Repartitioning DataFrame from $initialPartitions to $numPartitions partitions")
        df.repartition(numPartitions)
      case _ =>
        log.info(s"Partition count matches requested ($numPartitions), skipping repartition.")
        df
    }
  }

  /**
   * Repartitions or coalesces a DataFrame using zero, one, or many partition columns.
   *
   * Logic:
   *   - Collect only valid columns from partitionColumns.
   *   - If at least one valid column and current != target: df.repartition(numPartitions, validCols...)
   *   - Else fallback to same rules as single-column version (coalesce when shrinking, repartition when growing).
   *   - If current == target: return df unchanged.
   *
   * @param df
   *   Input DataFrame.
   * @param numPartitions
   *   Target number of partitions.
   * @param partitionColumns
   *   Columns to use for partitioning (may contain invalid or empty strings).
   * @return
   *   Repartitioned or coalesced DataFrame.
   */
  def rebalancePartitions(df: DataFrame, numPartitions: Int, partitionColumns: Seq[String]): DataFrame = {
    val initialPartitions = df.rdd.getNumPartitions
    val sanitized = partitionColumns.filter(_.trim.nonEmpty).map(_.trim)
    val validCols = sanitized.filter(df.columns.contains)

    val cmp = initialPartitions.compare(numPartitions) // -1 growing, 0 same, 1 shrinking

    if (cmp == 0) {
      log.info(s"Partition count matches requested ($numPartitions), skipping repartition (multi-col path).")
      df
    } else if (validCols.nonEmpty) {
      log.info(
        s"Repartitioning DataFrame from $initialPartitions to $numPartitions partitions by columns ${validCols.mkString("[", ", ", "]")}"
      )
      df.repartition(numPartitions, validCols.map(col): _*)
    } else {
      // No valid columns found; fallback to original heuristic
      if (cmp == 1) {
        log.info(
          s"Coalescing DataFrame from $initialPartitions to $numPartitions partitions (no valid partition columns)."
        )
        df.coalesce(numPartitions)
      } else {
        log.info(
          s"Repartitioning DataFrame from $initialPartitions to $numPartitions partitions (no valid partition columns)."
        )
        df.repartition(numPartitions)
      }
    }
  }

  /**
   * Gets the current number of executors in the Spark cluster, excluding the driver.
   * https://github.com/apache/spark/blob/v3.5.7/core/src/main/scala/org/apache/spark/SparkStatusTracker.scala#L116
   */
  def currentExecutorCount(spark: SparkSession): Int = spark.sparkContext.statusTracker.getExecutorInfos.length - 1

  def coresPerExecutor(spark: SparkSession): Int =
    spark.conf.getOption("spark.executor.cores").map(_.toInt).getOrElse(1)

  def totalExecutorCores(spark: SparkSession): Int = currentExecutorCount(spark) * coresPerExecutor(spark)

  // A heuristic: partitions = total cores * factor (factor 2–4 for CPU-bound, 1–2 for IO-heavy)
  def recommendedPartitions(spark: SparkSession, factor: Int = 5): Int = {
    val totalCores = totalExecutorCores(spark)
    log.info(s"totalCores=$totalCores")
    // Fallback: if dynamic allocation has not yet ramped, at least defaultParallelism
    math.max(spark.sparkContext.defaultParallelism, totalCores * factor)
  }

  def adaptiveRepartitionDF(
      spark: SparkSession,
      df: DataFrame,
      factor: Int = 2
  ): DataFrame = {
    val targetPartitionNo: Int = recommendedPartitions(spark, factor)
    log.info(
      s"Repartitioning DF to $targetPartitionNo partitions (executors=${currentExecutorCount(spark)}, cores=${totalExecutorCores(spark)})."
    )
    rebalancePartitions(df, targetPartitionNo)
  }

  def adaptiveRepartitionRDD[T: ClassTag](
      spark: SparkSession,
      rdd: RDD[T],
      factor: Int = 2,
      minGrowthPct: Int = 25
  ): RDD[T] = {
    val target = recommendedPartitions(spark, factor)
    val current = rdd.getNumPartitions
    val growthPct = if (current == 0) 100 else ((target - current) * 100.0 / current).toInt
    if (target > current && growthPct >= minGrowthPct) {
      log.info(
        s"Repartitioning from $current to $target partitions (executors=${currentExecutorCount(spark)}, cores=${totalExecutorCores(spark)})."
      )
      rdd.repartition(target)
    } else {
      log.info(s"No repartition: current=$current target=$target growthPct=$growthPct%.")
      rdd
    }
  }
}
