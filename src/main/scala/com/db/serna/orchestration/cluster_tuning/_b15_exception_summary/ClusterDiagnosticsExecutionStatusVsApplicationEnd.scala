package com.db.serna.orchestration.cluster_tuning._b15_exception_summary

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.slf4j.LoggerFactory

import java.io.{File, PrintWriter}

/**
 * Compares B15 application end execution status with `exception_summary_times.csv`
 * and generates categorized diagnostics, orphan reports, and a compact summary report.
 *
 * [[input]] B15 application-end CSV:
 * - `DefaultB15CsvPath`
 * - Can be overridden by passing a date argument like `2025_12_20`
 * - Can also be overridden explicitly by passing a full CSV path as the first positional argument
 *
 * [[input]] Exception summary CSV:
 * - `DefaultExceptionSummaryCsvPath`
 * - Can be overridden explicitly by passing a full CSV path as the second positional argument
 *
 * [[output]] Categorized CSV reports under `DefaultOutputDir`:
 * - `SuccessStatusVsSuccessSummaryFile`
 * - `SuccessStatusVsExceptionSummaryFile`
 * - `FailureStatusVsSuccessSummaryFile`
 * - `NullStatusVsSuccessSummaryFile`
 * - `NullStatusVsExceptionSummaryFile`
 * - `OrphanRecipeExistsInSummaryNotInB15File`
 * - `OrphanRecipeExistsInB15AndNotInSummaryFile`
 *
 * [[output]] Plain-text summary report:
 * - `FullStatusVsExceptionSummaryReportFile`
 *
 * Argument precedence:
 * 1. explicit B15 CSV path (arg 0, if it ends with `.csv`)
 * 2. date argument like `2025_12_20` for the B15 input folder
 * 3. `DefaultB15CsvPath`
 * 4. explicit exception-summary CSV path (next `.csv` arg if present)
 * 5. explicit output directory (next non-CSV arg if present)
 */
object ClusterDiagnosticsExecutionStatusVsApplicationEnd {

  private val logger = LoggerFactory.getLogger(getClass)

  val DefaultB15CsvPath: String = "src/main/resources/composer/dwh/config/cluster_tuning/inputs/2025_12_20/b15_application_end_with_recipe_null_status.csv"
  val DefaultExceptionSummaryCsvPath: String = "src/main/resources/xmltobq/blueprint_exceptions/exception_summary_times.csv"
  val DefaultOutputDir: String = "src/main/scala/com/db/serna/orchestration/cluster_tuning/_b15_exception_summary"

  val SuccessStatusVsSuccessSummaryFile: String = "B15_success_status_vs_success_summary.csv"
  val SuccessStatusVsExceptionSummaryFile: String = "B15_success_status_vs_exception_summary.csv"
  val FailureStatusVsSuccessSummaryFile: String = "B15_failure_status_vs_success_summary.csv"
  val NullStatusVsSuccessSummaryFile: String = "B15_null_status_vs_success_summary.csv"
  val NullStatusVsExceptionSummaryFile: String = "B15_null_status_vs_exception_summary.csv"
  val OrphanRecipeExistsInSummaryNotInB15File: String = "orphan_recipe_exists_in_summary_not_in_b15.csv"
  val OrphanRecipeExistsInB15AndNotInSummaryFile: String = "orphan_recipe_exists_in_b15_and_not_in_summary.csv"
  val FullStatusVsExceptionSummaryReportFile: String = "_B15_full_status_vs_exception_summary.txt"

  val SummarySuccess: String = "SUCCESS"
  val StatusSuccess: String = "SUCCESS"
  val StatusFailure: String = "FAILURE"

  final case class OutputBundleDfs(
                                    successStatusVsSuccessSummary: DataFrame,
                                    successStatusVsExceptionSummary: DataFrame,
                                    failureStatusVsSuccessSummary: DataFrame,
                                    nullStatusVsSuccessSummary: DataFrame,
                                    nullStatusVsExceptionSummary: DataFrame,
                                    orphanRecipeExistsInSummaryNotInB15: DataFrame,
                                    orphanRecipeExistsInB15AndNotInSummary: DataFrame,
                                    fullStatusVsExceptionSummaryReport: String
                                  )

  def main(args: Array[String]): Unit = {
    implicit val spark: SparkSession = SparkSession.builder()
      .master("local[2]")
      .appName("ClusterDiagnosticsExecutionStatusVsApplicationEnd")
      .getOrCreate()

    val dateArg = args.find(_.matches("\\d{4}_\\d{2}_\\d{2}"))
    val csvArgs = args.filter(_.toLowerCase.endsWith(".csv"))
    val nonCsvNonDateArgs = args.filterNot(a => a.matches("\\d{4}_\\d{2}_\\d{2}") || a.toLowerCase.endsWith(".csv"))

    val b15CsvPath = csvArgs.headOption
      .orElse(dateArg.map(defaultB15CsvPathForDate))
      .getOrElse(DefaultB15CsvPath)
    val exceptionCsvPath = csvArgs.lift(1).getOrElse(DefaultExceptionSummaryCsvPath)
    val outputDir = nonCsvNonDateArgs.headOption.getOrElse(DefaultOutputDir)

    logger.info(
      s"Starting ClusterDiagnosticsExecutionStatusVsApplicationEnd. " +
        s"dateArg=${dateArg.getOrElse("<none>")} " +
        s"b15CsvPath=$b15CsvPath exceptionCsvPath=$exceptionCsvPath outputDir=$outputDir"
    )

    val rawB15 = readB15Csv(b15CsvPath)
    val rawException = readExceptionSummaryCsv(exceptionCsvPath)

    logger.info(s"Loaded raw inputs. b15Rows=${rawB15.count()} exceptionSummaryRows=${rawException.count()}")

    val outputs: OutputBundleDfs = buildOutputs(rawB15, rawException)

    writeOutputs(outputs, outputDir)
    logger.info(s"Finished ClusterDiagnosticsExecutionStatusVsApplicationEnd. Outputs written under $outputDir")
    spark.stop()
  }

  /** Reads the B15 CSV preserving quoted multiline failure messages in `message`. */
  def readB15Csv(path: String)(implicit spark: SparkSession): DataFrame = {
    logger.info(s"Reading B15 application-end CSV from $path")
    spark.read
      .option("header", "true")
      .option("multiLine", "true")
      .option("escape", "\"")
      .option("quote", "\"")
      .csv(path)
  }

  /** Reads the exception summary CSV, which also contains quoted values and may span multiple lines. */
  def readExceptionSummaryCsv(path: String)(implicit spark: SparkSession): DataFrame = {
    logger.info(s"Reading exception summary CSV from $path")
    spark.read
      .option("header", "true")
      .option("multiLine", "true")
      .option("escape", "\"")
      .option("quote", "\"")
      .csv(path)
  }

  /** Normalizes the B15 input to the canonical columns used by the comparison outputs. */
  def normalizeB15(df: DataFrame): DataFrame = {
    df.select(
      trim(col("cluster_name")).as("cluster_name"),
      trim(col("recipe_filename")).as("recipe_filename"),
      trim(col("job_id")).as("job_id"),
      trim(col("app_start_iso")).as("app_start_iso"),
      trim(col("avg_job_duration_in_mins_sec")).as("avg_job_duration_in_mins_sec"),
      normalizeStatusColumn(col("status")).as("status"),
      trim(col("message")).as("message")
    )
  }

  /**
   * Normalizes the exception summary input and collapses historical duplicates,
   * keeping only the latest summary row per `recipe_filename` before joining.
   */
  def normalizeExceptionSummary(df: DataFrame): DataFrame = {
    val normalized = df.select(
      trim(col("RECIPE_FILE_PATH")).as("RECIPE_FILE_PATH"),
      trim(col("EXECUTION_DATE_TIME")).as("EXECUTION_DATE_TIME"),
      trim(col("EXECUTION_DURATION")).as("EXECUTION_DURATION"),
      normalizeSummaryColumn(col("EXCEPTIONS_SUMMARY")).as("EXCEPTIONS_SUMMARY")
    ).withColumn("recipe_filename", extractRecipeFilenameFromPathColumn(col("RECIPE_FILE_PATH")))

    latestExceptionSummaryPerRecipe(normalized)
  }

  /** Builds all categorized outputs, orphan reports, and the summary text report in one normalized pass. */
  def buildOutputs(b15Input: DataFrame, exceptionInput: DataFrame): OutputBundleDfs = {
    val rawB15Count = b15Input.count()
    val rawExceptionCount = exceptionInput.count()

    val b15 = normalizeB15(b15Input).cache()
    val exceptionSummary = normalizeExceptionSummary(exceptionInput).cache()

    logger.info(
      s"Building outputs from normalized inputs. normalizedB15Rows=${b15.count()} normalizedExceptionRows=${exceptionSummary.count()}"
    )

    val joined = b15.join(exceptionSummary, Seq("recipe_filename"), "inner")

    val baseColumns = Seq(
      col("cluster_name"),
      col("recipe_filename"),
      col("job_id"),
      col("app_start_iso"),
      col("avg_job_duration_in_mins_sec"),
      col("status"),
      col("message")
    )

    val successStatusVsSuccessSummary = joined
      .filter(isSuccessStatus(col("status")) && isSuccessSummary(col("EXCEPTIONS_SUMMARY")))
      .select(baseColumns: _*)

    val successStatusVsExceptionSummary = joined
      .filter(isSuccessStatus(col("status")) && isExceptionSummary(col("EXCEPTIONS_SUMMARY")))
      .select((baseColumns :+ col("EXCEPTIONS_SUMMARY").as("EXCPETED_EXCEPTION_FROM_SUMMARY")): _*)

    val failureStatusVsSuccessSummary = joined
      .filter(isFailureStatus(col("status")) && isSuccessSummary(col("EXCEPTIONS_SUMMARY")))
      .select(baseColumns: _*)

    val nullStatusVsSuccessSummary = joined
      .filter(isNullStatus(col("status")) && isSuccessSummary(col("EXCEPTIONS_SUMMARY")))
      .select(baseColumns: _*)

    val nullStatusVsExceptionSummary = joined
      .filter(isNullStatus(col("status")) && isExceptionSummary(col("EXCEPTIONS_SUMMARY")))
      .select((baseColumns :+ col("EXCEPTIONS_SUMMARY").as("EXCPETED_EXCEPTION_FROM_SUMMARY")): _*)

    val orphanRecipeExistsInSummaryNotInB15 = exceptionSummary
      .join(b15.select("recipe_filename").distinct(), Seq("recipe_filename"), "left_anti")
      .select(
        col("recipe_filename"),
        col("RECIPE_FILE_PATH"),
        col("EXECUTION_DATE_TIME"),
        col("EXECUTION_DURATION"),
        col("EXCEPTIONS_SUMMARY")
      )

    val orphanRecipeExistsInB15AndNotInSummary = b15
      .join(exceptionSummary.select("recipe_filename").distinct(), Seq("recipe_filename"), "left_anti")
      .select(baseColumns: _*)

    val report = buildFullStatusVsExceptionSummaryReport(
      rawB15Count = rawB15Count,
      rawExceptionCount = rawExceptionCount,
      distinctB15Recipes = b15.select("recipe_filename").distinct().count(),
      distinctSummaryRecipes = exceptionSummary.select("recipe_filename").distinct().count(),
      matchedDistinctRecipes = joined.select("recipe_filename").distinct().count(),
      successStatusVsSuccessSummaryCount = successStatusVsSuccessSummary.count(),
      successStatusVsExceptionSummaryCount = successStatusVsExceptionSummary.count(),
      failureStatusVsSuccessSummaryCount = failureStatusVsSuccessSummary.count(),
      nullStatusVsSuccessSummaryCount = nullStatusVsSuccessSummary.count(),
      nullStatusVsExceptionSummaryCount = nullStatusVsExceptionSummary.count(),
      orphanSummaryCount = orphanRecipeExistsInSummaryNotInB15.count(),
      orphanB15Count = orphanRecipeExistsInB15AndNotInSummary.count(),
      b15SuccessCount = b15.filter(isSuccessStatus(col("status"))).count(),
      b15FailureCount = b15.filter(isFailureStatus(col("status"))).count(),
      b15NullCount = b15.filter(isNullStatus(col("status"))).count()
    )

    logger.info(
      s"Built categorized outputs. " +
        s"successVsSuccess=${successStatusVsSuccessSummary.count()} " +
        s"successVsException=${successStatusVsExceptionSummary.count()} " +
        s"failureVsSuccess=${failureStatusVsSuccessSummary.count()} " +
        s"nullVsSuccess=${nullStatusVsSuccessSummary.count()} " +
        s"nullVsException=${nullStatusVsExceptionSummary.count()} " +
        s"orphanSummary=${orphanRecipeExistsInSummaryNotInB15.count()} " +
        s"orphanB15=${orphanRecipeExistsInB15AndNotInSummary.count()}"
    )

    OutputBundleDfs(
      successStatusVsSuccessSummary = successStatusVsSuccessSummary,
      successStatusVsExceptionSummary = successStatusVsExceptionSummary,
      failureStatusVsSuccessSummary = failureStatusVsSuccessSummary,
      nullStatusVsSuccessSummary = nullStatusVsSuccessSummary,
      nullStatusVsExceptionSummary = nullStatusVsExceptionSummary,
      orphanRecipeExistsInSummaryNotInB15 = orphanRecipeExistsInSummaryNotInB15,
      orphanRecipeExistsInB15AndNotInSummary = orphanRecipeExistsInB15AndNotInSummary,
      fullStatusVsExceptionSummaryReport = report
    )
  }

  /** Writes all generated [[output]] artifacts under the resolved output directory. */
  def writeOutputs(outputs: OutputBundleDfs, outputDirPath: String): Unit = {
    val outputDir = new File(outputDirPath)
    if (!outputDir.exists()) outputDir.mkdirs()

    writeSingleCsv(outputs.successStatusVsSuccessSummary, new File(outputDir, SuccessStatusVsSuccessSummaryFile))
    writeSingleCsv(outputs.successStatusVsExceptionSummary, new File(outputDir, SuccessStatusVsExceptionSummaryFile))
    writeSingleCsv(outputs.failureStatusVsSuccessSummary, new File(outputDir, FailureStatusVsSuccessSummaryFile))
    writeSingleCsv(outputs.nullStatusVsSuccessSummary, new File(outputDir, NullStatusVsSuccessSummaryFile))
    writeSingleCsv(outputs.nullStatusVsExceptionSummary, new File(outputDir, NullStatusVsExceptionSummaryFile))
    writeSingleCsv(outputs.orphanRecipeExistsInSummaryNotInB15, new File(outputDir, OrphanRecipeExistsInSummaryNotInB15File))
    writeSingleCsv(outputs.orphanRecipeExistsInB15AndNotInSummary, new File(outputDir, OrphanRecipeExistsInB15AndNotInSummaryFile))
    writeTextFile(outputs.fullStatusVsExceptionSummaryReport, new File(outputDir, FullStatusVsExceptionSummaryReportFile))

    logger.info(
      s"Wrote output artifacts: $SuccessStatusVsSuccessSummaryFile, $SuccessStatusVsExceptionSummaryFile, " +
        s"$FailureStatusVsSuccessSummaryFile, $NullStatusVsSuccessSummaryFile, $NullStatusVsExceptionSummaryFile, " +
        s"$OrphanRecipeExistsInSummaryNotInB15File, $OrphanRecipeExistsInB15AndNotInSummaryFile, $FullStatusVsExceptionSummaryReportFile"
    )
  }

  /** Builds the plain-text high-level summary used for quick review in the PR and local runs. */
  def buildFullStatusVsExceptionSummaryReport(
                                               rawB15Count: Long,
                                               rawExceptionCount: Long,
                                               distinctB15Recipes: Long,
                                               distinctSummaryRecipes: Long,
                                               matchedDistinctRecipes: Long,
                                               successStatusVsSuccessSummaryCount: Long,
                                               successStatusVsExceptionSummaryCount: Long,
                                               failureStatusVsSuccessSummaryCount: Long,
                                               nullStatusVsSuccessSummaryCount: Long,
                                               nullStatusVsExceptionSummaryCount: Long,
                                               orphanSummaryCount: Long,
                                               orphanB15Count: Long,
                                               b15SuccessCount: Long,
                                               b15FailureCount: Long,
                                               b15NullCount: Long
                                             ): String = {
    val lines = Seq(
      s"B15 input rows: $rawB15Count",
      s"Exception summary input rows: $rawExceptionCount",
      s"Distinct B15 recipes: $distinctB15Recipes",
      s"Distinct summary recipes: $distinctSummaryRecipes",
      s"Matched distinct recipes: $matchedDistinctRecipes (${formatPct(matchedDistinctRecipes, distinctB15Recipes)})",
      "",
      s"B15_success_status_vs_success_summary: $successStatusVsSuccessSummaryCount (${formatPct(successStatusVsSuccessSummaryCount, rawB15Count)})",
      s"B15_success_status_vs_exception_summary: $successStatusVsExceptionSummaryCount (${formatPct(successStatusVsExceptionSummaryCount, rawB15Count)})",
      s"B15_failure_status_vs_success_summary: $failureStatusVsSuccessSummaryCount (${formatPct(failureStatusVsSuccessSummaryCount, rawB15Count)})",
      s"B15_null_status_vs_success_summary: $nullStatusVsSuccessSummaryCount (${formatPct(nullStatusVsSuccessSummaryCount, rawB15Count)})",
      s"B15_null_status_vs_exception_summary: $nullStatusVsExceptionSummaryCount (${formatPct(nullStatusVsExceptionSummaryCount, rawB15Count)})",
      "",
      s"Orphan recipes in summary and not in B15: $orphanSummaryCount",
      s"Orphan recipes in B15 and not in summary: $orphanB15Count",
      "",
      s"B15 SUCCESS rows: $b15SuccessCount (${formatPct(b15SuccessCount, rawB15Count)})",
      s"B15 FAILURE rows: $b15FailureCount (${formatPct(b15FailureCount, rawB15Count)})",
      s"B15 NULL/empty status rows: $b15NullCount (${formatPct(b15NullCount, rawB15Count)})"
    )
    lines.mkString("\n") + "\n"
  }

  def normalizeStatus(raw: String): String =
    Option(raw).map(_.trim).filter(_.nonEmpty).map(_.stripPrefix("\"").stripSuffix("\"").trim.toUpperCase).orNull

  def normalizeSummary(raw: String): String =
    Option(raw).map(_.trim).filter(_.nonEmpty).map(_.stripPrefix("\"").stripSuffix("\"").trim).orNull

  def extractRecipeFilenameFromPath(path: String): String =
    Option(path).map(_.trim).filter(_.nonEmpty).flatMap { value =>
      value.split('/').lastOption.map(_.trim).filter(_.nonEmpty)
    }.orNull

  /** Resolves the dated B15 input path from a `yyyy_MM_dd` argument. */
  def defaultB15CsvPathForDate(date: String): String =
    s"src/main/resources/composer/dwh/config/cluster_tuning/inputs/$date/b15_application_end_with_recipe_null_status.csv"

  /** Keeps only the latest exception summary row per recipe, avoiding historical fan-out on join. */
  def latestExceptionSummaryPerRecipe(df: DataFrame): DataFrame = {
    val window = Window.partitionBy("recipe_filename")
      .orderBy(
        to_timestamp(col("EXECUTION_DATE_TIME"), "yyyy/MM/dd HH:mm:ss.SSS").desc_nulls_last,
        col("EXECUTION_DURATION").cast("long").desc_nulls_last,
        col("RECIPE_FILE_PATH").asc_nulls_last
      )

    df.withColumn("rn", row_number().over(window))
      .filter(col("rn") === 1)
      .drop("rn")
  }

  private def normalizeStatusColumn(column: org.apache.spark.sql.Column): org.apache.spark.sql.Column =
    when(length(trim(column)) === 0 || column.isNull, lit(null).cast("string"))
      .otherwise(upper(trim(regexp_replace(column, "^\"|\"$", ""))))

  private def normalizeSummaryColumn(column: org.apache.spark.sql.Column): org.apache.spark.sql.Column =
    when(length(trim(column)) === 0 || column.isNull, lit(null).cast("string"))
      .otherwise(trim(regexp_replace(column, "^\"|\"$", "")))

  private def extractRecipeFilenameFromPathColumn(column: org.apache.spark.sql.Column): org.apache.spark.sql.Column =
    regexp_extract(column, "([^/]+\\.json)$", 1)

  private def isSuccessStatus(column: org.apache.spark.sql.Column): org.apache.spark.sql.Column =
    column === StatusSuccess

  private def isFailureStatus(column: org.apache.spark.sql.Column): org.apache.spark.sql.Column =
    column === StatusFailure

  private def isNullStatus(column: org.apache.spark.sql.Column): org.apache.spark.sql.Column =
    column.isNull || length(trim(column)) === 0

  private def isSuccessSummary(column: org.apache.spark.sql.Column): org.apache.spark.sql.Column =
    column === SummarySuccess

  private def isExceptionSummary(column: org.apache.spark.sql.Column): org.apache.spark.sql.Column =
    column.isNotNull && length(trim(column)) > 0 && column =!= SummarySuccess

  private def formatPct(numerator: Long, denominator: Long): String =
    if (denominator <= 0) "0.00%" else f"${numerator.toDouble * 100.0 / denominator.toDouble}%.2f%%"

  private def writeSingleCsv(df: DataFrame, targetFile: File): Unit = {
    val tempDir = new File(targetFile.getParentFile, s".${targetFile.getName}.tmp")
    if (tempDir.exists()) FileUtils.deleteDirectory(tempDir)
    if (targetFile.exists()) targetFile.delete()

    df.coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .option("header", "true")
      .csv(tempDir.getAbsolutePath)

    val partFile = Option(tempDir.listFiles()).toSeq.flatten.find(_.getName.startsWith("part-"))
      .getOrElse(throw new IllegalStateException(s"No part file generated for ${targetFile.getName}"))

    FileUtils.copyFile(partFile, targetFile)
    FileUtils.deleteDirectory(tempDir)
  }

  private def writeTextFile(content: String, targetFile: File): Unit = {
    val pw = new PrintWriter(targetFile)
    try pw.write(content)
    finally pw.close()
  }
}