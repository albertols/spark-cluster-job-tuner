package com.db.serna.orchestration.cluster_tuning.auto.oss_mock

import com.db.serna.orchestration.cluster_tuning.auto.{ClusterMachineAndRecipeAutoTuner => AutoTuner}
import com.db.serna.orchestration.cluster_tuning.single.{ClusterMachineAndRecipeTuner => SingleTuner}
import org.slf4j.LoggerFactory

import java.io.File

/**
 * CLI entry point for the OSS mock-data generator.
 *
 * Examples:
 *   --date=2099_01_01                                                # single-date, default scenario (baseline), default seed
 *   --date=2099_01_01 --scenario=oomHeavy --seed=42                  # single-date, custom scenario+seed
 *   --reference-date=2099_01_01 --current-date=2099_01_02 \
 *     --scenario=multiDateBaseline                                   # multi-date pair
 *   --date=2099_01_01 --full                                         # also runs the single-date tuner end-to-end
 *   --reference-date=2099_01_01 --current-date=2099_01_02 \
 *     --scenario=multiDateBaseline --full                            # writes inputs, runs both single tuner passes,
 *                                                                    # then runs the AutoTuner so the frontend has _auto_tuner_analysis.json
 *   --date=2099_01_01 --scenario=syntheticSpan --full                # b20-missing / b21-present synthesis (single-date)
 *   --reference-date=2099_01_01 --current-date=2099_01_02 \
 *     --scenario=multiDateSyntheticSpan --full                       # same, paired across two dates for the frontend's 3-card view
 *
 * Notes:
 *   - The tuner reads inputs from the canonical path
 *     `src/main/resources/composer/dwh/config/cluster_tuning/inputs/<date>/`,
 *     so by default we write there. Override with `--inputs-root` if you only
 *     want CSVs on disk for inspection — `--full` will then warn that the
 *     tuner cannot find them.
 *   - DAG and timer maps are NOT generated (canonical project-wide files at
 *     `…/composer/dwh/config/_dag_*.csv` are left alone). Mock cluster names
 *     resolve to `UNKNOWN_DAG_ID` / `ZERO_TIMER` in the summaries — that's
 *     expected and harmless.
 */
object OssMockMain {

  private val logger = LoggerFactory.getLogger(getClass)

  private val CanonicalInputsRoot: File =
    new File("src/main/resources/composer/dwh/config/cluster_tuning/inputs")

  private final case class Args(
                                 date: Option[String],
                                 referenceDate: Option[String],
                                 currentDate: Option[String],
                                 scenarioName: String,
                                 seed: Long,
                                 inputsRoot: File,
                                 full: Boolean
                               )

  def main(args: Array[String]): Unit = {
    val parsed = parse(args)
    if (parsed.date.isDefined) runSingle(parsed)
    else runMulti(parsed)
  }

  // ── Argument parsing ──────────────────────────────────────────────────────

  private def parse(args: Array[String]): Args = {
    val kv: Map[String, String] = args.toSeq.flatMap {
      case s if s.startsWith("--") =>
        val body = s.drop(2)
        val eq   = body.indexOf('=')
        if (eq < 0) Some(body -> "true") else Some(body.take(eq) -> body.drop(eq + 1))
      case _ => None
    }.toMap

    val date          = kv.get("date")
    val referenceDate = kv.get("reference-date")
    val currentDate   = kv.get("current-date")
    val scenarioName  = kv.getOrElse("scenario", "baseline")
    val seed          = kv.get("seed").map(_.toLong).getOrElse(1234L)
    val inputsRoot    = kv.get("inputs-root").map(new File(_)).getOrElse(CanonicalInputsRoot)
    val full          = kv.get("full").contains("true")

    (date, referenceDate, currentDate) match {
      case (Some(_), None, None) => // single-date
      case (None, Some(_), Some(_)) => // multi-date
      case (None, None, None) =>
        usageAndExit("missing --date or (--reference-date + --current-date)")
      case _ =>
        usageAndExit("--date is mutually exclusive with --reference-date / --current-date; pass exactly one mode")
    }

    if (inputsRoot.getCanonicalPath != CanonicalInputsRoot.getCanonicalPath && full) {
      logger.warn(
        s"--inputs-root=${inputsRoot.getPath} differs from canonical " +
        s"${CanonicalInputsRoot.getPath}; --full will run the tuner against the canonical path " +
        "regardless, which will not find your generated CSVs. Either drop --full or write to the canonical root."
      )
    }

    Args(date, referenceDate, currentDate, scenarioName, seed, inputsRoot, full)
  }

  private def usageAndExit(reason: String): Nothing = {
    val singles = MockScenarios.singleDateNames.mkString(", ")
    val multis  = MockScenarios.multiDateNames.mkString(", ")
    System.err.println(
      s"""OssMockMain: $reason
         |
         |Usage:
         |  --date=YYYY_MM_DD                                       (single-date)
         |  --reference-date=YYYY_MM_DD --current-date=YYYY_MM_DD   (multi-date)
         |
         |  --scenario=<name>     single: $singles
         |                        multi : $multis
         |                        default: baseline
         |  --seed=N              default: 1234
         |  --inputs-root=<path>  default: ${CanonicalInputsRoot.getPath}
         |  --full                also run the tuner (and AutoTuner for multi-date)
         |""".stripMargin
    )
    sys.exit(2)
  }

  // ── Single-date flow ──────────────────────────────────────────────────────

  private def runSingle(a: Args): Unit = {
    val date = a.date.get
    val builder = MockScenarios.singleDate.getOrElse(
      a.scenarioName,
      usageAndExit(s"unknown single-date scenario '${a.scenarioName}'. Known: ${MockScenarios.singleDateNames.mkString(", ")}")
    )
    val scenario = builder(date, a.seed)
    val outDir   = new File(a.inputsRoot, date)
    val written  = MockGen.writeAll(scenario, outDir)
    summarize(scenario, Seq(date -> written))

    if (a.full) {
      logger.info(s"oss_mock: --full requested; invoking ClusterMachineAndRecipeTuner.main for $date")
      SingleTuner.main(Array(date))
    }
  }

  // ── Multi-date flow ───────────────────────────────────────────────────────

  private def runMulti(a: Args): Unit = {
    val refDate = a.referenceDate.get
    val curDate = a.currentDate.get
    val builder = MockScenarios.multiDate.getOrElse(
      a.scenarioName,
      usageAndExit(s"unknown multi-date scenario '${a.scenarioName}'. Known: ${MockScenarios.multiDateNames.mkString(", ")}")
    )
    val multi = builder(refDate, curDate, a.seed)
    val written = multi.perDate.toSeq.sortBy(_._1).map { case (date, scenario) =>
      val outDir = new File(a.inputsRoot, date)
      date -> MockGen.writeAll(scenario, outDir)
    }
    multi.perDate.toSeq.sortBy(_._1).foreach { case (_, s) =>
      summarize(s, Nil) // per-date headline
    }
    summarizeFiles(written)

    if (a.full) {
      logger.info(s"oss_mock: --full requested; invoking ClusterMachineAndRecipeTuner.main for both dates")
      SingleTuner.main(Array(refDate))
      SingleTuner.main(Array(curDate))
      logger.info(s"oss_mock: invoking ClusterMachineAndRecipeAutoTuner.main with reference=$refDate current=$curDate")
      AutoTuner.main(Array(s"--reference-date=$refDate", s"--current-date=$curDate"))
    }
  }

  // ── Logging helpers ───────────────────────────────────────────────────────

  private def summarize(scenario: MockScenario, written: Seq[(String, Seq[File])]): Unit = {
    val totalRecipes  = scenario.clusters.map(_.recipes.size).sum
    val totalIncarn   = scenario.clusters.map(_.incarnations.size).sum
    val totalAutoEvts = scenario.clusters.flatMap(_.incarnations).flatMap(_.autoscaler).map(_.schedule.size).sum
    val totalExits    = scenario.clusters.map(_.driverExitCodes.size).sum
    val totalOoms     = scenario.clusters.map(_.oomEvents.size).sum
    logger.info(
      s"oss_mock: scenario='${scenario.name}' clusters=${scenario.clusters.size} " +
      s"recipes=$totalRecipes incarnations=$totalIncarn autoscaler_events=$totalAutoEvts " +
      s"b14_exits=$totalExits b16_ooms=$totalOoms seed=${scenario.seed}"
    )
    summarizeFiles(written)
  }

  private def summarizeFiles(written: Seq[(String, Seq[File])]): Unit = {
    written.foreach { case (date, files) =>
      logger.info(s"oss_mock: $date -> wrote ${files.size} CSVs into ${files.headOption.map(_.getParent).getOrElse("?")}")
    }
  }
}
