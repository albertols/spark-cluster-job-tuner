package com.db.serna.orchestration.cluster_tuning.auto.oss_mock

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.nio.file.Files

/**
 * CLI smoke tests for OssMockMain. The `--full` flag chains the actual tuner + AutoTuner; we deliberately do NOT
 * exercise it here to keep these unit tests fast and side-effect-free. End-to-end coverage of `--full` belongs in a
 * manual / integration smoke run (see _OSS_MOCK.md).
 */
class OssMockMainSpec extends AnyFunSuite with Matchers {

  private def tmpDir(): File = Files.createTempDirectory("oss-mock-cli-").toFile

  // List of expected files written by every invocation, regardless of scenario.
  private val expectedFiles: Seq[String] = Seq(
    "b13_recommendations_inputs_per_recipe_per_cluster.csv",
    "b1_average_number_of_executors_per_job_by_cluster.csv",
    "b2_peak_executors_seen.csv",
    "b3_average_recipefilename_per_cluster.csv",
    "b4_peak_job_duration_per_cluster.csv",
    "b5_a_times_job_reaches_max_executor_per_cluster.csv",
    "b6_total_jobs_per_cluster.csv",
    "b7_total_runtime_all_jobs_per_cluster.csv",
    "b8_P95_job_duration_per_recipe_per_cluster.csv",
    "b9_time_at_cap_per_run_and_per_cluster.csv",
    "b10_executor_churn_per_job_adds_removes.csv",
    "b11_max_concurrent_jobs_per_cluster_in_window.csv",
    "b12_p95_max_executors_per_recipe_per_cluster.csv",
    "b14_clusters_with_nonzero_exit_codes.csv",
    "b16_oom_job_driver_exceptions.csv",
    "b20_cluster_span_time.csv",
    "b21_cluster_autoscaler_values.csv"
  )

  test("single-date invocation writes all expected CSVs into <inputs-root>/<date>/") {
    val root = tmpDir()
    OssMockMain.main(
      Array(
        "--date=2099_01_01",
        "--scenario=baseline",
        s"--inputs-root=${root.getAbsolutePath}"
      )
    )
    val dateDir = new File(root, "2099_01_01")
    dateDir.exists() shouldBe true
    expectedFiles.foreach { name =>
      withClue(s"missing: $name in ${dateDir.getPath}") {
        new File(dateDir, name).exists() shouldBe true
      }
    }
  }

  test("multi-date invocation writes both date directories") {
    val root = tmpDir()
    OssMockMain.main(
      Array(
        "--reference-date=2099_01_01",
        "--current-date=2099_01_02",
        "--scenario=multiDateBaseline",
        s"--inputs-root=${root.getAbsolutePath}"
      )
    )
    Seq("2099_01_01", "2099_01_02").foreach { date =>
      val dateDir = new File(root, date)
      withClue(s"missing date dir: $date") { dateDir.exists() shouldBe true }
      expectedFiles.foreach { name =>
        withClue(s"missing: $name in $date") {
          new File(dateDir, name).exists() shouldBe true
        }
      }
    }
  }

  test("seed makes single-date generation byte-deterministic across invocations") {
    val a = tmpDir()
    val b = tmpDir()
    val args = (root: File) =>
      Array(
        "--date=2099_01_01",
        "--scenario=baseline",
        "--seed=99",
        s"--inputs-root=${root.getAbsolutePath}"
      )
    OssMockMain.main(args(a))
    OssMockMain.main(args(b))
    val nameA = new File(new File(a, "2099_01_01"), "b13_recommendations_inputs_per_recipe_per_cluster.csv")
    val nameB = new File(new File(b, "2099_01_01"), "b13_recommendations_inputs_per_recipe_per_cluster.csv")
    val bytesA = java.nio.file.Files.readAllBytes(nameA.toPath)
    val bytesB = java.nio.file.Files.readAllBytes(nameB.toPath)
    bytesA shouldBe bytesB
  }
}
