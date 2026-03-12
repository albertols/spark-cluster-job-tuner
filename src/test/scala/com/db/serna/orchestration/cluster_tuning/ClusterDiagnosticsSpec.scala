package com.db.serna.orchestration.cluster_tuning

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.io.{File, PrintWriter}
import java.nio.file.Files

class ClusterDiagnosticsSpec extends AnyFunSuite with Matchers {

  private val sampleRecords = Seq(
    ExitCodeRecord("2026-01-01T00:00:00Z", "job-1", "cluster-a", 247),
    ExitCodeRecord("2026-01-01T00:01:00Z", "job-2", "cluster-a", 247),
    ExitCodeRecord("2026-01-01T00:02:00Z", "job-3", "cluster-a", 1),
    ExitCodeRecord("2026-01-01T00:03:00Z", "job-4", "cluster-b", 1),
    ExitCodeRecord("2026-01-01T00:04:00Z", "job-5", "cluster-b", 1),
    ExitCodeRecord("2026-01-01T00:05:00Z", "job-6", "cluster-c", 247)
  )

  // ── detectSignals ─────────────────────────────────────────────────────────

  test("detectSignals identifies YarnDriverEviction for exit code 247") {
    val signals = ClusterDiagnosticsProcessor.detectSignals(sampleRecords)
    val clusterASignals = signals("cluster-a")
    val evictions = clusterASignals.collect { case e: YarnDriverEviction => e }
    evictions should have size 1
    evictions.head.evictionCount shouldBe 2
    evictions.head.clusterName shouldBe "cluster-a"
    evictions.head.affectedJobs should contain("job-1")
    evictions.head.affectedJobs should contain("job-2")
  }

  test("detectSignals identifies NonZeroExitPattern for non-247 codes") {
    val signals = ClusterDiagnosticsProcessor.detectSignals(sampleRecords)
    val clusterBSignals = signals("cluster-b")
    val patterns = clusterBSignals.collect { case e: NonZeroExitPattern => e }
    patterns should have size 1
    patterns.head.dominantExitCode shouldBe 1
    patterns.head.occurrenceCount shouldBe 2
  }

  test("detectSignals clusters with only 247 have no NonZeroExitPattern") {
    val only247 = Seq(ExitCodeRecord("2026-01-01T00:00:00Z", "j1", "cluster-c", 247))
    val signals = ClusterDiagnosticsProcessor.detectSignals(only247)
    val patterns = signals("cluster-c").collect { case e: NonZeroExitPattern => e }
    patterns shouldBe empty
  }

  test("detectSignals returns empty map for empty input") {
    ClusterDiagnosticsProcessor.detectSignals(Seq.empty) shouldBe empty
  }

  test("YarnDriverEviction.description contains eviction count") {
    val e = YarnDriverEviction("cluster-a", 3, Seq("j1", "j2"))
    e.description should include("3")
    e.description should include("247")
  }

  test("NonZeroExitPattern.description contains exit code and count") {
    val p = NonZeroExitPattern("cluster-b", 1, 5)
    p.description should include("1")
    p.description should include("5")
  }

  // ── computeOverrides ──────────────────────────────────────────────────────

  test("computeOverrides generates override only for clusters with YarnDriverEviction") {
    val signals = ClusterDiagnosticsProcessor.detectSignals(sampleRecords)
    val overrides = ClusterDiagnosticsProcessor.computeOverrides(signals)
    overrides.keys should contain("cluster-a")
    overrides.keys should contain("cluster-c")
    overrides.keys should not contain "cluster-b"
  }

  test("computeOverrides boosts driver memory by 4 GB above base") {
    val signals = ClusterDiagnosticsProcessor.detectSignals(sampleRecords)
    val overrides = ClusterDiagnosticsProcessor.computeOverrides(signals, baseDriverMemoryGb = 4)
    overrides("cluster-a").driverMemoryGb shouldBe Some(8)
  }

  test("computeOverrides ensures at least 4 driver cores") {
    val signals = ClusterDiagnosticsProcessor.detectSignals(sampleRecords)
    val overrides = ClusterDiagnosticsProcessor.computeOverrides(signals, baseDriverCores = 2)
    overrides("cluster-a").driverCores shouldBe Some(4)
  }

  test("computeOverrides includes diagnosticReason in override") {
    val signals = ClusterDiagnosticsProcessor.detectSignals(sampleRecords)
    val overrides = ClusterDiagnosticsProcessor.computeOverrides(signals)
    overrides("cluster-a").diagnosticReason should not be empty
    overrides("cluster-a").diagnosticReason should include("247")
  }

  test("computeOverrides returns empty map for no eviction signals") {
    val noEviction = Seq(ExitCodeRecord("2026-01-01T00:00:00Z", "j1", "cluster-x", 1))
    val signals = ClusterDiagnosticsProcessor.detectSignals(noEviction)
    val overrides = ClusterDiagnosticsProcessor.computeOverrides(signals)
    overrides shouldBe empty
  }

  // ── loadExitCodes ─────────────────────────────────────────────────────────

  test("loadExitCodes returns empty Seq when file does not exist") {
    val result = ClusterDiagnosticsProcessor.loadExitCodes(new File("/nonexistent/b14.csv"))
    result shouldBe empty
  }

  test("loadExitCodes parses CSV and strips triple-quotes from cluster_name") {
    val tmpFile = Files.createTempFile("b14_test", ".csv").toFile
    tmpFile.deleteOnExit()
    val pw = new PrintWriter(tmpFile)
    try {
      pw.println("timestamp,job_id,cluster_name,driver_exit_code,msg")
      pw.println("2026-01-01T00:00:00Z,job-1,\"\"\"cluster-wf-foo\"\"\",247,some message")
      pw.println("2026-01-01T00:01:00Z,job-2,\"\"\"cluster-wf-bar\"\"\",1,other message")
    } finally pw.close()

    val records = ClusterDiagnosticsProcessor.loadExitCodes(tmpFile)
    records should have size 2
    records.head.clusterName shouldBe "cluster-wf-foo"
    records.head.driverExitCode shouldBe 247
    records(1).clusterName shouldBe "cluster-wf-bar"
    records(1).driverExitCode shouldBe 1
  }

  test("loadExitCodes skips rows with missing or non-integer exit code") {
    val tmpFile = Files.createTempFile("b14_bad", ".csv").toFile
    tmpFile.deleteOnExit()
    val pw = new PrintWriter(tmpFile)
    try {
      pw.println("timestamp,job_id,cluster_name,driver_exit_code,msg")
      pw.println("2026-01-01T00:00:00Z,job-1,cluster-a,247,ok")
      pw.println("2026-01-01T00:01:00Z,job-2,cluster-b,not_a_number,bad")
      pw.println("2026-01-01T00:02:00Z,job-3,,0,empty cluster")
    } finally pw.close()

    val records = ClusterDiagnosticsProcessor.loadExitCodes(tmpFile)
    records should have size 1
    records.head.clusterName shouldBe "cluster-a"
  }

  // ── promoteMasterForEviction ──────────────────────────────────────────────

  test("promoteMasterForEviction: n2-highcpu-32 → n2-standard-32 (variant step up)") {
    val m = MachineCatalog.byName("n2-highcpu-32").get
    ClusterDiagnosticsProcessor.promoteMasterForEviction(m).name shouldBe "n2-standard-32"
  }

  test("promoteMasterForEviction: n2-standard-32 → n2-highmem-32 (variant step up)") {
    val m = MachineCatalog.byName("n2-standard-32").get
    ClusterDiagnosticsProcessor.promoteMasterForEviction(m).name shouldBe "n2-highmem-32"
  }

  test("promoteMasterForEviction: n2-highmem-32 → n2-highmem-48 (variant exhausted, core step up)") {
    val m = MachineCatalog.byName("n2-highmem-32").get
    ClusterDiagnosticsProcessor.promoteMasterForEviction(m).name shouldBe "n2-highmem-48"
  }

  test("promoteMasterForEviction: e2-standard-32 → n2-standard-32 (e2-highmem-32 absent, cross-family)") {
    // e2-highmem only goes up to 16 cores in the catalog — so e2-standard-32 promotes to n2
    val m = MachineCatalog.byName("e2-standard-32").get
    val promoted = ClusterDiagnosticsProcessor.promoteMasterForEviction(m)
    promoted.name shouldBe "n2-standard-32"
  }

  test("promoteMasterForEviction: e2-standard-16 → e2-highmem-16 (variant step; e2-highmem-16 exists)") {
    val m = MachineCatalog.byName("e2-standard-16").get
    ClusterDiagnosticsProcessor.promoteMasterForEviction(m).name shouldBe "e2-highmem-16"
  }

  test("promoteMasterForEviction: e2-highcpu-8 → e2-standard-8 (variant step)") {
    val m = MachineCatalog.byName("e2-highcpu-8").get
    ClusterDiagnosticsProcessor.promoteMasterForEviction(m).name shouldBe "e2-standard-8"
  }

  test("promoteMasterForEviction: n2d-standard-32 → n2d-highmem-32 (variant step)") {
    val m = MachineCatalog.byName("n2d-standard-32").get
    ClusterDiagnosticsProcessor.promoteMasterForEviction(m).name shouldBe "n2d-highmem-32"
  }

  test("promoteMasterForEviction: promoted machine has more memory than original") {
    Seq("n2-highcpu-32", "n2-standard-32", "e2-standard-16", "n2d-highcpu-32").foreach { name =>
      val m = MachineCatalog.byName(name).get
      val promoted = ClusterDiagnosticsProcessor.promoteMasterForEviction(m)
      withClue(s"$name → ${promoted.name}: ") {
        promoted.memoryGb should be >= m.memoryGb
      }
    }
  }

  test("parseMachineName round-trips family/variant/cores for all families") {
    val cases = Seq(
      ("n2-standard-32",  "n2",  "standard", 32),
      ("n2d-highmem-48",  "n2d", "highmem",  48),
      ("e2-highcpu-8",    "e2",  "highcpu",   8),
      ("c3-standard-44",  "c3",  "standard", 44),
      ("c4-highmem-96",   "c4",  "highmem",  96)
    )
    cases.foreach { case (name, expectedFamily, expectedVariant, expectedCores) =>
      val (family, variant, cores) = ClusterDiagnosticsProcessor.parseMachineName(name)
      withClue(s"parsing '$name': ") {
        family  shouldBe expectedFamily
        variant shouldBe expectedVariant
        cores   shouldBe expectedCores
      }
    }
  }

  // ── triple-quote stripping logic ──────────────────────────────────────────

  test("triple-quote stripping: replaceAll quotes gives clean cluster name") {
    val raw = "\"\"\"cluster-wf-spark-42\"\"\""
    raw.replaceAll("\"", "").trim shouldBe "cluster-wf-spark-42"
  }
}
