package com.db.serna.orchestration.cluster_tuning

import org.slf4j.LoggerFactory

import java.io.File

// ── Raw b14 record ────────────────────────────────────────────────────────────
final case class ExitCodeRecord(
  timestamp: String,
  jobId: String,
  clusterName: String,
  driverExitCode: Int
)

// ── Diagnostic signal hierarchy ───────────────────────────────────────────────
// Each subtype represents a detected failure pattern on a specific cluster.
// New patterns (MemoryHeap, GC pressure, etc.) should be added as additional subtypes.
sealed trait DiagnosticSignal {
  def clusterName: String
  def description: String
}

// YARN driver eviction: driver killed by YARN due to resource pressure on the master node.
// Exit code 247 is the canonical YARN out-of-memory / container-eviction code for Dataproc.
// Remedy: boost driver memory and cores in clusterConf.
final case class YarnDriverEviction(
  clusterName: String,
  evictionCount: Int,
  affectedJobs: Seq[String]
) extends DiagnosticSignal {
  val description: String =
    s"YARN driver evicted $evictionCount time(s) (exit 247). Sample jobs: ${affectedJobs.take(3).mkString(", ")}"
}

// Generic non-zero exit pattern — covers "silent FAILED" jobs without a specific remedy yet.
// Documented for observability; no automatic config override is applied.
final case class NonZeroExitPattern(
  clusterName: String,
  dominantExitCode: Int,
  occurrenceCount: Int
) extends DiagnosticSignal {
  val description: String =
    s"Non-zero exit code $dominantExitCode occurred $occurrenceCount time(s)"
}

// Future extension stubs (not yet implemented):
// final case class MemoryHeapSignal(...)  extends DiagnosticSignal
// final case class GcPressureSignal(...)  extends DiagnosticSignal

// ── Driver resource override ──────────────────────────────────────────────────
// Applied to clusterConf in manualJson / daJson when a YarnDriverEviction is detected.
// Fields are injected as additional keys into the cluster's JSON config block.
// promotedMasterMachineType: upgraded master machine selected by promoteMasterForEviction();
//   replaces master_machine_type in the output JSON when set.
final case class DriverResourceOverride(
  clusterName: String,
  driverMemoryGb: Option[Int],
  driverCores: Option[Int],
  driverMemoryOverheadGb: Option[Int],
  diagnosticReason: String,
  promotedMasterMachineType: Option[MachineType] = None
)

// ── Diagnostics processor ─────────────────────────────────────────────────────
object ClusterDiagnosticsProcessor {

  private val logger = LoggerFactory.getLogger(getClass)

  val YarnEvictionCode: Int = 247

  // Load b14_clusters_with_nonzero_exit_codes.csv.
  // The cluster_name field is triple-double-quoted in the CSV due to BigQuery JSON embedding:
  //   """cluster-name"""  →  strip all " characters after split.
  def loadExitCodes(b14File: File): Seq[ExitCodeRecord] = {
    if (!b14File.exists()) {
      logger.warn(s"b14 diagnostics file not found: ${b14File.getPath}. Skipping diagnostics.")
      return Seq.empty
    }
    val rows = Csv.parse(b14File)
    logger.info(s"b14: parsed ${rows.size} rows from ${b14File.getName}")
    val records = rows.flatMap { r =>
      for {
        ts      <- r.get("timestamp")
        jobId   <- r.get("job_id")
        rawCluster <- r.get("cluster_name")
        cluster  = rawCluster.replaceAll("\"", "").trim
        if cluster.nonEmpty
        code    <- r.get("driver_exit_code").flatMap(Csv.toInt)
      } yield ExitCodeRecord(ts, jobId, cluster, code)
    }
    logger.info(s"b14: loaded ${records.size} exit-code records for ${records.map(_.clusterName).distinct.size} clusters")
    records
  }

  // Aggregate exit-code records into DiagnosticSignal instances per cluster.
  def detectSignals(records: Seq[ExitCodeRecord]): Map[String, Seq[DiagnosticSignal]] = {
    val byCluster = records.groupBy(_.clusterName)
    byCluster.map { case (cluster, recs) =>
      val signals = scala.collection.mutable.ArrayBuffer.empty[DiagnosticSignal]

      // Detect YARN evictions (exit code 247)
      val evictions = recs.filter(_.driverExitCode == YarnEvictionCode)
      if (evictions.nonEmpty) {
        signals += YarnDriverEviction(
          clusterName   = cluster,
          evictionCount = evictions.size,
          affectedJobs  = evictions.map(_.jobId).distinct
        )
        logger.info(s"b14: cluster '$cluster' has ${evictions.size} YARN eviction(s) (exit 247)")
      }

      // Detect other dominant non-zero codes
      val others = recs.filterNot(_.driverExitCode == YarnEvictionCode)
      if (others.nonEmpty) {
        val dominant = others.groupBy(_.driverExitCode).maxBy(_._2.size)
        signals += NonZeroExitPattern(cluster, dominant._1, dominant._2.size)
        logger.info(s"b14: cluster '$cluster' dominant non-247 exit code=${dominant._1} count=${dominant._2.size}")
      }

      cluster -> signals.toSeq
    }
  }

  // ── Master machine promotion for YARN driver evictions ───────────────────
  // Promotion path: highcpu → standard → highmem (same family + cores).
  // If that machine doesn't exist in the catalog (e.g. e2-highmem-32 is absent),
  // try the next available core count within the same family+variant.
  // If e2 and no upgrade exists within e2, cross-promote to n2-{variant}-{cores}.
  //
  // Examples:
  //   n2-highcpu-32  → n2-standard-32   (variant step)
  //   n2-standard-32 → n2-highmem-32    (variant step)
  //   n2-highmem-32  → n2-highmem-48    (core step, highmem at max variant)
  //   e2-standard-32 → n2-standard-32   (e2-highmem-32 absent, cross-family)
  //   e2-standard-16 → e2-highmem-16    (variant step — e2-highmem-16 exists)
  def promoteMasterForEviction(current: MachineType): MachineType = {
    val (fam, variant, cores) = parseMachineName(current.name)

    // Step 1: variant promotion (highcpu→standard, standard→highmem), same cores
    val variantUpgraded: Option[MachineType] = variant match {
      case "highcpu" => MachineCatalog.byName(s"$fam-standard-$cores")
      case "standard" => MachineCatalog.byName(s"$fam-highmem-$cores")
      case _ => None
    }

    // Step 2: next core step within same family+variant
    val coreUpgraded: Option[MachineType] = variantUpgraded.orElse {
      nextCoreStepMachine(fam, variant, cores)
    }

    // Step 3: cross-family to n2 when e2 has no valid upgrade
    val familyUpgraded: Option[MachineType] = coreUpgraded.orElse {
      if (fam == "e2") {
        MachineCatalog.byName(s"n2-$variant-$cores")
          .orElse(MachineCatalog.byName(s"n2-standard-$cores"))
      } else None
    }

    val promoted = familyUpgraded.getOrElse(current)
    if (promoted.name != current.name)
      logger.info(s"Master promotion: ${current.name} → ${promoted.name}")
    else
      logger.warn(s"Master promotion: no upgrade found for ${current.name}, keeping as-is")
    promoted
  }

  // Returns the smallest available core count above currentCores for the given family+variant.
  private def nextCoreStepMachine(family: String, variant: String, currentCores: Int): Option[MachineType] =
    MachineCatalog.defaults
      .filter(m => familyOf(m.name) == family && variantOf(m.name) == variant && m.cores > currentCores)
      .sortBy(_.cores)
      .headOption

  // Decompose "n2-standard-32" → ("n2", "standard", 32); handles n2d, e2, c3, c4, etc.
  private[cluster_tuning] def parseMachineName(name: String): (String, String, Int) = {
    val family = name.takeWhile(_ != '-')
    val rest   = name.drop(family.length + 1)
    val cores  = rest.reverse.takeWhile(_ != '-').reverse.toInt
    val variant = rest.dropRight(cores.toString.length + 1)
    (family, variant, cores)
  }

  private def familyOf(n: String): String  = n.takeWhile(_ != '-')
  private def variantOf(n: String): String = { val p = n.split("-"); if (p.length >= 2) p(1) else "standard" }

  // Compute DriverResourceOverride for clusters with YarnDriverEviction signals.
  // Heuristic: boost driver memory by +4 GB and ensure at least 4 driver cores.
  def computeOverrides(
    signals: Map[String, Seq[DiagnosticSignal]],
    baseDriverMemoryGb: Int = 4,
    baseDriverCores: Int = 2
  ): Map[String, DriverResourceOverride] = {
    signals.flatMap { case (cluster, sigs) =>
      val evictions = sigs.collect { case e: YarnDriverEviction => e }
      if (evictions.isEmpty) {
        None
      } else {
        val e = evictions.head
        val boostedMemGb   = baseDriverMemoryGb + 4
        val boostedCores   = math.max(baseDriverCores, 4)
        val overheadGb     = math.max(1, boostedMemGb / 4)
        val override_ = DriverResourceOverride(
          clusterName            = cluster,
          driverMemoryGb         = Some(boostedMemGb),
          driverCores            = Some(boostedCores),
          driverMemoryOverheadGb = Some(overheadGb),
          diagnosticReason       = e.description
        )
        logger.info(s"b14: override for '$cluster': driverMemory=${boostedMemGb}GB driverCores=$boostedCores (reason: ${e.description})")
        Some(cluster -> override_)
      }
    }
  }
}
