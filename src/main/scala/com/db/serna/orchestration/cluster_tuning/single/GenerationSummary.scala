package com.db.serna.orchestration.cluster_tuning.single

import org.slf4j.LoggerFactory

import java.io.{BufferedWriter, File, FileWriter}

// ── Quota tracker ─────────────────────────────────────────────────────────────
// One instance per run(). Records cores consumed per machine family as clusters are
// planned, and enforces hard caps on C3/C4 cluster counts and excluded families.
// withinQuota() is called inside chooseMachines before scoring; recordCluster() is
// called after the best machine is confirmed.
class QuotaTracker(quotas: Quotas) {

  private val logger = LoggerFactory.getLogger(getClass)

  private var usedCores: Map[String, Int] = Map(
    "e2" -> 0, "n2" -> 0, "n2d" -> 0,
    "c3" -> 0, "c4" -> 0, "n4"  -> 0, "n4d" -> 0
  ).withDefaultValue(0)

  private var c3ClusterCount: Int = 0
  private var c4ClusterCount: Int = 0

  // Returns true if this machine is permanently blocked regardless of core quota.
  // Hard constraints: excluded families and C3/C4 cluster-count caps.
  // These are NEVER bypassed, even when the soft-quota fallback is triggered.
  def isHardBlocked(machine: MachineType, pref: MachineSelectionPreference): Boolean = {
    val family = familyOf(machine.name)
    pref.excludedFamilies.contains(family) ||
      (family == "c3" && c3ClusterCount >= pref.c3MaxClusters) ||
      (family == "c4" && c4ClusterCount >= pref.c4MaxClusters)
  }

  // Returns true if adding this machine×workers combination stays within quota.
  def withinQuota(machine: MachineType, workers: Int, pref: MachineSelectionPreference): Boolean = {
    val family = familyOf(machine.name)

    if (pref.excludedFamilies.contains(family)) {
      logger.debug(s"QuotaTracker: '$family' is excluded")
      return false
    }

    if (family == "c3" && c3ClusterCount >= pref.c3MaxClusters) {
      logger.warn(s"QuotaTracker: c3 cluster cap reached (${pref.c3MaxClusters}), blocking ${machine.name}")
      return false
    }

    if (family == "c4" && c4ClusterCount >= pref.c4MaxClusters) {
      logger.warn(s"QuotaTracker: c4 cluster cap reached (${pref.c4MaxClusters}), blocking ${machine.name}")
      return false
    }

    val addedCores = machine.cores * workers
    val projected  = usedCores(family) + addedCores
    val quota      = quotas.forFamily(family)
    val ok         = quota == 0 || projected <= quota
    if (!ok) {
      logger.warn(s"QuotaTracker: quota risk for '$family': used=${usedCores(family)} + $addedCores = $projected > $quota")
    }
    ok
  }

  // Record the final allocation after a cluster is planned.
  def recordCluster(worker: MachineType, workers: Int, pref: MachineSelectionPreference): Unit = {
    val family     = familyOf(worker.name)
    val addedCores = worker.cores * workers
    usedCores      = usedCores.updated(family, usedCores(family) + addedCores)
    if (family == "c3") c3ClusterCount += 1
    if (family == "c4") c4ClusterCount += 1
    logger.debug(s"QuotaTracker: recorded $family +$addedCores cores (total=${usedCores(family)}, c3=$c3ClusterCount, c4=$c4ClusterCount)")
  }

  // Returns proportional quota usage for a family: usedCores / quotaCores.
  // Returns 0.0 when quota is 0 (unlimited). Exceeds 1.0 when over quota.
  // Used in chooseMachines to penalise families proportionally as they fill relative to quota,
  // naturally distributing allocations across N2/N2D/E2 in proportion to their quota limits.
  def quotaPressure(family: String): Double = {
    val quota = quotas.forFamily(family)
    if (quota <= 0) 0.0 else usedCores(family).toDouble / quota
  }

  // family -> (usedCores, quotaCores)
  def usageSummary: Map[String, (Int, Int)] =
    usedCores.map { case (family, used) => family -> (used, quotas.forFamily(family)) }

  private def familyOf(machineName: String): String = machineName.takeWhile(_ != '-')
}

// ── Generation summary model ──────────────────────────────────────────────────
final case class GenerationSummaryEntry(
  clusterName: String,
  workerMachineType: String,
  workerFamily: String,
  numWorkers: Int,
  maxWorkersFromPolicy: Int,
  totalCores: Int,
  maxTotalCores: Int,
  diagnosticSignals: Seq[String],
  strategyName: String,
  biasMode: String,
  topologyPreset: String
)

final case class GenerationSummary(
  generatedAt: String,
  date: String,
  strategyName: String,
  biasMode: String,
  topologyPreset: String,
  quotas: Quotas,
  totalClusters: Int,
  totalPredictedNodes: Int,     // sum of (numWorkers + 1 master) across all clusters
  totalMaxNodes: Int,            // sum of (maxWorkersFromPolicy + 1 master) for autoscaler ceiling
  quotaUsageByFamily: Map[String, (Int, Int)],  // family -> (usedCores, quotaCores)
  clustersWithDiagnosticOverrides: Int,
  entries: Seq[GenerationSummaryEntry]
)

// ── Generation summary writer ─────────────────────────────────────────────────
object GenerationSummaryWriter {

  private val logger = LoggerFactory.getLogger(getClass)

  def toJson(summary: GenerationSummary): String = {
    import Json._

    val familyUsageFields: Seq[(String, String)] =
      summary.quotaUsageByFamily.toSeq.sortBy(_._1).map { case (family, (used, quota)) =>
        val pct = if (quota > 0) f"${used * 100.0 / quota}%.1f" else "N/A"
        family -> obj(
          "used_cores"  -> num(used),
          "quota_cores" -> num(quota),
          "pct_used"    -> str(pct)
        )
      }

    val quotaArray: String = arr(summary.quotas.productIterator.toList.indices.map { _ => "" }: _*)  // unused

    val entriesArr: String = arr(summary.entries.map { e =>
      obj(
        "cluster_name"           -> str(e.clusterName),
        "worker_machine_type"    -> str(e.workerMachineType),
        "worker_family"          -> str(e.workerFamily),
        "num_workers"            -> num(e.numWorkers),
        "max_workers_from_policy"-> num(e.maxWorkersFromPolicy),
        "total_cores"            -> num(e.totalCores),
        "max_total_cores"        -> num(e.maxTotalCores),
        "strategy"               -> str(e.strategyName),
        "bias_mode"              -> str(e.biasMode),
        "topology_preset"        -> str(e.topologyPreset),
        "diagnostic_signals"     -> arr(e.diagnosticSignals.map(Json.str): _*)
      )
    }: _*)

    Json.pretty(obj(
      "generated_at"                       -> str(summary.generatedAt),
      "date"                               -> str(summary.date),
      "strategy"                           -> str(summary.strategyName),
      "bias_mode"                          -> str(summary.biasMode),
      "topology_preset"                    -> str(summary.topologyPreset),
      "total_clusters"                     -> num(summary.totalClusters),
      "total_predicted_nodes"              -> num(summary.totalPredictedNodes),
      "total_max_nodes"                    -> num(summary.totalMaxNodes),
      "clusters_with_diagnostic_overrides" -> num(summary.clustersWithDiagnosticOverrides),
      "quota_usage_by_family"              -> obj(familyUsageFields: _*),
      "clusters"                           -> entriesArr
    ))
  }

  def writeSummary(outDir: File, summary: GenerationSummary): Unit = {
    if (!outDir.exists()) outDir.mkdirs()

    val jsonFile = new File(outDir, "_generation_summary.json")
    val bwJson = new BufferedWriter(new FileWriter(jsonFile))
    try {
      bwJson.write(toJson(summary))
      logger.info(s"Wrote generation summary JSON: ${jsonFile.getPath}")
    } finally bwJson.close()

    val csvFile = new File(outDir, "_generation_summary.csv")
    val bwCsv = new BufferedWriter(new FileWriter(csvFile))
    try {
      bwCsv.write("cluster_name,worker_machine_type,worker_family,num_workers,max_workers_from_policy," +
        "total_cores,max_total_cores,strategy,bias_mode,topology_preset,diagnostic_signals\n")
      summary.entries.foreach { e =>
        val signals = e.diagnosticSignals.mkString("; ").replace(",", ";")  // avoid CSV comma in signals
        bwCsv.write(
          s"""${e.clusterName},${e.workerMachineType},${e.workerFamily},${e.numWorkers},""" +
          s"""${e.maxWorkersFromPolicy},${e.totalCores},${e.maxTotalCores},""" +
          s"""${e.strategyName},${e.biasMode},${e.topologyPreset},"$signals"\n"""
        )
      }
      logger.info(s"Wrote generation summary CSV: ${csvFile.getPath} rows=${summary.entries.size}")
    } finally bwCsv.close()
  }
}
