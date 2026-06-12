package com.db.serna.orchestration.cluster_tuning.single

// ── Capacity status ──────────────────────────────────────────────────────────

sealed trait CapacityStatus { def label: String }
object CapacityStatus {
  case object Ok         extends CapacityStatus { val label = "ok"         }
  case object Clamped    extends CapacityStatus { val label = "clamped"    }
  case object Tight      extends CapacityStatus { val label = "tight"      }
  case object Infeasible extends CapacityStatus { val label = "infeasible" }
}

/**
 * Outcome of guarding one recipe. For manual recipes `newMin == newInitial == newMax` (the clamped
 * `spark.executor.instances`). Percentages are the job's max footprint as a share of the cluster's scaled-max
 * (autoscaling-policy max) cores / memory, computed from the POST-clamp executor count.
 */
final case class GuardResult(
    isManual: Boolean,
    newMin: Int,
    newInitial: Int,
    newMax: Int,
    maxCoreUsagePct: Double,
    maxMemoryUsagePct: Double,
    status: CapacityStatus
)

/**
 * Hard cap on a recipe's executor count so it can never demand more than `ratio` of what the cluster can physically
 * schedule. Capacity is computed by PER-NODE bin-packing — `maxWorkers × executorsPerNode` — NOT an aggregate
 * `totalCores/execCores`, because a memory-heavy executor fragments per node and the aggregate overestimates
 * (e.g. a 4c/18GB executor on a 48c/192GB node fits 10/node = 60 cluster-wide, but the aggregate says 64 — the extra
 * 4 would linger forever waiting for containers). The `ratio` (default 0.90) reserves headroom for `memoryOverhead`,
 * the NodeManager/OS, and the driver/AM.
 */
object CapacityGuard {

  val DefaultRatio: Double = 0.90

  private def round1(x: Double): Double = math.round(x * 10.0) / 10.0

  /** (scaledMaxCores, scaledMaxMemGb) at the cluster's autoscaling-policy max size. */
  def scaledMax(nodeCores: Int, nodeMemGb: Int, maxWorkers: Int): (Int, Int) =
    (nodeCores * maxWorkers, nodeMemGb * maxWorkers)

  def guard(
      isManual: Boolean,
      currentMin: Int,
      currentInitial: Int,
      currentMax: Int,
      execCores: Int,
      execMemGb: Int,
      nodeCores: Int,
      nodeMemGb: Int,
      maxWorkers: Int,
      ratio: Double
  ): GuardResult = {
    val (scaledCores, scaledMemGb) = scaledMax(nodeCores, nodeMemGb, maxWorkers)

    val ec = math.max(1, execCores)
    val em = math.max(1, execMemGb)
    // Physical max executors per node (no ratio) and the ratio-limited max per node.
    val perNodeRaw = math.min(nodeCores / ec, nodeMemGb / em) // integer floor for positive ints
    val perNodeRatio =
      math.min(math.floor(ratio * nodeCores / ec).toInt, math.floor(ratio * nodeMemGb / em).toInt)

    val (capExecutors, edge) =
      if (perNodeRaw <= 0) (1, CapacityStatus.Infeasible) // one executor bigger than a whole node
      else if (perNodeRatio <= 0) (math.max(1, maxWorkers), CapacityStatus.Tight) // fits raw node, not within ratio
      else (math.max(1, maxWorkers * perNodeRatio), CapacityStatus.Ok)

    val newMax = math.min(currentMax, capExecutors)
    val (nMin, nInit) =
      if (isManual) (newMax, newMax)
      else {
        val mn  = math.min(currentMin, newMax)
        val ini = math.max(mn, math.min(currentInitial, newMax))
        (mn, ini)
      }

    val corePct = if (scaledCores > 0) round1(100.0 * newMax * ec / scaledCores) else 0.0
    val memPct  = if (scaledMemGb > 0) round1(100.0 * newMax * em / scaledMemGb) else 0.0

    val status = edge match {
      case CapacityStatus.Ok => if (newMax < currentMax) CapacityStatus.Clamped else CapacityStatus.Ok
      case other             => other
    }

    GuardResult(isManual, nMin, nInit, newMax, corePct, memPct, status)
  }
}
