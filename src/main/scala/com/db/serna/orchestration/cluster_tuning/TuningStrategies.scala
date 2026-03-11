package com.db.serna.orchestration.cluster_tuning

// ── Executor topology preset ──────────────────────────────────────────────────
// memoryPerCoreGb = GB per executor core.  totalMemoryGb = cores * memoryPerCoreGb.
// Example: 8cx1GBpc → 8 cores, 8 GB total (matches current policy defaults).
final case class ExecutorTopologyPreset(cores: Int, memoryPerCoreGb: Int) {
  require(cores > 0 && memoryPerCoreGb > 0, "cores and memoryPerCoreGb must be positive")
  def totalMemoryGb: Int = cores * memoryPerCoreGb
  def label: String = s"${cores}cx${memoryPerCoreGb}GBpc"
}

object ExecutorTopologyPreset {
  val Cores8MemPC1GB: ExecutorTopologyPreset = ExecutorTopologyPreset(8, 1)  // 8 GB total  — default (current)
  val Cores8MemPC2GB: ExecutorTopologyPreset = ExecutorTopologyPreset(8, 2)  // 16 GB total
  val Cores8MemPC4GB: ExecutorTopologyPreset = ExecutorTopologyPreset(8, 4)  // 32 GB total
  val Cores4MemPC1GB: ExecutorTopologyPreset = ExecutorTopologyPreset(4, 1)  // 4 GB total
  val Cores4MemPC2GB: ExecutorTopologyPreset = ExecutorTopologyPreset(4, 2)  // 8 GB total
  val Default: ExecutorTopologyPreset = Cores8MemPC1GB

  def fromLabel(label: String): Option[ExecutorTopologyPreset] = label match {
    case "8cx1GBpc" => Some(Cores8MemPC1GB)
    case "8cx2GBpc" => Some(Cores8MemPC2GB)
    case "8cx4GBpc" => Some(Cores8MemPC4GB)
    case "4cx1GBpc" => Some(Cores4MemPC1GB)
    case "4cx2GBpc" => Some(Cores4MemPC2GB)
    case _          => None
  }
}

// ── Bias mode: scoring weight presets ────────────────────────────────────────
sealed trait BiasMode {
  def costWeight: Double
  def sufficiencyWeight: Double
  def utilizationWeight: Double
  def workerPenaltyWeight: Double
  def oversizePenaltyWeight: Double
  def concurrencyBufferPct: Double
  def name: String
}

// Prefer lower hourly cost; smaller concurrency buffer; penalise large worker counts harder.
case object CostBiased extends BiasMode {
  val costWeight           = 0.7
  val sufficiencyWeight    = 0.3
  val utilizationWeight    = 0.2
  val workerPenaltyWeight  = 0.4
  val oversizePenaltyWeight = 0.3
  val concurrencyBufferPct = 0.10
  val name                 = "cost_biased"
}

// Prefer enough headroom and high utilisation; larger concurrency buffer; cost is secondary.
case object PerformanceBiased extends BiasMode {
  val costWeight           = 0.1
  val sufficiencyWeight    = 0.8
  val utilizationWeight    = 0.5
  val workerPenaltyWeight  = 0.1
  val oversizePenaltyWeight = 0.05
  val concurrencyBufferPct = 0.40
  val name                 = "performance_biased"
}

// Balanced — matches the legacy hardcoded weights exactly for full backward compatibility.
case object CostPerformanceBalance extends BiasMode {
  val costWeight           = 0.4
  val sufficiencyWeight    = 0.5
  val utilizationWeight    = 0.3
  val workerPenaltyWeight  = 0.3
  val oversizePenaltyWeight = 0.2
  val concurrencyBufferPct = 0.25
  val name                 = "cost_performance_balance"
}

object BiasMode {
  def fromName(name: String): Option[BiasMode] = name.toLowerCase match {
    case "cost_biased" | "cost"        => Some(CostBiased)
    case "performance_biased" | "perf" => Some(PerformanceBiased)
    case "balance" | "default"         => Some(CostPerformanceBalance)
    case _                             => None
  }
}

// ── Machine family preference ─────────────────────────────────────────────────
// preferredCores: machines with exactly this core count receive a score bonus.
// allowedFamilies: candidate list is filtered to these families only.
// familyPriority: tie-break within equal scores (lower number = higher priority).
// c3/c4MaxClusters: hard cap on number of clusters that may use c3/c4.
// excludedFamilies: always filtered out before scoring.
final case class MachineSelectionPreference(
  preferredCores: Int,
  allowedFamilies: List[String],
  familyPriority: Map[String, Int],
  c3MaxClusters: Int,
  c4MaxClusters: Int,
  excludedFamilies: Set[String],
  // Weight applied to the dynamic quota-pressure term in the scoring formula.
  // Penalty = familyPriorityWeight * (usedCores / quotaCores) per family.
  // Families that have consumed more of their proportional quota receive a higher penalty,
  // distributing allocations across N2/N2D/E2 roughly in ratio to their quota limits.
  // 0.20 keeps quota balancing competitive with cost while avoiding lock-in to any one family.
  familyPriorityWeight: Double = 0.20
)

object MachineSelectionPreference {
  // Priority: N2-32 > N2D-32 > E2-32; C3/C4 exceptional (max 1 each); N4/N4D excluded.
  val Default: MachineSelectionPreference = MachineSelectionPreference(
    preferredCores   = 32,
    allowedFamilies  = List("n2", "n2d", "e2", "c3", "c4"),
    familyPriority   = Map("n2" -> 1, "n2d" -> 2, "e2" -> 3, "c3" -> 4, "c4" -> 5),
    c3MaxClusters    = 1,
    c4MaxClusters    = 1,
    excludedFamilies = Set("n4", "n4d")
  )
}

// ── GCP core quotas per family (europe-west3 approximations) ─────────────────
final case class Quotas(
  e2: Int  = 5000,
  n2: Int  = 5000,
  n2d: Int = 3000,
  c3: Int  = 500,
  c4: Int  = 500,
  n4: Int  = 500,
  n4d: Int = 500
) {
  def forFamily(family: String): Int = family.toLowerCase match {
    case "e2"  => e2
    case "n2"  => n2
    case "n2d" => n2d
    case "c3"  => c3
    case "c4"  => c4
    case "n4"  => n4
    case "n4d" => n4d
    case _     => 0
  }
}

object Quotas {
  val Default: Quotas = Quotas()
}

// ── Tuning strategy ───────────────────────────────────────────────────────────
// A TuningStrategy bundles topology, bias mode, machine preferences, quotas, and
// all per-run policy knobs. toTuningPolicy() adapts them to the existing TuningPolicy
// case class so that all downstream methods (chooseMachines, planManualRecipes, etc.)
// require no internal changes.
trait TuningStrategy {
  def name: String
  def biasMode: BiasMode
  def executorTopology: ExecutorTopologyPreset
  def machinePreference: MachineSelectionPreference
  def quotas: Quotas

  // Policy knobs (non-bias fields)
  def capHitBoostPct: Double
  def capHitThreshold: Double
  def preferMaxWorkers: Int
  def perWorkerPenaltyPct: Double
  def memoryOverheadRatio: Double
  def osAndDaemonsReserveGb: Int
  def manualInstancesFrom: String   // "p95" | "avg" | "round_up_p95"
  def minExecutorInstances: Int
  def daMinFrom: String             // "avg" | "fixed2"
  def daInitialEqualsMin: Boolean

  final def toTuningPolicy(defaultMachine: MachineType): TuningPolicy = TuningPolicy(
    executorCores             = executorTopology.cores,
    executorMemoryGb          = executorTopology.totalMemoryGb,
    memoryOverheadRatio       = memoryOverheadRatio,
    osAndDaemonsReserveGb     = osAndDaemonsReserveGb,
    defaultWorker             = defaultMachine,
    defaultMaster             = defaultMachine,
    manualInstancesFrom       = manualInstancesFrom,
    minExecutorInstances      = minExecutorInstances,
    daMinFrom                 = daMinFrom,
    daInitialEqualsMin        = daInitialEqualsMin,
    capHitBoostPct            = capHitBoostPct,
    capHitThreshold           = capHitThreshold,
    preferMaxWorkers          = preferMaxWorkers,
    perWorkerPenaltyPct       = perWorkerPenaltyPct,
    costWeight                = biasMode.costWeight,
    concurrencyBufferPct      = biasMode.concurrencyBufferPct,
    preferEightCoreExecutors  = executorTopology.cores == 8,
    sufficiencyWeight         = biasMode.sufficiencyWeight,
    utilizationWeight         = biasMode.utilizationWeight,
    workerPenaltyWeight       = biasMode.workerPenaltyWeight,
    oversizePenaltyWeight     = biasMode.oversizePenaltyWeight
  )
}

// ── Default strategy: backward-compatible with the pre-refactoring hardcoded policy ──
object DefaultTuningStrategy extends TuningStrategy {
  val name                  = "default"
  val biasMode              = CostPerformanceBalance
  val executorTopology      = ExecutorTopologyPreset.Default   // 8cx1GBpc = 8 GB
  val machinePreference     = MachineSelectionPreference.Default
  val quotas                = Quotas.Default
  val capHitBoostPct        = 0.20
  val capHitThreshold       = 0.30
  val preferMaxWorkers      = 6
  val perWorkerPenaltyPct   = 0.05
  val memoryOverheadRatio   = 0.10
  val osAndDaemonsReserveGb = 4
  val manualInstancesFrom   = "p95"
  val minExecutorInstances  = 1
  val daMinFrom             = "avg"
  val daInitialEqualsMin    = true
}

// ── Cost-biased strategy ──────────────────────────────────────────────────────
object CostBiasedStrategy extends TuningStrategy {
  val name                  = "cost_biased"
  val biasMode              = CostBiased
  val executorTopology      = ExecutorTopologyPreset.Cores8MemPC1GB
  val machinePreference     = MachineSelectionPreference.Default
  val quotas                = Quotas.Default
  val capHitBoostPct        = 0.10   // smaller boost: prefer tighter allocation
  val capHitThreshold       = 0.40
  val preferMaxWorkers      = 4      // prefer even smaller clusters
  val perWorkerPenaltyPct   = 0.08
  val memoryOverheadRatio   = 0.10
  val osAndDaemonsReserveGb = 4
  val manualInstancesFrom   = "p95"
  val minExecutorInstances  = 1
  val daMinFrom             = "avg"
  val daInitialEqualsMin    = true
}

// ── Performance-biased strategy ───────────────────────────────────────────────
object PerformanceBiasedStrategy extends TuningStrategy {
  val name                  = "performance_biased"
  val biasMode              = PerformanceBiased
  val executorTopology      = ExecutorTopologyPreset.Cores8MemPC2GB  // 16 GB total — more headroom
  val machinePreference     = MachineSelectionPreference.Default
  val quotas                = Quotas.Default
  val capHitBoostPct        = 0.30   // larger boost when hitting cap
  val capHitThreshold       = 0.20   // trigger boost sooner
  val preferMaxWorkers      = 8      // tolerate more workers
  val perWorkerPenaltyPct   = 0.02
  val memoryOverheadRatio   = 0.10
  val osAndDaemonsReserveGb = 4
  val manualInstancesFrom   = "p95"
  val minExecutorInstances  = 2
  val daMinFrom             = "avg"
  val daInitialEqualsMin    = false  // initial > min for faster scale-up
}

object TuningStrategy {
  def fromName(name: String): Option[TuningStrategy] = name.toLowerCase match {
    case "default"             => Some(DefaultTuningStrategy)
    case "cost_biased" | "cost" => Some(CostBiasedStrategy)
    case "performance_biased" | "perf" => Some(PerformanceBiasedStrategy)
    case _                     => None
  }

  val all: List[TuningStrategy] = List(DefaultTuningStrategy, CostBiasedStrategy, PerformanceBiasedStrategy)
}
