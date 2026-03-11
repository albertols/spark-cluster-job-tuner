The following features must be implemented soon. Refactor and redesign to:

0. Make code more modularised for new upcoming features.

1. Including a decorator in @[ClusterMachineAndRecipeTuner.scala](ClusterMachineAndRecipeTuner.scala) or similar
   structure/design pattern for quickly changing behaviour/config of the tuner. E.g, dominant by:

- jobs: 8 cores x 1 GB (current one) or 8 cores x 2 GB or 8 cores x 4 GB or 4 cores x 1 GB or 4 cores x 2 GB, etc. OR
  maybe different clusters can have different topologies.
- other similar ideas in this direction, such as dynamic config biases: cost-biased, performant-biased,
  cost-performance-balance (default), etc

HINT: we should prioritise horizontal scalability so -32 cores machines should be most dominant ones (default) so
Datarpoc autoscaler would allocate more as load is required, minimising risks of CPU quota issues.

2. Include decorator in @[ClusterMachineAndRecipeTuner.scala](ClusterMachineAndRecipeTuner.scala) or similar to refine
   specifically both cluster (master or worker) and jobs(spark job params by new .csv insights coming from future
   LogAnalytics queries' outptuts), e.g:
   a. Adding from @[b14_clusters_with_nonzero_exit_codes.sql](log_analytics/b14_clusters_with_nonzero_exit_codes.sql)
   driver
   resources for certain
   clusters @[b14_clusters_with_nonzero_exit_codes.csv](../../../../../../resources/composer/dwh/config/cluster_tuning/inputs/2025_12_20/b14_clusters_with_nonzero_exit_codes.csv)
   due to driver_exit_codes in YARN (e.g: 247, many jobs die / evicted from the dataproc
   cluster, without graceful stop, without exceptions. Reported also as "FAILED" in the GUI but logs are represented as
   usual but half way ( likely from huge concurrency in the cluster and the master can cope?). traces in Dataproc GUI
   are just the same as the logging ones. So silent FAILED. Detecting these type of things we can tweak cluster-.json
   configs in an event driven basis.
   b. Prepare and outline the design for potential future MemoryHeap, MemoryOverhead issues, etc
   c. Prepare and outline the design for potential future GC issues
   d. others LogAnalytics -> .csv inputs?

3. Adjust Machine Type Quotas (do not worry too much about this now, rough estimation in new generation_summary, and design to include in the future a UltimateTuner hourly-minute based stats, now kick off with a basic one)

```
final case class Quotas(e2: Int, n2: Int, n2d: Int, c3: Int, c4: Int, n4: Int, n4d: Int)
private val defaultQuotas = Quotas(e2 = 5000, n2 = 5000, n2d = 3000, c3 = 500, c4 = 500, n4= 500, n4d = 500)
```

NOTE_1: with priority for N2-32, N2D-32, E2-32; in this order. C4 and C3 they only should be used exceptionally in one
cluster maximum each (high demanding ones, and avoiding max number of nodes * CPU tyme less than CPU quota). N4 and N4D
should be
avoided from the time now.
NOTE_2: Maximum number of cores should not be reached, although, clusters can scale up or down so it is hard to
estimate (max
total numbers of master and workers * maximum_no_of_corresponding_auotscaling_policy)

**** OTHER REQUIREMENTS ****

- We need to include unit test to ensure these new features, covering all the scenarios, patterns and configs.
- We need to output under the corresponding output a decent generation_summary, with the config nuances, stats,
  potential prediction of usage of (considering starting times of cluster
  @/Users/serna/IdeaProjects/spark-cluster-job-tuner/src/main/resources/composer/dwh/config/_
  dag_cluster_creation_time.csv):
    - predicted total no_of_nodes in the project (including masters and workers)
    - CPU Quota usage
- Write up design document, with visual diagrams as code to make things more understable and elegant
- 
