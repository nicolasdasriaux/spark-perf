# Hints for Profiling with Spark UI

Hints will help you navigating and finding information in Spark UI.

**Warning icons** :warning: signal elements that are important to be aware of.

* [Partitioning](partitioning/hints.md)
* [Bucketing](bucketing/hints.md)
* [Coalescing and Repartitioning](coalescing-and-repartitioning/hints.md)
* [Reading CSV](reading-csv/hints.md)
* [Reading JSON](reading-json/hints.md)
* [Broadcast Hash Join](broadcast-hash-join/hints.md)
* [Shuffled Hash Join](shuffled-hash-join/hints.md)
* [Sort Merge Join](sort-merge-join/hints.md)
* [Join Skew](#join-skew)

## Join Skew

### Observing skew

**Details for Query 0**

* Click on Job **0** link

**Details for Job 0**

* Click on **Stage 2** (longest running stage) in **Event Timeline** after unfolding
* Or click on **Stage 2** (stage where join is performed) in **DAG Visualization**

**Details for Stage 2**

* **Event Timeline** diagram

  ![Skew Event Timeline](join-skew/skew-event-timeline.png)

* **Summary Metrics for 200 Completed Tasks** table

  ![Skew Summary Metrics](join-skew/skew-summary-metrics.png)

* **Tasks** table

### Fixing skew with salting

**Details for Query 1**

* Click on Job **1** link

**Details for Job 1**

* Click on **Stage 5** (longest running stage) in **Event Timeline** after unfolding
* Or click on **Stage 5** (stage where join is performed) in **DAG Visualization**

**Details for Stage 5**

* **Event Timeline** diagram

  ![Salting Event Timeline](join-skew/salting-event-timeline.png)

* **Summary Metrics for 200 Completed Tasks** table

  ![Salting Summary Metrics](join-skew/salting-summary-metrics.png)

* **Tasks** table
