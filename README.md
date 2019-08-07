
https://github.com/steveloughran/winutils/blob/master/hadoop-2.7.1/bin/winutils.exe


C:\development\programs\hadoop-2.7.1\bin


```
HADOOP_HOME=C:\development\programs\hadoop-2.7.1
```

```
winutils chmod -R 777 C:\tmp\hive
```

Schema auto-scan
JSON Line
CSV with CR
JDBC

http://localhost:4040

# Broadcast Hash Join

See `BroadcastHashJoinSpec` class

* **LocalTableScan** \
  [`id`#2L, `name`#3]
  
* **LocalTableScan** \
  [`id`#7L, `customer_id`#8L]
  
* **BroadcastExchange** \
  HashedRelationBroadcastMode(List(input[0, bigint, false]))
  
* **BroadcastHashJoin** \
  [`id`#2L], [`customer_id`#8L], Inner, BuildLeft
  
* **Project** \
  [`id`#2L AS `customer_id`#24L, `name`#3, `id`#7L AS `order_id`#25L]
  
# Shuffled Hash Join

See `ShuffledHashJoinSpec` class

* **LocalTableScan** \
  [`id`#2L, `name`#3]

* **LocalTableScan** \
  [`id`#7L, `customer_id`#8L]

* **Exchange** \
  hashpartitioning(`id`#2L, 100)
  
* **Exchange** \
  hashpartitioning(`customer_id`#8L, 100)
  
* **ShuffledHashJoin** \
  [`id`#2L], [`customer_id`#8L], Inner, BuildLeft
  
* **Project** \
  [`id`#2L AS `customer_id`#24L, `name`#3, `id`#7L AS `order_id`#25L]

# Sort Merge Join

See `SortMergeJoinSpec` class

* **LocalTableScan** \
  [`id`#2L, `name`#3]
  
* **LocalTableScan** \
  [`id`#7L, `customer_id`#8L]
  
* **Exchange** \
  hashpartitioning(`id`#2L, 200)
  
* **Exchange** \
  hashpartitioning(`customer_id`#8L, 200)
  
* **Sort** \
  [`id`#2L ASC NULLS FIRST], false, 0
  
* **Sort** \
  [`customer_id`#8L ASC NULLS FIRST], false, 0
  
* **SortMergeJoin** \
  [`id`#2L], [`customer_id`#8L], Inner
  
* **Project** \
  [`id`#2L AS `customer_id`#24L, `name`#3, `id`#7L AS `order_id`#25L]

# Partitioning

See `PartitioningSpec` class

## Without Partitioning

* **FileScan parquet** \
  `default.country_customers_no_partition` \
  [`id`#13L,`name`#14,`country`#15] \
  Batched: true, Format: Parquet, \
  Location: **InMemoryFileIndex**[file:/C:/development/presentations/spark-perf/spark-warehouse/country_customers..., \
  PartitionFilters: [], \
  PushedFilters: [IsNotNull(`country`), EqualTo(`country`,France)], \
  ReadSchema: struct<`id`:bigint,`name`:string,`country`:string>
  
* **Filter** \
  (isnotnull(`country`#15) && (`country`#15 = France))
  
* **Project** \
  [`id`#13L, `name`#14, `country`#15]

## With Partitioning

* **FileScan parquet** \
  default.country_customers_partition \
  [`id`#33L,`name`#34,`country`#35] \
  Batched: true, Format: Parquet, \
  Location: **PrunedInMemoryFileIndex**[file:/C:/development/presentations/spark-perf/spark-warehouse/country_cus..., \
  **PartitionCount: 1**, \
  **PartitionFilters: [isnotnull(`country`#35), (`country`#35 = France)]**, \
  PushedFilters: [], \
  ReadSchema: struct<`id`:bigint,`name`:string>

# Bucketing

See `BucketingSpec` class

## Without bucketing

* **FileScan parquet** \
  `default.orders_no_bucket` \
  [`id`#8L,`customer_id`#9L] \
  Batched: true, Format: Parquet, \
  Location: InMemoryFileIndex[file:/C:/development/presentations/spark-perf/spark-warehouse/orders_no_bucket], \
  PartitionFilters: [], \
  **PushedFilters: [In(`customer_id`, [1,2,3,4,5,6,7,8,9,10])]**, \
  ReadSchema: struct<`id`:bigint,`customer_id`:bigint>

* **Filter** \
  `customer_id`#9L IN (1,2,3,4,5,6,7,8,9,10)

* **HashAggregate** \
  (keys=[`customer_id`#9L], \
  functions=[partial_count(`id`#8L)], \
  output=[`customer_id`#9L, `count`#19L])

* **Exchange** \
  hashpartitioning(`customer_id`#9L, 200)

* **HashAggregate** \
  (keys=[`customer_id`#9L], \
  functions=[count(`id`#8L)], \
  output=[`customer_id`#9L, `order_count`#15L])

## With Bucketing

* **FileScan parquet** \
  `default.orders_bucket` \
  [`id`#29L,`customer_id`#30L] \
  Batched: true, Format: Parquet, \
  Location: InMemoryFileIndex[file:/C:/development/presentations/spark-perf/spark-warehouse/orders_bucket], \
  PartitionFilters: [], \
  **PushedFilters: [In(`customer_id`, [1,2,3,4,5,6,7,8,9,10])]**, \
  ReadSchema: struct<`id`:bigint,`customer_id`:bigint>, \
  **SelectedBucketsCount: 7 out of 10**

* **Filter** \
  `customer_id`#30L IN (1,2,3,4,5,6,7,8,9,10)

* **HashAggregate** \
  (keys=[`customer_id`#30L], \
  functions=[**partial_count**(`id`#29L)], \
  output=[`customer_id`#30L, `count`#40L])

* **HashAggregate** \
  (keys=[`customer_id`#30L], \
  functions=[**count**(`id`#29L)], \
  output=[`customer_id`#30L, `order_count`#36L])

# Coalescing and Repartitioning

See `CoalesceRepartitionSpec` class

## Neither coalescing nor repartitioning

* **Scan** \
  [obj#2]

**Stage 0** (8 tasks)

* **SerializeFromObject** \
  [assertnotnull(input[0, Order, true]).id AS `id`#3L, assertnotnull(input[0, Order, true]).customer_id AS `customer_id`#4L]
  
* **Project** \
  [`customer_id`#4L]

* **HashAggregate** \
  (keys=[`customer_id`#4L], \
  functions=[partial_count(1)], \
  output=[`customer_id`#4L, `count`#15L])
  
* **Exchange** \
  hashpartitioning(`customer_id`#4L, 200)

**Stage 1** (200 tasks)

* **HashAggregate** \
  (keys=[`customer_id`#4L], \
  functions=[count(1)], \
  output=[`customer_id`#4L, `order_count`#9L])
  
* **Execute CreateDataSourceTableAsSelectCommand** \
  `order_counts`, Overwrite, [`customer_id`, `order_count`]
 
## Coalescing

* **Scan** \
  [obj#18]

**Stage 2** (8 tasks)

* **SerializeFromObject** \
  [assertnotnull(input[0, Order, true]).id AS `id`#19L, assertnotnull(input[0, Order, true]).customer_id AS `customer_id`#20L]

* **Project** \
  [`customer_id`#20L]

* **HashAggregate** \
  (keys=[`customer_id`#20L],
  functions=[partial_count(1)],
  output=[`customer_id`#20L, `count`#31L])

* **Exchange** \
  hashpartitioning(`customer_id`#20L, 200)

**Stage 3** (20 tasks)

* **HashAggregate** \
  (keys=[`customer_id`#20L], \
  functions=[count(1)], \
  output=[`customer_id`#20L, `order_count`#25L])
  
* **Coalesce** \
  20

* **Execute CreateDataSourceTableAsSelectCommand** \
  `order_counts_coalesce`, Overwrite, [`customer_id`, `order_count`]

## Repartitioning

* **Scan** \
  [obj#34]

**Stage 4** (8 tasks)

* **SerializeFromObject** \
  [assertnotnull(input[0, Order, true]).id AS `id`#35L, assertnotnull(input[0, Order, true]).customer_id AS `customer_id`#36L]

* **Project** \
  [`customer_id`#36L]
  
* **HashAggregate** \
  (keys=[`customer_id`#36L], \
  functions=[partial_count(1)], \
  output=[`customer_id`#36L, count#47L])

* **Exchange** \
  hashpartitioning(`customer_id`#36L, 200)

**Stage 5** (200 tasks)

* **HashAggregate** \
  (keys=[`customer_id`#36L], \
  functions=[count(1)], \
  output=[`customer_id`#36L, `order_count`#41L])

* **Exchange** \
  RoundRobinPartitioning(20)

**Stage 6** (20 tasks)

* **Execute CreateDataSourceTableAsSelectCommand** \
  `order_counts_repartition`, Overwrite, [`customer_id`, `order_count`]

# Join Skew

See `JoinSkewSpec` class

## Observing skew

**Stage 2**

![Skew Event Timeline](skew-event-timeline.png)

![Skew Summary Metrics](skew-summary-metrics.png)

## Fixing skew with salting

**Stage 5**

![Salting Event Timeline](salting-event-timeline.png)

![Salting Summary Metrics](salting-summary-metrics.png)
