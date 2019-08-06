
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

## Size, `broadcast` and `hint("broadcast")`

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

# Bucketing

## Without bucketing

* **FileScan parquet** \
  `default.orders_no_bucket` \
  [`id`#8L,`customer_id`#9L] \
  Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/C:/development/presentations/spark-perf/spark-warehouse/orders_no_bucket], PartitionFilters: [], \
  **PushedFilters: [In(`customer_id`, [1,2,3,4,5,6,7,8,9,10])]**, \
  ReadSchema: struct<`id`:bigint,`customer_id`:bigint>

* **Filter** \
  `customer_id`#9L IN (1,2,3,4,5,6,7,8,9,10)

* **HashAggregate**
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
  [`id`#29L,`customer_id`#30L]
  Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/C:/development/presentations/spark-perf/spark-warehouse/orders_bucket], PartitionFilters: [], \
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

# Partitioning

## Without Partitioning

* **FileScan parquet** \
  `default.country_customers_no_partition` \
  [`id`#13L,`name`#14,`country`#15] \
  Batched: true, Format: Parquet,
  Location: **InMemoryFileIndex**[file:/C:/development/presentations/spark-perf/spark-warehouse/country_customers..., \
  PartitionFilters: [], \
  PushedFilters: [IsNotNull(`country`), EqualTo(`country`,France)],
  ReadSchema: struct<`id`:bigint,`name`:string,`country`:string>
  
* **Filter** \
  (isnotnull(`country`#15) && (`country`#15 = France))
  
* **Project** \
  [`id`#13L, `name`#14, `country`#15]

## With Partitioning

* FileScan parquet \
  default.country_customers_partition \
  [`id`#33L,`name`#34,`country`#35] \
  Batched: true, Format: Parquet, \
  Location: **PrunedInMemoryFileIndex**[file:/C:/development/presentations/spark-perf/spark-warehouse/country_cus..., \
  **PartitionCount: 1**, \
  **PartitionFilters: [isnotnull(`country`#35), (`country`#35 = France)]**, \
  PushedFilters: [], \
  ReadSchema: struct<`id`:bigint,`name`:string>

# Shuffled Hash Join

# Sort Merge Join

# Join Skew

# Coalesce and Repartition
