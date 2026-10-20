# Lab 6: Introduction to Spark's RDD API

Report for the lab [`06.spark-introduction/lab-pyspark-rdd.md`](https://github.com/adaltas/ece-bigdata-2026-fall/blob/main/06.spark-introduction/lab-pyspark-rdd.md), completed on Onyxia (`vscode-pyspark` service) with PySpark 4.1.1 and Python 3.13, in the `pyspark --master local[*]` shell, on local copies of `bronze/users.csv` and `bronze/orders.csv` downloaded from the bucket `user-l-schuttig-ece`.

The code of the lab and of the exercises is in [`lab6/rdd_lab.py`](./lab6/rdd_lab.py). It runs end to end with `spark-submit lab6/rdd_lab.py`.

## Results by section

| Section | Result |
| --- | --- |
| Environment | `users.csv` (7,351 bytes) and `orders.csv` (331,568 bytes) downloaded from the bronze layer. |
| RDD creation and partitions | `textFile()` creates 2 partitions for each file. `repartition(8)` gives 8 partitions. |
| Lazy evaluation and lineage | The lineage of `orders_data` is `HadoopRDD[2]` → `MapPartitionsRDD[3]` → `PythonRDD[10]`, with 2 partitions, before any data is read. `count()` returns 2829 twice. The Spark UI shows one job per `count()`, each with 1 stage of 2 tasks (0.4 s then 0.2 s), plus the job of `first()` with a single task. |
| Multiline address | `users_lines.count()` returns 101: 1 header + 50 users × 2 physical lines. Filtering on the leading UUID keeps 50 records. |
| Manual parsing of orders | Each order becomes a dict: `quantity` is an `int`, `date` stays a string. |
| `reduceByKey` vs `groupByKey` | Same totals per user (e.g. `f143262f-…`: 12). Shuffle write: **2.5 KiB** with `reduceByKey` (stage 7) vs **6.1 KiB** with `groupByKey` (stage 9). |
| Manual deduplication | 2829 orders before and after: the generator produces no duplicates. |
| Join | `(user_id, (order, username))`, e.g. `kayla51`. `join` and `leftOuterJoin` both return 2829 rows. The join shuffles both sides: 178.2 KiB of shuffle write (stage 14), against 2.5 KiB for `reduceByKey`, because whole order records are moved. |

## Deviations from the instructions

- **Spark UI.** `http://localhost:4040` is not reachable from the browser on Onyxia. The UI is exposed by the second link of the service, `https://user-l-schuttig-ece-235107-spark.lab.adm.adaltas.cloud/`. The `/proxy/4040/` path of VS Code loses its prefix on the redirect to `/jobs/`.
- **Exercise 3.** The CSV column is `user_uuid`: `user_id` only exists in the RDD, renamed by `parse_order`. `groupBy("user_id")` fails with `UNRESOLVED_COLUMN.WITH_SUGGESTION`.
- **Commit.** The lab has no commit section: the code is saved in `lab6/rdd_lab.py` and this report in `LAB6.md`.

## Answers to the questions

**What decides the number of partitions `textFile()` picks by default, for a file this size?**
`textFile()` uses `minPartitions = min(defaultParallelism, 2)`, so 2. The file (332 KB) is much smaller than an input split (the block size, 32 to 128 MB), so the minimum decides, not the size of the file. A large file would get about one partition per split.

**`repartition()` and `coalesce()` both change the partition count. Which one shuffles data, and which one doesn't?**
`repartition()` always performs a full shuffle and redistributes the records evenly. `coalesce(n)`, when reducing the number of partitions, merges existing partitions without a shuffle: cheaper, but it cannot increase the number of partitions evenly.

**Call `orders_data.count()` a second time. Does the Spark UI show a new stage? Why?**
Yes: a new job with a new stage of 2 tasks (jobs 1 and 2). `orders_data` is not cached, so every action replays the whole lineage, reading and filtering the file again.

**At which point would `.cache()` change that answer?**
Calling `orders_data.cache()` before the first `count()`. The first action then stores the partitions in memory (Storage tab) and the second one reads them instead of the file.

**Why does `spark.read.csv(multiLine=True)` not have this problem, while `sc.textFile()` does?**
The CSV reader parses the CSV syntax: a line break inside quotes belongs to the value, not to the end of the record. With `multiLine=True`, each file is also read as a whole, so a record is never split between two partitions. `textFile()` only knows line breaks and ignores quotes.

**What has to be true of a file format for a line-oriented reader like `textFile()` to be safe to use on it?**
One record per line: no raw line break inside a value (escaped as `\n` instead), as in JSON Lines or a CSV without multiline fields. Every line can then be parsed independently, and the file can be split at any line boundary.

**What would happen if one line in `orders.csv` had an extra comma inside `product`?**
`line.split(",")` would return 6 fields, and the unpacking would raise `ValueError: too many values to unpack`. Because of lazy evaluation, the error would appear at the first action reaching that partition, not when `orders_rdd` is defined: the task fails, is retried, and the job fails. A real CSV parser (the `csv` module) and a `try/except` would be needed.

**Where does the DataFrame API perform the equivalent check, and when?**
The CSV reader checks every row against the schema, declared or inferred, during the scan, and the `mode` option decides what happens to a malformed row: `PERMISSIVE` (default, invalid fields become `null`, the raw row can be kept in `_corrupt_record`), `DROPMALFORMED` or `FAILFAST`. Column names are checked even earlier, when the plan is analyzed, before any data is read: exercise 3 failed immediately with `UNRESOLVED_COLUMN`, while a typo in an RDD lambda only fails inside a task.

**`reduceByKey` combines values on each partition before shuffling. What does `groupByKey` shuffle instead, and why is that more expensive?**
`groupByKey` shuffles every `(user_id, quantity)` pair, 2829 records, while `reduceByKey` sends one partial sum per user and per partition, at most 100 records: 6.1 KiB against 2.5 KiB of shuffle write here. `groupByKey` also keeps all the values of a key in memory on the receiving side, which can exhaust it with a skewed key.

**For which aggregations would `groupByKey` be unavoidable?**
When the computation needs all the values at once and cannot be built from partial results: an exact median or quantiles, the complete sorted list of the orders of a customer, or any function which is not associative. `reduceByKey` requires an associative and commutative function (sum, min, max).

**What plays the role of `partitionBy` here?**
The shuffle of `reduceByKey`: the pairs are hash-partitioned on `order_id`, so all the versions of an order land on the same partition, where `most_recent` reduces them two by two, replacing `ORDER BY ordered_at DESC` and `row_number() = 1`.

**Which version states the intent more directly?**
The window function: `row_number() OVER (PARTITION BY order_id ORDER BY ordered_at DESC) = 1` reads as the business rule. The RDD version hides it in a custom combiner, compares dates as strings (correct only because of the ISO format) and cannot be optimized by Catalyst.

**What would need to be true of how `orders_kv` and `users_kv` are partitioned for Spark to skip the shuffle?**
Both RDDs must be co-partitioned: partitioned with the same partitioner, for example `partitionBy(n)` on `user_id` with the same `n`, and cached. The join then becomes a narrow dependency. With a table as small as `users` (50 rows), a broadcast join is the usual alternative: broadcast a dict of the users and look them up in a `map` over the orders.

**Replace `join()` with `leftOuterJoin()`. What changes if a `user_id` in `orders` has no matching user?**
The inner `join()` silently drops the order. `leftOuterJoin()` keeps it with `None` as the username: `(user_id, (order, None))`. Here both return 2829 rows, every order having a user (see exercise 2).

**What is the risk of calling `.collect()` on `enriched`, on a much larger dataset?**
`collect()` brings every element of every partition into the memory of the single driver: an `OutOfMemoryError`, and a network transfer which defeats distributed processing. Filter or aggregate first, use `take(n)`, or write the result to storage.

## Exercises

### 1. `aggregateByKey`

`aggregateByKey((0, 0), seqOp, combOp)` computes `(order_count, total_quantity)` per user in a single pass: `seqOp` adds an order to the accumulator inside a partition, `combOp` merges the accumulators of two partitions after the shuffle.

| User | (orders, quantity) | Average quantity |
| --- | --- | --- |
| `f143262f-…` (`kayla51`) | (4, 12) | 3.0 |
| `969b6662-…` | (12, 40) | 3.33 |
| `ce9e1a11-…` | (85, 253) | 2.98 |

The totals match `reduceByKey`.

### 2. Orders with no matching user

An order with `user_id = 00000000-0000-0000-0000-000000000000` is added with `union()`. After `leftOuterJoin(users_kv)`, filtering on `username is None` returns 1 order, `fake-order-1`. An inner `join()` would have dropped it silently.

### 3. RDD vs DataFrame

```python
orders_df = spark.read.csv("orders.csv", header=True, inferSchema=True)
user_quantity_df = orders_df.groupBy("user_uuid").sum("quantity")
```

Two lines against about fifteen for the RDD version (parsing function, header filter, `map`, `reduceByKey`), with the same totals. `explain()`:

```text
HashAggregate(keys=[user_uuid], functions=[sum(quantity)])
+- Exchange hashpartitioning(user_uuid, 200)
   +- HashAggregate(keys=[user_uuid], functions=[partial_sum(quantity)])
      +- FileScan csv [user_uuid, quantity] ReadSchema: struct<user_uuid:string,quantity:int>
```

The RDD `toDebugString` lists the RDDs: `HadoopRDD` → `MapPartitionsRDD` → `PythonRDD` (parsing, `CachedPartitions: 2; MemorySize: 168.0 KiB`) → `PairwiseRDD` → `ShuffledRDD` → `PythonRDD`. The DataFrame plan shows what Catalyst does on its own: it reads only 2 of the 5 columns (column pruning), applies a partial aggregation before the shuffle, the equivalent of `reduceByKey`, and types the columns. The RDD version parses every field of every line in Python, and only what the code explicitly asks for is optimized.

### 4. With and without cache

Measured in the shell with `time.perf_counter()`:

| Action | With `.cache()` | Without `.cache()` |
| --- | --- | --- |
| `orders_rdd.count()` | 0.430 s, 168.0 KiB read from memory (stage 33) | 0.492 s, 417.9 KiB read from the file and parsed again (stage 36) |
| `user_quantity.count()` | 0.163 s, the map stage is skipped and only 2.5 KiB of shuffle files are read (stage 35) | 0.308 s, a new `reduceByKey` stage reads 417.9 KiB (stages 37 and 38) |

With the cache, the `toDebugString` shows `CachedPartitions: 2; MemorySize: 168.0 KiB` and the Storage tab lists the RDD. `unpersist()` removes it. The gain is small and not stable on 330 KB of data: the script run gave 0.198 s cached vs 0.183 s without cache for the first count. The fixed cost of scheduling tasks and starting Python workers dominates. The volumes read show the real difference: on a large dataset, the cache avoids reading and parsing the source again for every action.
