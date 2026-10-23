# Lab 7: Bronze to silver with Spark DataFrames

Report for the lab [`07.sparksql-and-dataframes/lab-pyspark-silver.md`](https://github.com/adaltas/ece-bigdata-2026-fall/blob/main/07.sparksql-and-dataframes/lab-pyspark-silver.md), completed on Onyxia (`vscode-pyspark` service, 4 CPU and 4000 Mi) with Spark 4.1.1, Scala 2.13, Iceberg 1.11.0 (`iceberg-spark-runtime-4.1_2.13`) and the bucket `user-l-schuttig-ece`.

The code is in [`lab7/`](./lab7), executed in the PySpark shell with `exec(open(...).read())`, in this order:

| File | Content |
| --- | --- |
| [`silver_lab.py`](./lab7/silver_lab.py) | SparkSession with S3A and the Iceberg catalog `lab`, bronze layer, `stg_users` |
| [`orders.py`](./lab7/orders.py) | `stg_orders`, window deduplication, Spark SQL version, query per state |
| [`write.py`](./lab7/write.py) | Parquet and Iceberg outputs, snapshots and files of the table |
| [`exercises.py`](./lab7/exercises.py) | The 4 exercises |

The Iceberg metadata is committed in [`warehouse/`](./warehouse), the data files are excluded by `.gitignore`.

## Results by section

| Section | Result |
| --- | --- |
| Environment | Spark 4.1.1 / Scala 2.13, `hadoop-aws-3.4.2` and the AWS SDK `bundle-2.29.52` in `$SPARK_HOME/jars`. |
| SparkSession | `spark.read.csv("s3a://…/bronze/orders.csv")` returns 2829 rows with the `TemporaryAWSCredentialsProvider`. |
| Bronze layer | 50 users, 2829 orders. With `multiLine=True` each two-line address stays in one record (`908 Jennifer Squares\nRobinsonshire, KY 01352`), while `textFile()` returned 101 lines in the RDD lab. `birthdate` is inferred as `date`. |
| `stg_users` | Address split into `street`, `city`, `state`, `zip_code`. Military addresses (`Unit 6184 Box 9593\nDPO AP 09617`) get a `NULL` city. |
| `stg_orders` | 2829 orders, 2832 with 3 injected duplicates, 2829 after the window deduplication, no `order_id` left with a count above 1. |
| Spark SQL | Same deduplication with `row_number() OVER (PARTITION BY order_id ORDER BY ordered_at DESC)`: same 2829 rows and columns. |
| Query per state | Only `AE` (3 of 3) and `AP` (2 of 2) have missing cities: the military addresses. |
| Parquet | `silver/spark/users.parquet/` and `orders.parquet/`: one `part-00000-….snappy.parquet` each (9,581 and 139,556 bytes) and a `_SUCCESS` file. Read back: 50 and 2829 rows. |
| Iceberg | `lab.silver.users` and `lab.silver.orders`: one data file each (7,436 and 84,055 bytes, zstd compression) and the metadata. First snapshot `6272162234183241684`, operation `overwrite` (`createOrReplace`). Read back: 50 and 2829 rows. |
| Comparison with dbt | Same 2829 orders, same total quantity 8615, same 50 users. `dbt build`: `PASS=22`, 5.1 s (18 s including `uv` and Python startup). |

## Deviations from the instructions

- **Resources.** The first service had 2 CPU and 2000 Mi: the container was `OOMKilled` twice while Spark ran, the configuration of the service setting `spark.driver.memory: 2g`. The service was recreated with the 4 CPU and 4000 Mi required by the lab.
- **Eviction.** The new pod was evicted once: the node was low on `ephemeral-storage` and the pod used 466 MB of it with a request of 0. Everything outside `/home/onyxia/work` was lost (`~/.aws`, the Spark session). PySpark was then started with `--driver-memory 1g --conf spark.local.dir=/home/onyxia/work/.spark-tmp --conf spark.jars.ivy=/home/onyxia/work/.ivy2`, so that temporary files and the Iceberg jar live on the persistent volume.
- **Credentials.** The `AWS_*` variables of a service are set when it starts and expire after 12 hours. The session reads the credentials renewed from the Onyxia storage settings in `~/.aws/credentials` instead.
- **Session.** The `pyspark` shell creates a session at startup, which the S3A and Iceberg settings of `getOrCreate()` would not reach: it is stopped first.
- **Code execution.** Long pastes into the Python shell were truncated by the terminal: the code is written in files executed with `exec()`.
- **dbt output.** The dbt Parquet exports had been removed at the cleanup of the dbt lab. They were recreated as `silver/dbt/stg_users.parquet` and `silver/dbt/stg_orders.parquet` from `lab_medallion.duckdb`. The paths of the comparison commands, inconsistent in the instructions, were adapted.
- **Iceberg metadata commit.** `aws s3 sync` also downloads the data files: `warehouse/**/data/` was added to `.gitignore`.
- **`version-hint.text`.** `aws s3 sync` skips a file of the same size as the local copy unless it is newer locally: the 1-byte pointer went from `1` to `2` after the append and was not downloaded. It was copied explicitly with `aws s3 cp` (`--exact-timestamps` would also work).
- **Exercise 2.** `%%timeit` is a Jupyter magic: `timeit.repeat` is used instead, best of 5 runs after a warm-up.
- **Spark UI.** Exposed at `https://user-l-schuttig-ece-716131-spark.lab.adm.adaltas.cloud/`, not `localhost:4040`.

## Answers to the questions

### SparkSession

**The credentials are temporary. What happens to a running Spark job if the service is restarted mid-lab, and why?**
The job is lost. In `local[*]` mode the driver and the executors run in the JVM of the service pod: a restart kills it, with the session, the cached data and everything outside the persistent volume. This happened in this lab (`OOMKilled`, then an eviction). The new pod gets new STS credentials. Even without a restart, the credentials are copied into the session configuration when it starts: once they expire (12 hours at most), every S3A call fails with `ExpiredToken` or 403, nothing renews them.

**Why does a plain `SimpleAWSCredentialsProvider` fail here, while it worked in setups without a session token?**
It only sends the access key and the secret key. Temporary STS credentials are only valid together with their session token, and SeaweedFS rejects the requests without it (`InvalidAccessKeyId` or 403). `TemporaryAWSCredentialsProvider` adds the token to every request. Long-term keys, such as those of an IAM user, have no token, which is why the simple provider worked with them.

### Silver layer

**Explain the output of the code that was just run.**
The query counts the users per state and, among them, those with a `NULL` city. Only `AE` (3 of 3) and `AP` (2 of 2) have missing cities, and all of their addresses do: these are military addresses (`DPO AE 12345`), which have no city. The query validates the address parsing, and in particular the conversion of the empty string returned by `regexp_extract()` into `NULL`: without it, `missing_city` would be 0 everywhere.

**Compare the DataFrame and SQL cells. Which one for a one-off exploration, which one for a model reused by several colleagues?**
SQL for a one-off exploration: shorter, readable by everyone, including analysts who do not write Python. The DataFrame API for a shared model: it can be split into reusable and parameterized functions, unit tested and versioned, and the editor checks the function names. SQL organized in a tool such as dbt is the other option. Both compile to the same Catalyst plan: the choice does not change the performance.

**Where else could the empty string of `regexp_extract()` silently produce a wrong result?**
In `state` and `zip_code`, which are not converted to `NULL`. An address not matching the pattern would get an empty state: not counted as missing, grouped under `""` in the query per state, and accepted by a `not_null` test. The same `when(col == "", None)` should be applied to both columns.

### Parquet

**How many part-files are there under `users.parquet/`, and why?**
One, and also one under `orders.parquet/`. Spark writes one file per partition of the DataFrame. `users` comes from a CSV read with `multiLine=True`, which cannot be split: 1 partition, and no shuffle follows. `orders` goes through the window function, so through a shuffle into 200 partitions, but Adaptive Query Execution coalesces them into one given the small size of the data. `_SUCCESS` marks the end of the job; with the S3A committer it contains a JSON summary of the write (11 KB).

**`coalesce(1)` before `.write` would produce a single file. What do you lose on a larger dataset?**
The parallelism: a single task on a single executor writes the whole file, a bottleneck in time and memory. `coalesce` also reduces the parallelism of the operations before it in the same stage. A single huge file is also slower to read back in parallel.

### Iceberg

**Compare `silver/orders.parquet/` and `warehouse/silver/orders/`. Which files hold the data, and which ones describe the table?**
The Parquet files hold the data: `part-….parquet` on one side, `data/….parquet` on the other. The Parquet directory has no description of the table beyond the schema in the footer of each file and `_SUCCESS`. Iceberg adds `metadata/`:
- `vN.metadata.json`: schema, partitioning, list of snapshots, current snapshot;
- `snap-….avro`: the manifest list of a snapshot;
- `…-m0.avro`: the manifest, listing the data files with their column statistics;
- `version-hint.text`: the number of the current version.

**A second job overwrites the Parquet directory while a query reads it. What does the reader see? What changes with Iceberg?**
The overwrite deletes then rewrites files: the reader can hit missing files (`FileNotFoundException`), a mix of old and new files, or an empty directory. There is no isolation. With Iceberg, the writer creates new files and a new metadata version, then swaps the pointer to the current version atomically. A reader keeps the snapshot it started with, whose files stay in place until the snapshot expires.

**The pointer to the current version is a file in the bucket. What can go wrong if two jobs commit at the same time, and what would a catalog service add?**
Both jobs can write the next `metadata.json` and update `version-hint.text`. The `hadoop` catalog relies on a rename, which is not atomic on S3: one commit can silently overwrite the other, losing data or leaving an inconsistent table. A catalog service (REST catalog, Nessie, Polaris, Hive Metastore, Glue) replaces the file with an atomic compare-and-swap: the second commit fails cleanly and is retried on top of the new version. It also provides access control and lets several engines discover the tables.

### Comparison with dbt

**What differs between the two schemas? Spark has no `UUID` type: what did you do about it, and what is the consequence for the consumers?**

| Column | dbt (DuckDB) | Spark |
| --- | --- | --- |
| `order_id`, `user_id` | `UUID` | `VARCHAR` |
| `ordered_at` | `TIMESTAMP WITH TIME ZONE` | `TIMESTAMP` |
| `quantity`, `product` | `INTEGER`, `VARCHAR` | `INTEGER`, `VARCHAR` |

The identifiers are cast to strings: 36 bytes instead of 16, slower joins, no validation of the format, and a cast is needed to join with the dbt tables. `ordered_at` also loses its time zone: Spark writes its Parquet timestamps with a legacy encoding, read by DuckDB as a naive local time. The consumers must know the values are UTC; `spark.sql.parquet.outputTimestampType=TIMESTAMP_MICROS` would keep the UTC semantics.

**One object per model with dbt, one directory of part-files with Spark. Which one is easier for a consumer to read? Which one scales?**
The single file of dbt is easier: one path, one file. The directory of Spark scales: every task writes its own file in parallel, and the reads are parallelized the same way. The consumer must read the whole directory, with no consistency guarantee during a write, which a table format such as Iceberg provides.

**What did this lab use instead of the dbt tests, and what would you need to write to get the same guarantees?**
Only `assert` statements on row counts and a duplicate check after the deduplication. The equivalent checks would be: `groupBy().count()` filtered on `count > 1` for `unique`, `filter(isNull)` for `not_null`, `filter(~isin(...))` for `accepted_values`, and a `left_anti` join for `relationships`, each failing the job when it returns rows. Libraries such as Great Expectations or PyDeequ package these checks.

**What plays the role of `ref()` and `source()` in the notebook?**
The execution order of the cells, here of the files, and the Python variables passed from one to the next. Spark knows the lineage of each DataFrame, but there is no graph between models: nothing prevents running a cell before the one it depends on, and nothing rebuilds only what depends on a changed model.

**Compare the time to build the silver layer with `dbt build` and with the cells of this lab. Why are these numbers not directly comparable?**
`dbt build` takes 5.1 s, 18 s with the startup of `uv` and Python, and also runs 19 tests. On the Spark side, starting the JVM and resolving the Iceberg package alone take tens of seconds, and the cells mix display actions, Parquet and Iceberg writes and the injected duplicates. Transformations are lazy and only run at an action. With 2,829 rows, the fixed costs dominate, not the processing of the data.

**dbt with DuckDB or Spark: a daily batch of 10 GB, a daily batch of 10 TB, a team of analysts who only know SQL?**
- 10 GB: dbt with DuckDB, a single machine is enough and there is no cluster to operate.
- 10 TB: Spark, distributed, or dbt with a Spark or Trino adapter.
- Analysts who only know SQL: dbt, on the engine matching the volume.

## Exercises

### 1. `birthdate_is_valid`

The first order date of each user (`min(ordered_at)` per `user_id`) is joined to `stg_users`, and `birthdate_is_valid = coalesce(birthdate <= to_date(first_ordered_at), true)`, written once with the DataFrame API and once in Spark SQL. `exceptAll` in both directions returns `0 0`: the two versions agree.

5 users out of 50 are invalid: `nicolejohnson` (2022-12-06), `dayderek` (2026-10-05), `ronaldstephens` (2022-05-09), `cgrimes` (2024-07-18) and `charleslandry` (2025-12-03). They explain the 288 orders placed before the birth of their user, the same figures as in the dbt lab. The dbt fix also replaced the invalid birthdates with `NULL` so that the singular test passes again; here the flag is added and the birthdate is kept.

### 2. Python UDF and `pandas_udf`

| Version | 50 rows | 100,000 rows |
| --- | --- | --- |
| `regexp_extract()` | 0.124 s | 0.248 s |
| Python UDF | 0.190 s | 0.516 s |
| `pandas_udf` | 0.187 s | 0.417 s |

Best of 5 runs; the three versions return the same zip codes. `regexp_extract()` runs in the JVM with code generation, without leaving Spark. The Python UDF sends every row to a Python worker, serialized one by one, and calls the function once per row. The `pandas_udf` sends batches through Arrow and processes a whole `pandas.Series` at once: faster than the plain UDF, but still slower than the built-in function because of the transfer to Python. On 50 rows, the fixed cost of the job dominates.

### 3. Caching the bronze DataFrames

| Action | Bronze read from S3 | Bronze cached |
| --- | --- | --- |
| `stg_orders.count()` | 0.568 s | 0.405 s |
| `stg_users.count()` | 0.160 s | 0.076 s |

Materializing the cache costs 0.738 s once. After it, the scans of `users_bronze` and `orders_bronze` are replaced by in-memory relations, listed in the Storage tab of the Spark UI:

| Cached relation | Storage level | Cached partitions | Fraction cached | Size in memory | Size on disk |
| --- | --- | --- | --- | --- | --- |
| `FileScan csv … bronze/users.csv` | Disk Memory Deserialized 1x Replicated | 1 | 100 % | 9.2 KiB | 0 B |
| `FileScan csv … bronze/orders.csv` | Disk Memory Deserialized 1x Replicated | 1 | 100 % | 144.4 KiB | 0 B |

`cache()` on a DataFrame uses the `MEMORY_AND_DISK` level: the data stays in memory and only spills to disk when memory runs out. The cached orders take 144 KiB against 332 KB for the CSV file: Spark stores them in a typed, compressed columnar format instead of text. The CSV files are no longer downloaded and parsed, but `stg_orders` keeps a shuffle for the window function, which the cache does not remove.

### 4. Append and time travel

`writeTo("lab.silver.orders").append()` adds one order, `lab7-new-order-1`. The table now has 2 snapshots:

| snapshot_id | parent_id | operation | committed_at (UTC) |
| --- | --- | --- | --- |
| 6272162234183241684 | NULL | overwrite | 2026-10-23 14:03:20 |
| 51339553746962311 | 6272162234183241684 | append | 2026-10-23 14:16:10 |

The current version has 2830 rows; `SELECT count(*) FROM lab.silver.orders VERSION AS OF 6272162234183241684` returns 2829. The append wrote a second data file, a new manifest and manifest list, and `v2.metadata.json`, and moved `version-hint.text` to `2`: this metadata is committed in `warehouse/silver/orders/metadata/`.

The same append on the Parquet directory adds a second `part-00000-….snappy.parquet` file: the directory has 2830 rows in 2 files, and there is no way to read it as it was before, except by guessing and deleting the new file by hand. Iceberg keeps the history of the table in its metadata: every commit is a snapshot, which can be read with `VERSION AS OF` or `TIMESTAMP AS OF`, or restored with the `rollback_to_snapshot` procedure. The new order was added on purpose: after this exercise, the Spark outputs have one more order than the dbt outputs.
