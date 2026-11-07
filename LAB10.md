# Lab 10: Trino on the lakehouse

Report for the three labs of the module [`10.trino`](https://github.com/adaltas/ece-bigdata-2026-fall/tree/main/10.trino): [`lab-1-deployment.md`](https://github.com/adaltas/ece-bigdata-2026-fall/blob/main/10.trino/lab-1-deployment.md) (deploy Trino with Helm), [`lab-2-trino-gold.md`](https://github.com/adaltas/ece-bigdata-2026-fall/blob/main/10.trino/lab-2-trino-gold.md) (silver to gold) and [`lab-3-trino-star-schema.md`](https://github.com/adaltas/ece-bigdata-2026-fall/blob/main/10.trino/lab-3-trino-star-schema.md) (star schema), completed on Onyxia in the namespace `user-l-schuttig-ece`, with Trino 480, the Polaris catalog of the previous module and Iceberg tables in the bucket `user-l-schuttig-ece`.

| Path | Content |
| --- | --- |
| [`trino/deployment/`](./trino/deployment) | Helm chart: Trino 480, coordinator only, catalog `lakehouse` on the Polaris REST catalog |
| [`lab10/register_users.sh`](./lab10/register_users.sh) | Registers `lab.silver.users` (version 1) in Polaris as `silver.users` |
| [`lab10/trino_session.py`](./lab10/trino_session.py) | Trino connection, `sql()` and `show()` helpers |
| [`lab10/silver.py`](./lab10/silver.py) | Catalogs, types, counts, Iceberg metadata tables, plans of the silver layer |
| [`lab10/gold.py`](./lab10/gold.py) | Gold tables, validation, refresh and time travel |
| [`lab10/star.py`](./lab10/star.py) | Star schema, star queries, validation and additional checks |
| [`lab10/exercises.py`](./lab10/exercises.py) | Exercises 1 to 4 |
| [`lab10/spark_compare.py`](./lab10/spark_compare.py) | Exercise 5, in Spark through Polaris |
| [`lab_trino/gold/`](./lab_trino/gold) | Metadata of the gold tables, without the data files |

The Python files are executed with `exec(open(...).read())` in `uv run python`, after `trino_session.py`.

## Deployment (lab 1)

The `vscode-pyspark` StatefulSet was patched to the values of the lab (requests 200m and 400 Mi, limits 2 CPU and 2000 Mi, `memory.max` 2097152000), so that Trino fits in the namespace quota next to Polaris:

| Quota | Service | Polaris | PostgreSQL | Trino coordinator | Total | Quota |
| --- | --- | --- | --- | --- | --- | --- |
| `limits.memory` | 2000 Mi | 512 Mi | 256 Mi | 1 Gi | 3792 Mi | 4 Gi |
| `requests.memory` | 400 Mi | 256 Mi | 128 Mi | 224 Mi | 1008 Mi | 1 Gi |

`helm upgrade --install trino ./trino/deployment --wait` installed the release `trino` (chart 1.45.0, revision 1). `GET /v1/info` returns version `480`, state `ACTIVE`, `"starting": false`. The single coordinator also runs the tasks (`workers: 0`, `node-scheduler.include-coordinator`), with a 700 MB heap, `query.max-memory` 400 MB and `query.max-memory-per-node` 250 MB. The catalog `lakehouse` uses the Iceberg connector on the Polaris REST catalog, with the OAuth2 credential of the principal `dev` (Secret `trino-polaris-oauth`) and the S3 keys of the Secret `polaris-s3`. The chart also adds the demo catalogs `tpcds` and `tpch`.

## Results

### Silver layer (lab 2)

| Check | Result |
| --- | --- |
| `SHOW CATALOGS` | `lakehouse`, `system`, `tpcds`, `tpch` |
| `SHOW SCHEMAS FROM lakehouse` | `information_schema`, `silver`, `system` |
| `silver.orders` | 6 columns: `order_id` varchar, `user_id` varchar, `ordered_at` timestamp(6) with time zone, `quantity` integer, `product` varchar, `source` varchar |
| `silver.users` | 10 columns, as in the instructions |
| Counts | 2834 orders, 2833 distinct `order_id`, 8628 units, 50 users |
| Address parsing | `AE` 3 users / 3 missing cities, `AP` 2 / 2, other states 0 |
| Orphan orders | 0 |
| `orders$snapshots` / `$files` | 4 snapshots (overwrite, append, append, append), 4 data files (2829, 1, 3 and 1 rows) |

### Gold layer (lab 2)

| Table | Rows | Content |
| --- | --- | --- |
| `orders_by_state_month` | 51 | Orders, units and buyers per state and month |
| `top_products` | 6 | croissant 1523 units (rank 1), drink 1493, bread 1425, brioche 1424, donut 1390, cookie 1373 |
| `user_activity` | 50 | Age, orders, units, first and last order per user; top user: 100 orders, 301 units |

Validation passed: 8628 units in `silver.orders`, `orders_by_state_month` and `top_products`; 50 users and 2834 orders in `user_activity`. After the refresh, `top_products` has 3 snapshots, `append` (CTAS), `delete`, `append`, and `FOR VERSION AS OF 2770556588824467798` returns the 6 products of the first one.

### Star schema (lab 3)

| Table | Rows |
| --- | --- |
| `dim_user` | 50 |
| `dim_product` | 6 |
| `dim_date` | 119, from 2020-01-01 to 2020-04-28 |
| `fact_orders` | 2834 |

`dim_date` ends on 2020-04-28 instead of 2020-04-27: the order added in exercise 4 of the Spark lab is dated 2020-04-28. The star query rebuilds the first 10 rows of `orders_by_state_month` exactly (AL 65 orders, 199 units, 1 buyer, …). Validation passed: 2834 orders, 8628 units, 0 order without a complete dimension row. The additional checks found 1 duplicated `order_id` in `fact_orders`, and 0 duplicated `user_id`, duplicated product, wrong date key or product with wrong units.

## Deviations from the instructions

- **`silver.users` re-registered.** The cleanup of the Polaris lab drops `polaris.silver.users`, while the Trino labs read it. [`register_users.sh`](./lab10/register_users.sh) registers the version 1 of `lab.silver.users` (`v1.metadata.json`, snapshot `2479060873203483430`, the 50 users with the 10 expected columns) through the Polaris REST API, as `dev`, without copying any file.
- **Dataset.** 2834 orders instead of 5000, a `source` column and one duplicated `order_id`, all coming from the Iceberg lab. The duplicate is detected by the additional checks of the star schema.
- **Credentials.** The Polaris credentials are read from `/home/onyxia/work/.polaris-credentials.env` (see the Polaris report). The AWS credentials are exported with `eval "$(aws configure export-credentials --profile default --format env)"`.
- **Deployment files.** Extracted from the instructions by a script, as in the Polaris lab. The archive downloaded in `trino/deployment/charts/` is not committed.
- **Python shell.** The code is written in files executed with `exec()`; the writes are guarded (`CREATE TABLE IF NOT EXISTS`, checks before `INSERT`, `DELETE` and `MERGE`) so that a file can be run again.
- **Web interface.** The ingress of Trino is disabled: the history of the queries was read from `system.runtime.queries` instead of `/ui`.
- **Exercise 4.** `optimize` was also run on `orders_by_state_month`, which had several files after the `MERGE`, because `top_products` had a single file.
- **Exercise 5.** The Spark session runs with `--driver-memory 768m`, the service having 2000 Mi.
- **Gold metadata.** `lab_trino/gold/` excludes the data files, as in the instructions, and the metadata left by the table `orders_per_product` of the Polaris lab.

## Answers to the questions

### Connection

**What does the `catalog=` parameter of `connect()` change in the statements which follow?**
It sets the default catalog of the session, sent with every request: names which do not give a catalog are resolved in `lakehouse`. `silver.orders` means `lakehouse.silver.orders`, and `SHOW SCHEMAS` without `FROM` lists the schemas of `lakehouse`. No default schema is set, so a table name alone is not enough. Fully qualified names keep working, and `system.runtime.queries` still reaches the `system` catalog.

### Catalogs, schemas and tables

**Which Trino type holds `ordered_at`? Does it keep a time zone?**
`timestamp(6) with time zone`: Iceberg `timestamptz`, microsecond precision. The values are instants returned in UTC (`2020-01-01 00:43:06.694678+00:00`), unlike the plain Parquet files of the Spark lab, which DuckDB read as naive timestamps.

**What does Trino report for `order_id`?**
`varchar`. The Iceberg column is a `string`, because Spark has no `UUID` type; Trino maps the Iceberg type, not the content. Trino has a `uuid` type, but the column would have to be declared as an Iceberg `uuid` for Trino to use it.

### Query the silver layer

**Explain the output of the queries just run.**
2834 orders and 50 users, the figures of the Spark and Iceberg labs; 2833 distinct `order_id`, because the schema evolution section of the Iceberg lab copied one order with its identifier. The address query returns the same result as in Spark: only the military addresses (`AE` 3 of 3, `AP` 2 of 2) have no city. The orphan query returns 0: every order has its user, so an inner join on users loses no order.

**Which test of the dbt lab does the orphan query replace? What would you write to run it on every refresh?**
The `relationships` test of `stg_orders.user_id` towards `stg_users.user_id`, and the singular test `assert_orders_have_users`. To run it on every refresh, it must fail the pipeline: an assertion in the refresh script, run by the orchestrator right after the silver tables are written and before the gold tables are built, or a dbt project on Trino (`dbt-trino`) where `dbt build` runs the test before the gold models.

**Trino returned the results without a job to submit or a session to create. What does that tell about when to choose Trino instead of Spark?**
Trino is a long-running service: the query is sent over HTTP and answered by a cluster which is already started, with its catalogs configured. It suits interactive SQL, short queries, BI tools and many concurrent users. Spark starts a JVM and a session per application, which pays off for long transformations, Python or Scala code, and very large volumes.

### Iceberg metadata

**How many snapshots does `silver.orders` have? Which engine wrote them?**
Four, all written by Spark: the `overwrite` of `createOrReplace` and the `append` of exercise 4 in the Spark lab, then the two `append` of the Iceberg lab. Trino added a fifth one in exercise 3.

**Compare `aws s3 ls` with the `$files` table. Which one does not need access to the bucket, and why?**
The `$files` query: the client only talks to Trino, and Trino reads the manifests with its own S3 keys, located through Polaris. `aws s3 ls` needs S3 credentials on the client. The two lists also differ in nature: `$files` returns the data files of the current snapshot only, with their row counts, while a listing of the bucket also returns the files of older snapshots, of failed writes and of other tables sharing the directory.

### Query plan and execution

The distributed plan has 3 fragments: fragment 2 scans `users` (50 rows) and sends it as `BROADCAST`; fragment 1 scans `orders`, joins with `distribution = REPLICATED` and aggregates partially per state, with a dynamic filter on the 50 `user_id` pushed into the scan; fragment 0 receives the partial counts through a `RemoteSource` and computes the final aggregate. `REPLICATED` is chosen because `users` is small: copying it to every task is cheaper than repartitioning both tables by `user_id`. `EXPLAIN ANALYZE` reports 2884 input rows (2834 + 50), an execution of 3.40 s for about 270 ms of CPU, and a peak memory of 132 kB: the time is spent waiting, for instance 950 ms to generate the splits of `orders`. Each scan reads a fixed snapshot: `orders$data@5343930186918005652`, `users$data@2479060873203483430`.

### Gold layer

**Why does `user_activity` use a `LEFT JOIN` while `orders_by_state_month` uses an inner join? Which one depends on the result of the orphan query?**
`user_activity` describes every user, including users who never ordered, who would disappear from an inner join; here all 50 users have orders. `orders_by_state_month` needs the state of the user, so an order without user cannot be placed and the inner join drops it. This table depends on the orphan query: an orphan order would silently be missing from it, and its total of units would differ from the silver layer.

**`current_date` makes `age` change each day. What does it imply for the refresh of the table?**
The table is only correct on the day it is built: it must be rebuilt every day, even when no order changed, or `age` must be computed when the table is read, in a view, from `birthdate`. The value also depends on the date of the run, which makes the table impossible to reproduce. Here the 5 invalid birthdates (2022 to 2026) give ages of 0 to 4, which `current_date` hides instead of revealing.

**Where do the data files of the gold tables live, and who decided their location?**
Under `s3://user-l-schuttig-ece/warehouse/gold/<table>-<uuid>/`, for example `top_products-1cc551f44b214418892c27d3531ece24/data/`. Polaris provides the base location of the namespace, `warehouse/gold/`, and Trino adds a unique suffix to the table name (`iceberg.unique-table-location`), so that a table dropped and created again never reuses the files of the previous one.

### Refresh a gold table

**How many snapshots does the table have after the refresh, and what is the `operation` of each?**
Three: `append` (the `CREATE TABLE AS SELECT`), `delete` (the `DELETE`, a metadata-only delete of the whole file), `append` (the `INSERT`).

**Between the `DELETE` and the `INSERT`, a dashboard reads the table. What does it see? What would change with a `MERGE`?**
An empty table: the two statements are two commits, and the snapshot between them has no row. A `MERGE`, like `CREATE OR REPLACE TABLE … AS SELECT`, applies the whole change in a single commit: readers see either the old content or the new one, never an intermediate state.

### Compare with Spark

**What does each engine need to run the `top_products` query, beyond the query itself?**
Trino: a running cluster, the `trino` Python package and a user name; the S3 keys and the Polaris credential are configured once on the cluster. Spark: a session started for the run, the Iceberg runtime package downloaded at startup, the S3 keys and the Polaris credential of the user in the configuration, the memory of a driver, and a job planned and executed for the query.

**Trino or Spark: a dashboard refreshed every minute on the gold layer, a nightly transformation of 10 TB of bronze files, a data scientist exploring the silver layer with a query?**
- The dashboard: Trino, a warm cluster answering small queries in seconds for many users.
- The nightly 10 TB transformation: Spark, which retries failed tasks, spills to disk and scales its executors for a long batch.
- The data scientist: Trino for SQL exploration; Spark when the exploration continues in Python with DataFrames or machine learning.

**Trino fails the whole query when a worker is lost, unless fault-tolerant execution is enabled. Why is it acceptable for the queries of this lab, and not for a transformation of 10 TB?**
The queries of this lab last a few seconds: running them again costs little. A 10 TB transformation lasts hours on many workers, where the loss of one node becomes likely, and restarting from the beginning wastes the whole run. Spark recomputes only the lost tasks from the lineage and the shuffle files; Trino needs fault-tolerant execution, which spools the intermediate data to storage, to do the same.

### Star schema

**Are these checks sufficient to guarantee that every order is correctly represented in `fact_orders`?**
No. They compare totals and look for missing keys, so compensating errors pass. The duplicated `order_id` of this dataset passes all of them: 2834 rows in both tables, same units, every key found. Other undetected defects: two products with swapped keys (same totals, wrong product per order), a wrong `date_key` that still exists in `dim_date`, or a user dimension with two rows for one user combined with a missing order. The additional checks of [`star.py`](./lab10/star.py) add the uniqueness of `order_id` in the fact table and of the natural keys in the dimensions, the comparison of `date_key` with `ordered_at`, and the units per product compared with the silver layer; they found the duplicated `order_id`.

**What is the grain of `fact_orders`? Of `dim_date`?**
One row per order for `fact_orders`, the grain of `silver.orders`; one row per calendar day for `dim_date`.

**Which assertion fails if `silver.users` contains the same `user_id` twice? Which one fails if an order has no product (`NULL`)?**
A duplicated `user_id` gives two rows in `dim_user` and doubles the orders of that user in the join: the assertions on the number of orders and on the units fail. A `NULL` product never matches `p.product = o.product`, even if `dim_product` has a row for `NULL`: the order keeps a `NULL` `product_key`, and the assertion on unmatched orders fails.

**The surrogate keys are generated with `row_number()`. What happens to `user_key` if `dim_user` is rebuilt after a new user is added? What does it imply for `fact_orders`?**
The keys are numbered in the order of `user_id`: a new user shifts the key of every user sorted after it. A `fact_orders` built before points to the wrong users. The fact table must be rebuilt with every dimension, or the keys must be stable: kept with a `MERGE` that only numbers new users, or derived from the natural key.

**If a user moves to another state, which state do the past orders show? What would a dimension that keeps the history of the changes need?**
The new state: `dim_user` is overwritten (slowly changing dimension of type 1), so all past orders follow the user. Keeping the history needs a type 2 dimension: one row per version of the user with its own surrogate key, `valid_from`, `valid_to` and `is_current` columns, and a fact table that stores the key of the version valid when the order was placed.

**Compare `orders_by_state_month` and the star query which rebuilds it.**
`orders_by_state_month` answers one question, per state and month, from 51 precomputed rows: fast and simple, but any other question needs a new table. The star answers any combination of its dimensions (year, quarter, weekend, product, city, …) without a new table, at the cost of joins and of a scan of the fact table at query time. Both return the same numbers here.

## Exercises

### 1. Partitioning

`gold.orders_by_state_month_part` is created with `partitioning = ARRAY['month(month)']`: 4 files under `data/month_month=2020-01/` to `2020-04/` (17, 10, 13 and 11 rows), against a single 51-row file for the unpartitioned table. The `$partitions` table gives the partition values 600 to 603, months since 1970. Filtering on February 2020:

| Table | Input rows | Filtered | Physical input |
| --- | --- | --- | --- |
| `orders_by_state_month` | 51 | 80.39 % | 1.35 kB |
| `orders_by_state_month_part` | 10 | 0 % | 963 B |

On the partitioned table Iceberg prunes the files of the other months from the manifests before reading anything.

### 2. Cohorts

| Birth decade | Users | Orders | Average units per order |
| --- | --- | --- | --- |
| 1910 | 1 | 47 | 3.19 |
| 1930 | 5 | 202 | 3.11 |
| 1940 | 4 | 274 | 3.07 |
| 1950 | 6 | 340 | 3.01 |
| 1960 | 8 | 433 | 3.02 |
| 1970 | 5 | 369 | 2.99 |
| 1980 | 2 | 81 | 2.79 |
| 1990 | 5 | 402 | 3.07 |
| 2000 | 5 | 171 | 2.98 |
| 2010 | 4 | 227 | 3.09 |
| 2020 | 5 | 288 | 3.12 |

The decades 2010 and 2020 (9 users, children placing hundreds of orders) expose again the random birthdates of the generator.

### 3. Append, `MERGE` and time travel

One order (`lab10-new-order-1`, user of `VT`, 2 croissants, 2020-04-28) is inserted into `silver.orders` through Trino: snapshot `7559734716757919187`, written through Polaris, so the Hadoop catalog `lab` no longer sees the current version of the table. A `MERGE` refreshes `gold.orders_by_state_month`: only the row `VT`, April 2020 changes, from 83 orders and 253 units to 84 and 255, in a snapshot `overwrite`. `FOR VERSION AS OF 7709230552639853414` still returns 83 and 253. Units: 8630 in silver, 8628 in gold before the `MERGE`, 8630 after.

### 4. `optimize`

On `top_products`, which had a single file after the refresh, `optimize` changed nothing and committed no snapshot. On `orders_by_state_month`, the `MERGE` had left 3 files: the original 51-row data file, a position delete file (`content = 1`) marking the old row of `VT`, and a 1-row data file with its new version. `optimize` rewrote them into a single 51-row file, in a snapshot `replace`: same rows, fewer files, no delete file to apply when reading.

### 5. The same aggregation in Spark

In the Spark session of the Polaris lab, as `dev`, the aggregation of `orders_by_state_month` on `polaris.silver.orders` and `polaris.silver.users` returns 51 rows, 2835 orders and 8630 units, like the gold table written by Trino, read by Spark through Polaris. `exceptAll` in both directions returns 0 rows: the two engines produce the same result from the same Iceberg tables.