# Lab 3: SQL analytics with DuckDB

Report for the lab [`04.sql-analytics/lab-duckdb.md`](https://github.com/adaltas/ece-bigdata-2026-fall/blob/main/04.sql-analytics/lab-duckdb.md), completed on Onyxia (`vscode-pyspark` service, DuckDB v1.5.5) with the personal bucket `user-l-schuttig-ece`.

## Results by section

| Section | Result |
| --- | --- |
| Environment | The service credentials had expired (`InvalidAccessKeyId`). New credentials were written to `~/.aws` from the Onyxia storage settings, and the stale `AWS_*` environment variables were unset. |
| S3 configuration | The `s3_onyxia_connection` persistent secret was recreated with `PROVIDER credential_chain`. |
| Initialization file | `init.sql` defines the `bucket` variable (`s3://user-l-schuttig-ece`) and the `UTC` time zone. `*.duckdb`, `*.duckdb.wal` and `init.sql` are ignored by Git. |
| Bronze layer | `users.csv`: delimiter `,`, quote `"`, header. `orders.csv`: `uuid`, `user_uuid`, `date` (timestamp with time zone), `quantity`, `product`. |
| Tables | 50 users, 2829 orders, from 2020-01-01 to 2020-04-27. |
| Data quality | 0 orphan orders, 0 inactive users, no duplicated order identifier. |
| Aggregations | Croissant first (1523, 17.7 %), cookie last (1365, 15.8 %). 744 / 696 / 744 / 645 orders per month (one order per hour). |
| Joins | Top customer: `elizabeth57` (97 orders, quantity 317). |
| Window functions | Cumulative quantity and 7-day moving average match the instructions. Best seller per month: donut, croissant, bread, croissant. |
| Pivot | 4 months x 6 products. |
| Parquet export | 130 KB Parquet file vs 332 KB CSV. |
| Hive partitioning | 6 files `product=<value>/data_0.parquet`. Filtering on `cookie` scans 1/6 file (466 rows, 3 GET requests, 33.7 KiB). |
| CSV vs. Parquet | 252,416 orders: 29.6 MB CSV vs 11.5 MB Parquet. |
| Python API | `orders-report` command, product passed as a query parameter (`$product`). |

## Observation: non-deterministic ranking

In the top 5 customers, the 5th place is `wrightjames` instead of `stephen00` in the instructions: both have a quantity of 285. Without a tie-breaker, the order of equal rows is not guaranteed. A stable ranking needs a second key, for example `ORDER BY quantity DESC, username`.

## Exercises

**1. Average number of orders per user, and average quantity per order**

```sql
SELECT
  round(count(*) / count(DISTINCT user_uuid), 2) AS avg_orders_per_user,
  round(avg(quantity), 2) AS avg_quantity_per_order
FROM orders;
```

56.58 orders per user, 3.05 items per order.

**2. First and last order of each user, and number of days between them**

```sql
SELECT u.username, min(o.date) AS first_order, max(o.date) AS last_order,
  date_diff('day', min(o.date), max(o.date)) AS days_between
FROM orders o
JOIN users u ON o.user_uuid = u.uuid
GROUP BY u.username
ORDER BY days_between DESC;
```

The gaps range from 0 to 5 days only (`hoganashlee`: 5 days). The generator creates the orders user after user, one per hour: the orders of each customer are consecutive, which is not realistic. Note that `date_diff('day', ...)` counts the date boundaries crossed, not 24-hour periods; `max(date) - min(date)` returns the exact interval.

**3. Hour of the day with the highest quantity sold**

```sql
SELECT hour(date) AS hour, sum(quantity) AS quantity
FROM orders
GROUP BY hour
ORDER BY quantity DESC
LIMIT 5;
```

6 h UTC (385), just ahead of 15 h (382) and 0 h (379). The gap is negligible: the quantities are random and uniform, no hour really stands out.

**4. Month-over-month variation of the quantity sold per product**

```sql
WITH monthly AS (
  SELECT product, strftime(date, '%Y-%m') AS month, sum(quantity) AS quantity
  FROM orders
  GROUP BY ALL
)
SELECT product, month, quantity,
  lag(quantity) OVER (PARTITION BY product ORDER BY month) AS prev_quantity,
  round(100.0 * (quantity - prev_quantity) / prev_quantity, 1) AS mom_pct
FROM monthly
ORDER BY product, month;
```

| Product | Feb | Mar | Apr |
| --- | --- | --- | --- |
| bread | +14.9 % | +13.5 % | -19.2 % |
| brioche | -18.8 % | -4.9 % | -6.9 % |
| cookie | -1.3 % | -8.4 % | -18.2 % |
| croissant | +8.9 % | -1.8 % | +0.3 % |
| donut | -28.3 % | +2.2 % | -1.2 % |
| drink | -3.2 % | +10.1 % | -13.2 % |

January has no previous month (`NULL`). April stops on the 27th: part of its decrease comes from the incomplete month.

**5. Users who ordered every product at least once**

```sql
SELECT u.username, count(DISTINCT o.product) AS products
FROM orders o
JOIN users u ON o.user_uuid = u.uuid
GROUP BY u.username
HAVING count(DISTINCT o.product) = (SELECT count(DISTINCT product) FROM orders)
ORDER BY u.username;
```

45 users out of 50. The 5 others are the smallest customers, with 2 to 12 orders: `millertodd`, `kayla51`, `clarence34`, `heatherberger`, `perezrebecca`.

## Answers to the questions

**The persistent secret is stored in a file of your home directory. What are the risks, and why are they limited on Onyxia?**
The file is readable by any process running as the user, and can leak through a backup or an accidental commit. On Onyxia the credentials are temporary STS tokens (12 hours at most), each service runs in an isolated container of the user namespace, and with `credential_chain` the file stores no key at all.

**How would you restrict the access to S3 for a Kubernetes Job?**
A dedicated ServiceAccount federated with the identity provider (IRSA or OIDC to STS) to get short-lived credentials, a Secret mounted only in the Job pod, and a policy limited to the required prefix and actions, for example read-only on `bronze/`.

**Why does `approx_unique` return 40 and 3167?**
`SUMMARIZE` estimates the number of distinct values with HyperLogLog, a probabilistic algorithm: fast and memory efficient, but approximate.

**Which issue may occur with a large file whose first rows are not representative?**
The types are inferred from a sample. A later value which does not match the inferred type makes the read fail or is misinterpreted. Use `sample_size = -1` or declare the types explicitly with `columns`.

**Why is the `uuid` column barely compressed?**
Random and unique strings: dictionary encoding is useless and Snappy finds no repetition (102,191 vs 113,189 bytes).

**Why is the compressed size of some columns larger than their uncompressed size?**
The `date` and `product` columns are already compact (binary values, dictionary encoding). Snappy gains nothing but adds its own framing overhead (22,667 vs 22,661 bytes for `date`).

**Why is partitioning by `uuid` a bad idea?**
One directory and one tiny file per order. On object storage every request has a fixed latency, listing millions of objects is slow and costly, and small files cancel the benefits of Parquet. Queries rarely filter on a unique identifier.

**Which partition column would you choose for a dataset of orders growing every day?**
The date, per day or per month depending on the volume: each day adds a new partition without rewriting the others, and most queries filter on a time range.

**How many row groups does the file contain, and how many are skipped by the filter `date >= '2100-01-01'`?**
3 row groups (123,573, 124,276 and 4,567 rows). With 252,416 orders, one per hour, the dates end on 2048-10-17: the 3 row groups are skipped. Only the footer is read (1 GET request, 16 KiB).

**The orders are sorted by date. What would happen to the efficiency of the filter if they were shuffled?**
Every row group would cover the whole period, and the min/max statistics could no longer exclude any of them: a filter such as `date >= '2040-01-01'` would read all row groups instead of the last ones.

**Compare the execution time of the CSV and Parquet queries. Which part is due to the network, which part to the parsing?**

| Query | Data received | GET | Time |
| --- | --- | --- | --- |
| CSV, sum per product | 28.2 MiB | 1 | 1.20 s |
| Parquet, sum per product | 203.5 KiB | 4 | 0.311 s |
| Parquet, `date >= '2100-01-01'` | 16.0 KiB | 1 | 0.207 s |

The CSV query transfers about 140 times more data but is only about 4 times slower: the network inside the platform is fast, and a large part of the time is spent parsing text. Parquet reads only the footer and the column chunks of `product` and `quantity`, already typed and compressed.
