# Lab 4: Bronze to Silver transformation with dbt and DuckDB

Report for the lab [`05.etl-elt-and-medallion-architecture/lab-dbt.md`](https://github.com/adaltas/ece-bigdata-2026-fall/blob/main/05.etl-elt-and-medallion-architecture/lab-dbt.md), completed on Onyxia (`vscode-pyspark` service) with dbt Core 1.12.6, the dbt-duckdb adapter 1.11.0 and the personal bucket `user-l-schuttig-ece`. The dbt project is in [`lab_medallion/`](./lab_medallion).

## Results by section

| Section | Result |
| --- | --- |
| Installation | `dbt-duckdb` added to the uv project. `dbt debug`: all checks passed. |
| Bronze layer | `bronze.users` and `bronze.orders` declared as sources, read on S3 with `read_csv` through `external_location`. The bucket name comes from `LAB_BUCKET_NAME`. |
| Silver layer | `stg_users` (typed, address split into street, city, state and zip code) and `stg_orders` (typed, deduplicated with `qualify row_number()`), materialized as views in `main_silver`. |
| Reference data | `states` seed: 50 states, DC, 5 territories, 3 freely associated states and 3 military regions (62 rows), in `main_reference`. |
| Generic tests | 13 tests: `unique`, `not_null`, `accepted_values`, `relationships`. |
| Failing test investigation | `relationships` on `state` failed with 6 rows: `AE` (3), `AP` (2), `AA` (1). These are military addresses (Armed Forces Europe, Pacific, Americas): the data was valid, the reference was incomplete. Fixed by adding the 3 codes to the seed. |
| Singular tests | `assert_orders_have_users` passes. `assert_orders_after_user_birth` fails with 288 orders, then set to `severity='warn'`. |
| Full pipeline | `dbt build` from an empty database: 1 seed, 2 models, 15 tests, `PASS=17 WARN=1 ERROR=0`. |
| Lineage | `dbt ls --select +stg_orders` returns `source:lab_medallion.bronze.orders` and `stg_orders`. |
| Documentation | `dbt docs generate --static` writes `target/static_index.html` (2.4 MB). |
| Parquet export | `stg_users` and `stg_orders` exported to `s3://user-l-schuttig-ece/silver/dbt/`. `users.parquet`: 50 users (20 M, 30 F). `orders.parquet`: 2829 rows, identical to the view (`EXCEPT` returns 0 rows). |
| Exercises | `dbt build`: 1 seed, 2 models, 19 tests, `PASS=22 WARN=0 ERROR=0`. |

## Deviations from the instructions

- **Missing `states` seed.** `silver.yml` references `ref('states')`, but the section creating the seed was removed from the instructions. It was recovered from the history of the course repository (before commit `7684f4f`), with the `seeds: +schema: reference` configuration.
- **`dbt build` without `--select silver`.** After deleting the database, `--select silver` does not load the seed, and the `relationships` test on `states` fails because the table is missing.
- **DuckDB CLI.** The export uses the `duckdb` command of the image: the `duckdb` Python package installed by uv provides no `uv run duckdb` command.
- **`seeds/states.csv` added with `git add -f`.** The `*.csv` rule of the root `.gitignore` excludes the generated datasets, but the seed is source code required by `dbt build`.

## Answers to the questions

**The bronze CSV files are left untouched. What is the benefit when a bug is found in a transformation, six months later?**
The model is fixed and the silver layer is rebuilt from the original raw data with `dbt build`. Nothing was overwritten: the bronze layer is the source of truth, an audit trail and a replay point.

**Which other format and layout would you choose if the orders were ingested every hour for years?**
Parquet partitioned by ingestion date, for example `bronze/orders/date=2026-10-17/hour=10/`, or better an open table format such as Iceberg (transactions, schema evolution). On the dbt side, an incremental model would read only the new partitions instead of the whole history.

**The silver models are views. What happens when a view is queried, and when is it preferable to a table?**
A view stores the query only: every query reads the CSV on S3 again and replays the transformation. It is always up to date and uses no storage, but costs at every read. It suits small or rarely queried data, or when freshness matters most. A table is preferable when the transformation is expensive and the data is read often.

**`UUID` values are stored in 16 bytes. How many bytes does the `VARCHAR` representation use?**
36 bytes: 32 hexadecimal characters and 4 hyphens, more than twice as much. DuckDB also stores a 16-byte string header, the value being longer than the 12 characters that can be inlined. `UUID` is more compact and faster to compare in joins.

**Which strategy would you choose for a dashboard displaying the age of the customers? For a dashboard displaying the sales per region?**
For the age, replace the invalid birthdates with `NULL`: no absurd or negative age, no customer lost, and the number of unknown values can be displayed. For the sales per region, keep all the orders: the birthdate is not used, and rejecting 288 orders would distort the revenue for an unrelated defect. The strategy depends on how the data is used.

**The addresses associate states with random zip codes, `KY 01352` for example. Which test would detect it, and which reference data would it require?**
A test checking that the zip code belongs to the range of its state: Kentucky zip codes start with 40 to 42, while 013 belongs to Massachusetts. It requires the official mapping between zip code prefixes and states (USPS, or the ZIP Code Tabulation Areas of the Census Bureau), loaded as a seed and joined in a singular test.

## Exercises

### 1. Add a cleaned column

`stg_users` exposes `username_normalized = lower(trim(username))`, documented in `silver.yml` and tested with `unique` and `not_null`: two usernames differing only by case or spaces would be detected as duplicates.

### 2. Improve order validation

- `product` belongs to the expected list: generic `accepted_values` test.
- `user_id` exists in `stg_users`: generic `relationships` test, and the singular test `assert_orders_have_users`.
- `quantity` is positive: no built-in generic test covers it, hence the singular test [`assert_orders_positive_quantity`](./lab_medallion/tests/assert_orders_positive_quantity.sql). The `accepted_values` test on `[1, 2, 3, 4, 5]` also bounds the quantity.

### 3. Clean invalid birthdates

`stg_users` joins the first order of each user from `stg_orders` and computes `birthdate_is_valid = birthdate <= first order date` (`true` for a user without order). An invalid birthdate is replaced with `NULL`. 5 users out of 50 have an invalid birthdate: they explain the 288 orders reported by `assert_orders_after_user_birth`. The test passes again, and its `severity='warn'` configuration was removed so that it blocks the pipeline again if new invalid data appears. The column is documented and tested with `not_null`.

### 4. Detect invalid ZIP codes

Not implemented: it requires reference data which is not available in the lab environment.

- **Source of the reference data:** an authoritative provider, USPS or the Census Bureau ZCTA files, with a documented version.
- **Seed or not:** a seed fits a small and stable table such as the 3-digit prefixes per state (about 1,000 rows). A full list of zip codes (about 42,000 rows, updated regularly) should be ingested as a source in the bronze layer instead.
- **Silver layer:** yes. The silver layer validates and standardizes the data: the check is a test on `stg_users`, or a `zip_code_is_valid` flag following the same pattern as `birthdate_is_valid`, so that the gold layer can filter on it.
- **When the reference data changes:** reload the seed or the source, rebuild with `dbt build`, and review the new failures. The reference is versioned in Git with the project, so every result can be traced to the version of the reference used.

### 5. Materialization experiment

With `+materialized: table`, `dbt build` runs "1 seed, 2 table models, 19 data tests" (`PASS=22`) and `information_schema.tables` lists `stg_orders` and `stg_users` as `BASE TABLE` instead of `VIEW`. The `lab_medallion.duckdb` file now stores the transformed data (1.3 MB after the build), and `SELECT count(*) FROM main_silver.stg_orders` returns 2829 in 1 ms, without reading S3. The configuration was then restored to `view` (`PASS=22`, "2 view models").

| | View | Table |
| --- | --- | --- |
| Where is the data stored? | Nowhere: only the query is stored, the data stays in the CSV on S3 | In the DuckDB database file |
| When is the transformation executed? | At every query | Once, during `dbt run` / `dbt build` |
| What happens when the CSV changes? | The next query sees the new data | The table is stale until the next build |
| Storage | None | A copy of the transformed data |
| Avoids re-reading the source files for every query | No | Yes |

A view suits development and small data. A table suits data queried often or an expensive transformation, at the cost of storage and of a build to refresh it. An incremental model combines both for large, growing data.
