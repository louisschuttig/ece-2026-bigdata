# Lab 8: Explore Iceberg tables with Spark

Report for the lab [`08.iceberg-introduction/lab.md`](https://github.com/adaltas/ece-bigdata-2026-fall/blob/main/08.iceberg-introduction/lab.md), completed on Onyxia (`vscode-pyspark` service, 4 CPU and 4000 Mi) with Spark 4.1.1, Iceberg 1.11.0 and the Hadoop catalog `lab` (`s3a://user-l-schuttig-ece/warehouse`).

The code is in [`lab8/`](./lab8), executed in the PySpark shell with `exec(open(...).read())`:

| File | Content |
| --- | --- |
| [`session.py`](./lab8/session.py) | SparkSession, catalog, query, snapshots and files of `lab.silver.orders` |
| [`changes.py`](./lab8/changes.py) | Second snapshot, time travel, schema evolution on `lab.silver.orders` |
| [`exercises.py`](./lab8/exercises.py) | Exercises 1 to 4 on `lab.silver.users` |

The metadata of both tables is committed in [`warehouse/`](./warehouse); the data files are excluded by `.gitignore`.

## Results

### `lab.silver.orders`

| Step | Snapshot | Operation | Rows |
| --- | --- | --- | --- |
| Lab 7, `createOrReplace` | 6272162234183241684 | overwrite | 2829 |
| Lab 7, exercise 4 | 51339553746962311 | append | 2830 |
| Append of 3 orders | 8468921244469004716 | append | 2833 |
| `ALTER TABLE … ADD COLUMN source` | no new snapshot (`v4.metadata.json`) | | 2833 |
| `INSERT … 'spark'` | 5343930186918005652 | append | 2834 |

Before the changes: 2830 orders, total quantity 8617, 2 data files (2829 rows / 84,055 bytes and 1 row / 1,790 bytes). After them: 4 snapshots, 4 data files (2829, 1, 3 and 1 rows), 5 metadata versions (`v1` to `v5.metadata.json`), `source` = `spark` for 1 row and `NULL` for 2833.

Time travel: `VERSION AS OF` returns 2829, 2830 and 2833 rows for the first three snapshots.

### `lab.silver.users`

| Step | Snapshot | Operation | Rows |
| --- | --- | --- | --- |
| Lab 7, `createOrReplace` | 2479060873203483430 | overwrite | 50 |
| Exercise 2, batch of 3 users | 1087547039301485016 | append | 53 |
| Exercise 4, `ADD COLUMN country` | no new snapshot (`v3.metadata.json`) | | 53 |
| Exercise 4, user with `country = 'US'` | 3314613910367014025 | append | 54 |

## Deviations from the instructions

- **Existing tables.** `lab.silver.orders` and `lab.silver.users` were written by the Spark DataFrame lab, so the section "Create an Iceberg table" was skipped to keep their history: the figures start at 2829 orders and 2 snapshots instead of 5000 orders and 1 snapshot.
- **No cleanup.** The cleanup section deletes `warehouse/`, but the Polaris lab registers `warehouse/silver/orders` and the Trino labs read it: the warehouse is kept.
- **Duplicate order.** The `INSERT … SELECT … FROM lab.silver.orders LIMIT 1` of the schema evolution section copies an existing row with its `order_id`: the table now holds two rows with the same `order_id`, which a `unique` test would reject. The exercises use `uuid()` for new identifiers instead.
- **Credentials.** `eval "$(aws configure export-credentials --profile default --format env)"` exports the credentials renewed in `~/.aws` before PySpark starts, so the session code of the lab reads valid `AWS_*` variables.
- **Shell.** The lab runs in the PySpark shell, not in a notebook with the `Python (lab-pyspark uv)` kernel. PySpark is started with `--driver-memory 1g --conf spark.local.dir=/home/onyxia/work/.spark-tmp --conf spark.jars.ivy=/home/onyxia/work/.ivy2`, and the code is written in files executed with `exec()`.
- **Metadata sync.** `aws s3 sync --exact-timestamps` is used, so that `version-hint.text` (1 byte, same size in every version) is downloaded.

## Answers to the questions

### Inspect the files

**Which files contain the actual rows?**
The Parquet files under `warehouse/silver/orders/data/`, one per write: 84,055, 1,790, 2,014 and 2,201 bytes.

**Which files describe the table?**
The files under `metadata/`: `vN.metadata.json` (schemas, partition spec, properties, list of snapshots, current snapshot), one `snap-<id>-….avro` manifest list per snapshot, one `…-m0.avro` manifest per write listing the data files with their column statistics, and `version-hint.text`, the number of the current version for the Hadoop catalog.

**Why does an Iceberg table need more than the Parquet files?**
A directory of Parquet files does not say which files form the table, which schema the table has, or how it looked before the last write. The metadata lists exactly the files of each snapshot: a reader never lists the directory and never sees a half-written file, a writer commits by publishing new metadata, and the statistics of the manifests let a query skip files. History, time travel and schema evolution are built on it.

### Explore Iceberg metadata

**How many snapshots does the table currently have?**
Two at the start of the lab: the `overwrite` of the Spark DataFrame lab and the `append` of its exercise 4.

**How many data files belong to the current table?**
Two: 2829 rows (84,055 bytes) and 1 row (1,790 bytes).

**What information does the `snapshots` table contain that the Parquet files do not?**
The history of the table: snapshot id, parent id, commit time, operation, location of the manifest list, and a summary with the added and total records, files and bytes, and the engine and Iceberg versions that wrote it. A Parquet file only knows its own schema and rows.

### Create a second snapshot

**How many snapshots are there now?**
Three: the append of 3 orders added snapshot `8468921244469004716`, child of `51339553746962311`.

**Did the original Parquet files disappear?**
No. The `files` table still lists the 84,055-byte and 1,790-byte files; the append only added a 2,014-byte file with the 3 new rows (`manifests-kept` in the summary).

**Why does Iceberg create a new snapshot instead of simply replacing the table?**
Data files are immutable: a write adds files and publishes a new snapshot that references the old files and the new ones, then moves the pointer to it in one atomic commit. Readers keep the snapshot they started with, nothing is rewritten for an append, and the previous versions stay readable for time travel and rollback.

### Time travel

**What does time travel read?**
The metadata of the requested snapshot: its manifest list, then its manifests, then only the data files that belonged to the table at that commit. `VERSION AS OF 6272162234183241684` reads the 2829-row file only.

**Why can Iceberg access an older version without making a complete copy of the table?**
A snapshot is a list of references to immutable files, not a copy of the data. Consecutive snapshots share most of their files: the third snapshot references the two files of the second one plus the new file.

**What would happen if the old snapshot's metadata and data files were physically deleted?**
Time travel to that snapshot would fail with missing files. Deleting a data file still referenced by the current snapshot would also break current reads. Old versions are removed safely with the `expire_snapshots` procedure, which drops the snapshots from the metadata and deletes only the files no live snapshot references.

### Schema evolution

**Did the existing Parquet files have to be rewritten?**
No. The `ALTER TABLE` only wrote `v4.metadata.json`, whose current snapshot is still `8468921244469004716`: the 4 data files were not touched. The old files have no `source` column, and Iceberg returns `NULL` for it (2833 rows). Only the inserted row, written after the change, has a value.

**Where is the new schema recorded?**
In the table metadata: `v4.metadata.json` adds the new schema to its list of schemas and sets it as current. Iceberg identifies columns by field id, not by name or position.

**Why is schema evolution a table-level operation rather than simply changing a Parquet file?**
A table is many files written at different times. Changing the files one by one would mean rewriting all of them, and readers of a plain directory would meet files with different schemas in between. Iceberg changes the schema once, atomically, in the table metadata, and maps every file to it through the field ids, so adding, renaming or dropping a column is safe for all readers.

### Compare Iceberg with Parquet

**Which one is simply a collection of Parquet files?**
`silver/spark/orders.parquet/`: two `part-00000-….snappy.parquet` files and a `_SUCCESS` marker.

**Which one has table metadata and snapshots?**
`warehouse/silver/orders/`: `data/` with 4 Parquet files, and `metadata/` with 4 manifests, 4 manifest lists, `v1` to `v5.metadata.json` and `version-hint.text`.

**How would you safely replace a Parquet directory while another query is reading it?**
Never overwrite it in place: write the new version to a new directory, then switch the readers to it, for example by changing the path a view or a configuration points to, and delete the old directory once no query uses it. On S3 a rename is a copy followed by a delete, so it is neither instant nor atomic.

**How does Iceberg make the table state visible atomically?**
The writer first writes new data files, manifests, a manifest list and a new metadata file, which no reader references yet. The commit is a single swap of the pointer to the current metadata file: `version-hint.text` with the Hadoop catalog, a compare-and-swap with a catalog service. A reader sees either the old state or the new one.

**Why is a catalog needed once several engines need to share the same Iceberg table?**
All engines must agree on which metadata file is current and must not overwrite each other's commits. The Hadoop catalog depends on file renames, which are not atomic on S3, and every engine must be configured with the same warehouse path. A catalog service gives table names, an atomic commit, discovery of the tables, and access control, which the Polaris lab sets up.

## Exercises

### 1. `lab.silver.users` as an Iceberg table

The table already exists, written by the Spark DataFrame lab with `writeTo("lab.silver.users").createOrReplace()`: 50 rows, 10 columns (`user_id`, `username`, `name`, `sex`, `email`, `birthdate`, `street`, `city`, `state`, `zip_code`), one snapshot `2479060873203483430` (`overwrite`).

### 2. Append a new batch of users

Three existing users are copied with a new `user_id` (`uuid()`), a `_batch2` suffix on the username and a `batch2.` prefix on the email, then appended with `writeTo(...).append()`. The table gets a second snapshot, `1087547039301485016` (`append`), and 53 rows.

### 3. Time travel before and after the append

`VERSION AS OF 2479060873203483430` returns 50 rows, `VERSION AS OF 1087547039301485016` returns 53.

### 4. Add a `country` column

`ALTER TABLE lab.silver.users ADD COLUMN country STRING` writes `v3.metadata.json` without a new snapshot. An `INSERT` with the evolved schema adds one user with `country = 'US'`: snapshot `3314613910367014025`, 53 users with `NULL` and 1 with `US`. Data files: 50 rows (7,436 bytes), 3 rows (3,388 bytes) and 1 row (3,587 bytes).

### 5. S3 layout of the users and orders tables

Both tables follow the same layout under `warehouse/silver/<table>/`:

| | orders | users |
| --- | --- | --- |
| Data files (`data/`) | 4 | 3 |
| Manifests (`…-m0.avro`) | 4 | 3 |
| Manifest lists (`snap-….avro`) | 4 | 3 |
| Metadata versions (`vN.metadata.json`) | 5 | 4 |

Each write adds one data file, one manifest and one manifest list; each commit, including a schema change, adds one metadata version. The small files written by the appends (1 to 3 rows) show why tables need periodic compaction with the `rewrite_data_files` procedure.

### 6. Parquet, Iceberg and an Iceberg catalog

- **Parquet** is a file format: one file stores rows in columns, with its own schema and statistics in its footer. It knows nothing about the other files.
- **Iceberg** is a table format: a metadata layer over a set of Parquet files that defines the table, its schema and partitioning, its snapshots and the exact list of files of each one, with atomic commits, time travel and schema evolution.
- **An Iceberg catalog** maps a table name to its current metadata file and performs the commits. Here the Hadoop catalog is a directory of the bucket with `version-hint.text`; a catalog service such as Polaris adds atomic commits for concurrent writers, shared names across engines, and access control.