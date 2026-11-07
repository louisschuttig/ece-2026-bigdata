# Lab 9: Govern Iceberg tables with Polaris

Report for the two labs of the module [`09.polaris`](https://github.com/adaltas/ece-bigdata-2026-fall/tree/main/09.polaris): [`lab-1-deployment.md`](https://github.com/adaltas/ece-bigdata-2026-fall/blob/main/09.polaris/lab-1-deployment.md) (deploy Polaris with Helm) and [`lab-2.md`](https://github.com/adaltas/ece-bigdata-2026-fall/blob/main/09.polaris/lab-2.md) (govern the Iceberg tables), completed on Onyxia in the namespace `user-l-schuttig-ece`, with Polaris 1.8.0, Spark 4.1.1 and Iceberg 1.11.0.

| Path | Content |
| --- | --- |
| [`polaris/deployment/`](./polaris/deployment) | Helm chart of the deployment lab: Polaris and PostgreSQL dependencies, values, bootstrap Job, database Secret |
| [`lab9/session.py`](./lab9/session.py) | SparkSession with the Hadoop catalog `lab` and the Polaris REST catalog `polaris`, principal chosen by `POLARIS_PRINCIPAL` |
| [`lab9/dev.py`](./lab9/dev.py) | As `dev`: register `silver.orders`, create `silver.users`, add a column |
| [`lab9/analyst.py`](./lab9/analyst.py) | As `analyst`: read, failed writes, bypass of Polaris through S3 |
| [`lab9/exercises_dev.py`](./lab9/exercises_dev.py) | Exercise 1, and the `gold` namespace of exercise 2 |
| [`lab9/silver_reader.sh`](./lab9/silver_reader.sh) | Exercise 2, as `root`: catalog role `silver_reader`, principal role `silver_analyst`, principal `reader` |
| [`lab9/reader.py`](./lab9/reader.py) | Exercise 2: test of `reader` |
| [`lab9/cleanup.py`](./lab9/cleanup.py) | Cleanup as `dev` |

The Iceberg metadata is committed in [`warehouse/`](./warehouse). The credentials of the principals are kept outside the repository.

## Deployment (lab 1)

The `vscode-pyspark` StatefulSet was patched to 2 CPU and 2500 Mi (`cpu.max` 200000 100000, `memory.max` 2621440000), leaving room in the namespace quota:

| Quota `onyxia-quota` | Used by the service | Quota | Polaris needs (limits) |
| --- | --- | --- | --- |
| `limits.memory` | 2500 Mi | 4 Gi | Polaris 512 Mi, PostgreSQL 256 Mi, bootstrap Job 56 Mi |
| `limits.cpu` | 2 | 4 | Polaris 500m, PostgreSQL 250m, bootstrap Job 200m |

`helm upgrade --install polaris polaris/deployment --wait` (Helm 3.21.4) installed the release `polaris`, revision 1, `STATUS: deployed`: pods `polaris` and `polaris-postgresql-0` Running, `polaris-bootstrap` Completed, `/q/health` returns `UP`. The bootstrap Job created:

| Entity | Created |
| --- | --- |
| Catalog | `user-l-schuttig-ece`, `INTERNAL`, default base location and allowed location `s3://user-l-schuttig-ece/warehouse/` |
| Principals | `root`, `dev`, `analyst` |
| Principal roles | `service_admin`, `data_engineer` (→ `dev`), `data_analyst` (→ `analyst`) |
| Catalog roles | `catalog_admin`, `catalog_contributor` (→ `data_engineer`): `CATALOG_MANAGE_ACCESS`, `CATALOG_MANAGE_METADATA`, `CATALOG_MANAGE_CONTENT`; `catalog_reader` (→ `data_analyst`): `TABLE_READ_DATA`, `TABLE_READ_PROPERTIES`, `CATALOG_READ_PROPERTIES` |

## Results (lab 2)

| Step | Principal | Result |
| --- | --- | --- |
| `SHOW NAMESPACES` | `dev` | `lab`: `silver`; `polaris`: empty |
| `register_table` of `silver.orders` | `dev` | Points to `v5.metadata.json`: snapshot `5343930186918005652`, 2834 rows, 4 data files. 2834 rows through `polaris` and `lab`, 4 snapshots. Objects under `warehouse/silver/orders/`: 18 before and after |
| `polaris.silver.users` | `dev` | 54 rows, one data file of 54 rows, metadata `00000-0a58c875-….metadata.json`, snapshot `7786591667279543306` |
| `ADD COLUMN country` | `dev` | `00001-34aa1c7b-….metadata.json`, same snapshot |
| Read `silver.orders` per product | `analyst` | 459 bread, 468 brioche, 470 cookie, 491 croissant, 462 donut, 484 drink |
| `SHOW NAMESPACES IN polaris` | `analyst` | `Forbidden … not authorized for op LIST_NAMESPACES` |
| `INSERT INTO polaris.silver.orders` | `analyst` | `Forbidden … op ADD_TABLE_SNAPSHOT`; Spark aborts the write |
| `ALTER TABLE polaris.silver.users ADD COLUMN phone` | `analyst` | `Forbidden … op ADD_TABLE_SCHEMA` |
| Read through `lab`, and the Parquet files directly | `analyst` | 2834 rows each: Polaris is bypassed |
| Snapshots of `silver.orders` after the failed `INSERT` | `dev` | Still 4, and still 4 data files in the bucket |

## Deviations from the instructions

- **Order of the labs.** The Iceberg lab was done first, with 4000 Mi: the deployment lab lowers the service to 2500 Mi and restarts it.
- **Registered version.** The instructions register `v1.metadata.json`. The table was at version 5 after the Iceberg lab: registering `v1` would have exposed 2829 orders in Polaris. `lab9/dev.py` reads `version-hint.text` and registers `v5.metadata.json`.
- **`country` column.** `lab.silver.users` already had the `country` column of the Iceberg lab: it is dropped when the table is copied to Polaris, so that the `ADD COLUMN country` of the instructions applies.
- **Credentials of the principals.** The instructions store them in `/home/onyxia/.polaris-credentials.env`, outside the persistent volume: a restart of the service deletes them, and they cannot be retrieved. They are stored in `/home/onyxia/work/.polaris-credentials.env`, mode 600, outside the Git repository, with a local copy.
- **AWS credentials.** `eval "$(aws configure export-credentials --profile default --format env)"` exports the renewed credentials before creating the `polaris-s3` Secret and starting PySpark.
- **Deployment files.** The 600 lines of the deployment files were extracted from the instructions by a script instead of being pasted in the terminal, which truncates long pastes. The Helm archives downloaded in `polaris/deployment/charts/` are not committed.
- **Shell.** PySpark shell instead of a notebook: the principal is chosen with the environment variable `POLARIS_PRINCIPAL` when PySpark starts, instead of editing the session cell and restarting a kernel.
- **`session.py` as `analyst`.** `catalog_reader` has no privilege to list namespaces: `SHOW NAMESPACES IN polaris` is caught and reported instead of stopping the file.
- **Root secret.** `lab9/silver_reader.sh` uses the demo root secret `s3cr3t` of the instructions; a real deployment would read it from a Secret.

## Answers to the questions

### SparkSession with the Polaris catalog

**Which component answered each of the two `SHOW NAMESPACES` commands? How can you tell?**
For `lab`, Spark itself: the Hadoop catalog lists the directories of `s3a://user-l-schuttig-ece/warehouse` with S3A, hence `silver`. For `polaris`, the Polaris server, through a REST call to `http://polaris:8181/api/catalog`: its answer was empty while the directory `warehouse/silver/` exists in the bucket. Polaris answers from its PostgreSQL database, not from the bucket. As `analyst`, the same command fails with a `ForbiddenException` raised by `RESTSessionCatalog`, an HTTP error returned by Polaris.

**Where is the credential used: in the Spark session, in the bucket, or in a request to Polaris?**
In requests to Polaris. The session exchanges `clientId:clientSecret` for an OAuth token at `/api/catalog/v1/oauth/tokens`, then sends the token in the `Authorization` header of every catalog call. The bucket never sees it: data and metadata files are read with the S3A keys of the session (`HadoopFileIO`), because this storage has no credential vending.

### Register the Silver table

**What did `register_table` write, in Polaris and in the bucket?**
In Polaris, one entity in its database: the table `silver.orders` and the location of its current metadata file, `v5.metadata.json`. In the bucket, nothing: 18 objects under `warehouse/silver/orders/` before and after. No data or metadata file is copied or rewritten.

**Why is it dangerous to keep writing to this table through the `lab` catalog once it is registered in `polaris`?**
Each catalog keeps its own pointer to the current metadata: `version-hint.text` for `lab`, the database for Polaris. A commit through `lab` would write `v6.metadata.json` and move `version-hint.text`, while Polaris would stay on `v5`: its readers would not see the change, and the next commit through Polaris would start from `v5` and fork the history. Maintenance run through one catalog, such as `expire_snapshots` or the removal of orphan files, could also delete files the other one still references.

### Create tables as `dev`

**Where did Polaris store the files for `silver.users`, and why?**
Under `s3://user-l-schuttig-ece/warehouse/silver/users/`: Polaris derives the default location of a table from the base location of the catalog, the namespace and the table name. It is the directory where the `lab` catalog wrote its own `users` table in the Spark and Iceberg labs. The two tables coexist without overwriting each other because their files have different names (`vN.metadata.json` and `0000N-<uuid>.metadata.json`), but nothing but their metadata tells which file belongs to which table. Creating `polaris.gold.orders_per_product` likewise created `warehouse/gold/`, which the Hadoop catalog then listed as a namespace: `SHOW NAMESPACES IN lab` returned `gold` and `silver`.

**What is the difference between "registered" and "created through Polaris" for the data files?**
A registered table keeps the files written by another catalog: `s3a://` paths, metadata named `vN.metadata.json`, a history written outside Polaris; Polaris only points to its current metadata file. A table created through Polaris has a location chosen by Polaris, `s3://` paths and metadata files named `0000N-<uuid>.metadata.json`, each written by a commit that Polaris validated.

### Schema change as `dev`

**Which metadata file records the change? Is it the same file that `register_table` pointed to?**
`00001-34aa1c7b-….metadata.json`, a new file; the table still points to snapshot `7786591667279543306`. It is not the file `register_table` pointed to, which was `v5.metadata.json` of `silver.orders`. Every commit writes a new metadata file, and Polaris moves its pointer to it.

**Which privilege does Polaris check for this statement?**
The statement is an update of the table metadata, operation `ADD_TABLE_SCHEMA`, which requires `TABLE_WRITE_PROPERTIES` on the table. `dev` has it through `catalog_contributor` and `CATALOG_MANAGE_CONTENT`; `analyst` does not, and the same statement failed with `not authorized for op ADD_TABLE_SCHEMA`.

### Read as the analyst

**Which privilege is missing for the `INSERT`? Which privileges does `catalog_reader` provide?**
`TABLE_WRITE_DATA`: the refused operation is `ADD_TABLE_SNAPSHOT`, the commit of a new snapshot. `catalog_reader` provides `TABLE_READ_DATA`, `TABLE_READ_PROPERTIES` and `CATALOG_READ_PROPERTIES` only; it cannot even list the namespaces (`LIST_NAMESPACES`).

**Did the failed `INSERT` create a snapshot?**
No: as `dev`, `silver.orders` still has its 4 snapshots, and the bucket still holds 4 data files. Spark wrote the data file with its own S3 keys before the commit, then aborted the write when Polaris refused the commit and deleted the file.

**What Polaris cannot enforce on this storage.**
Polaris controls its API, not the bucket. The session of `analyst` holds the S3 keys of the bucket owner: it read the 2834 orders through the Hadoop catalog and directly from the Parquet files, and could write or delete files the same way. Enforcing the privileges on the data needs credential vending: Polaris hands out short-lived credentials scoped to the location of each table, and clients get no keys of their own. That requires an STS service on the storage, which is not available here (`stsUnavailable: true`).

## Exercises

### 1. Remove the `country` column

`ALTER TABLE polaris.silver.users DROP COLUMN country` wrote `00002-8cf8fab3-….metadata.json`; the table still has one snapshot, `7786591667279543306`, and 54 rows. Dropping a column is a metadata operation: Iceberg identifies columns by field id, and the new schema simply no longer contains that id. The Parquet files are not rewritten; the column is ignored when they are read. No data changed, so no snapshot was created.

### 2. A catalog role limited to the `silver` namespace

As `dev`, the namespace `gold` and the table `gold.orders_per_product` were created (6 products, 2834 orders, total quantity 8628). As `root`, [`silver_reader.sh`](./lab9/silver_reader.sh) created:

| Entity | Grants |
| --- | --- |
| Catalog role `silver_reader` | `CATALOG_READ_PROPERTIES` on the catalog; `NAMESPACE_READ_PROPERTIES`, `TABLE_LIST`, `TABLE_READ_PROPERTIES`, `TABLE_READ_DATA` on the namespace `silver` |
| Principal role `silver_analyst` | `silver_reader` |
| Principal `reader` | `silver_analyst` |

As `reader`:

| Statement | Result |
| --- | --- |
| `SHOW TABLES IN polaris.silver` | OK: `users`, `orders` |
| `SELECT count(*) FROM polaris.silver.orders` / `users` | OK: 2834 / 54 |
| `SHOW TABLES IN polaris.gold` | `Forbidden … op LIST_TABLES` |
| `SELECT count(*) FROM polaris.gold.orders_per_product` | `Forbidden … op LOAD_TABLE` |
| `INSERT INTO polaris.silver.orders` | `Forbidden … op ADD_TABLE_SNAPSHOT` |
| `SHOW NAMESPACES IN polaris` | `Forbidden … op LIST_NAMESPACES` |

Grants on a namespace apply to that namespace and to the tables it contains, so `reader` can list and read `silver` but has no access to `gold`, nor to the list of namespaces, which is a catalog-level operation. A separate principal and principal role keep the test isolated from `analyst`.

## Cleanup

As `dev`, `polaris.gold.orders_per_product`, the namespace `polaris.gold` and `polaris.silver.users` were dropped; `polaris.silver.orders` is kept for the Trino lab, and Polaris keeps running. `DROP TABLE` without `PURGE` removes only the catalog entry: `lab.silver.users`, in the same directory, still returns 54 rows.