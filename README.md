# Dataset generator

This project generates a random dataset consisting of users and orders. Scripts are written in Python and the project
uses [uv](https://docs.astral.sh/uv/).

## Usage

```bash
uv run dataset-users -h
#> usage: dataset-users [-h] [-c COUNT] [-o {csv,json,jsonline}]
uv run dataset-orders -h
#> usage: dataset-orders [-h] [-C COUNT_MIN] [-c COUNT_MAX] [-d DATE_FROM] [-o {csv,json,jsonline}] [-u COUNT_USERS]
```

## Labs of the Big Data Framework course

Reports and code of the labs of [adaltas/ece-bigdata-2026-fall](https://github.com/adaltas/ece-bigdata-2026-fall). The numbers of the reports do not follow the numbers of the modules: use this table.

| Module | Lab | Report | Code |
| --- | --- | --- | --- |
| 03 Object storage | [`lab-1-uv`](https://github.com/adaltas/ece-bigdata-2026-fall/blob/main/03.object-storage/lab-1-uv.md) | This README | [`src/`](./src), [`pyproject.toml`](./pyproject.toml) |
| 03 Object storage | [`lab-2-s3`](https://github.com/adaltas/ece-bigdata-2026-fall/blob/main/03.object-storage/lab-2-s3.md) | [`LAB2.md`](./LAB2.md) | [`job-upload-bronze.yaml`](./job-upload-bronze.yaml) |
| 04 SQL analytics | [`lab-duckdb`](https://github.com/adaltas/ece-bigdata-2026-fall/blob/main/04.sql-analytics/lab-duckdb.md) | [`LAB3.md`](./LAB3.md) | [`src/ece_2026_bigdata/orders_report.py`](./src/ece_2026_bigdata/orders_report.py) |
| 05 ETL, ELT and medallion | [`lab-dbt`](https://github.com/adaltas/ece-bigdata-2026-fall/blob/main/05.etl-elt-and-medallion-architecture/lab-dbt.md) | [`LAB4.md`](./LAB4.md) | [`lab_medallion/`](./lab_medallion) |
| 06 Spark introduction | [`lab-pyspark-rdd`](https://github.com/adaltas/ece-bigdata-2026-fall/blob/main/06.spark-introduction/lab-pyspark-rdd.md) | [`LAB6.md`](./LAB6.md) | [`lab6/`](./lab6) |
| 07 Spark SQL and DataFrames | [`lab-pyspark-silver`](https://github.com/adaltas/ece-bigdata-2026-fall/blob/main/07.sparksql-and-dataframes/lab-pyspark-silver.md) | [`LAB7.md`](./LAB7.md) | [`lab7/`](./lab7), [`warehouse/`](./warehouse) |
| 08 Iceberg introduction | [`lab`](https://github.com/adaltas/ece-bigdata-2026-fall/blob/main/08.iceberg-introduction/lab.md) | [`LAB8.md`](./LAB8.md) | [`lab8/`](./lab8), [`warehouse/`](./warehouse) |
| 09 Polaris | [`lab-1-deployment`](https://github.com/adaltas/ece-bigdata-2026-fall/blob/main/09.polaris/lab-1-deployment.md), [`lab-2`](https://github.com/adaltas/ece-bigdata-2026-fall/blob/main/09.polaris/lab-2.md) | [`LAB9.md`](./LAB9.md) | [`polaris/deployment/`](./polaris/deployment), [`lab9/`](./lab9) |
| 10 Trino | [`lab-1-deployment`](https://github.com/adaltas/ece-bigdata-2026-fall/blob/main/10.trino/lab-1-deployment.md), [`lab-2-trino-gold`](https://github.com/adaltas/ece-bigdata-2026-fall/blob/main/10.trino/lab-2-trino-gold.md), [`lab-3-trino-star-schema`](https://github.com/adaltas/ece-bigdata-2026-fall/blob/main/10.trino/lab-3-trino-star-schema.md) | [`LAB10.md`](./LAB10.md) | [`trino/deployment/`](./trino/deployment), [`lab10/`](./lab10), [`lab_trino/`](./lab_trino) |

The modules 01 (Linux commands) and 02 (containers and Kubernetes) have no deliverable in the repository. There is no `LAB5.md`: the reports were numbered in the order they were written, not by module.