# Welcome to my World-Wide-Importers-Spark project
The project uses WWI data using PySpark to ETL from the source system to Hive. Then, we will analyze the Superset

> This is project ETL data from csv files to hive. Then, this data will be analyzed with the superset

[![github release date](https://img.shields.io/github/release-date/loinguyen3108/Wide-World-Importers-Spark)](https://github.com/loinguyen3108/Wide-World-Importers-Spark/releases/tag/Latest) [![commit active](https://img.shields.io/github/commit-activity/w/loinguyen3108/Wide-World-Importers-Spark)](https://github.com/loinguyen3108/Wide-World-Importers-Spark/commit/main) [![license](https://img.shields.io/badge/license-Apache-blue)](https://github.com/nhn/tui.editor/blob/master/LICENSE) [![PRs welcome](https://img.shields.io/badge/PRs-welcome-ff69b4.svg)](https://github.com/loinguyen3108/Wide-World-Importers-Spark/issues) [![code with hearth by Loi Nguyen](https://img.shields.io/badge/DE-Loi%20Nguyen-orange)](https://github.com/loinguyen3108)

## 🚩 Table of Contents
- [🚩 Table of Contents](#-table-of-contents)
- [🎨 Stack](#-stack)
  - [⚙️ Setup](#️-setup)
- [WWI Star Schema](#wwi-star-schema)
- [✍️ Example](#️-example)
- [📜 License](#-license)

## 🎨 Stack

Project run in local based on `docker-compose.yml` in [bigdata-stack](https://github.com/loinguyen3108/bigdata-stack)

### ⚙️ Setup

**1. Run bigdata-stack**
```
git clone git@github.com:loinguyen3108/bigdata-stack.git

cd bigdata-stack

docker compose up -d

# setup superset
# 1. Setup your local admin account

docker exec -it superset superset fab create-admin --username admin --firstname Superset --lastname Admin --email admin@superset.com --password admin

2. Migrate local DB to latest

docker exec -it superset superset db upgrade

3. Load Examples

docker exec -it superset superset load_examples

4. Setup roles

docker exec -it superset superset init

Login and take a look -- navigate to http://localhost:8080/login/ -- u/p: [admin/admin]
```

**2. Spark Standalone**  
Setup at [spark document](https://spark.apache.org/docs/latest/spark-standalone.html)

**3. Dataset**  
Data setup at [WWI Dataset](https://learn.microsoft.com/en-us/sql/linux/tutorial-restore-backup-in-sql-server-container?view=sql-server-ver16)

**4. Environment**
```
export JDBC_URL=...
export JDBC_USER=...
export JDBC_PASSWORD=...
```

**5. Build dependencies**
```
./build_dependencies.sh
```

**6. Insert local packages**
```
./update_local_packages.sh
```

**7. Args help**
```
cd manager
python ingestion.py -h
python transform.py -h
cd ..
```

**8. Run**
```
# ingest data from postgres to datalake
spark-submit --py-files packages.zip manager/ingestion.py --table_name <table_name>

# transform data from datalake to hive
# Init dim_date
spark-submit --py-files packages.zip manager/transform .py--init --exec-date YYYY:MM:DD

#Transform
spark-submit --py-files packages.zip manager/transform.py
```

## WWI Star Schema
[WWI schema](https://drive.google.com/file/d/14JUyZdbpfdqgfxSIj0fBvxGwYPuAEpEc/view?usp=sharing)

## 📜 License

This software is licensed under the [Apache](https://github.com/loinguyen3108/dvdrental-etl/blob/master/LICENSE) © [Loi Nguyen](https://github.com/loinguyen3108).
