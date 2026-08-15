# Databricks notebook source

from pyspark.sql.functions import *

# COMMAND ----------




# COMMAND ----------
# Widgets

dbutils.widgets.text(
    "environment",
    "dev"
)

dbutils.widgets.text(
    "storage_account",
    "stworldcupanalytics"
)

environment = dbutils.widgets.get("environment")
storage_account = dbutils.widgets.get("storage_account")

catalog_name = f"worldcup_{environment}"

raw_path = (
    f"abfss://raw-{environment}@"
    f"{storage_account}.dfs.core.windows.net/worldcup2026/"
)

bronze_path = (
    f"abfss://bronze-{environment}@"
    f"{storage_account}.dfs.core.windows.net/"
)

print(f"Environment : {environment}")
print(f"Catalog     : {catalog_name}")
print(f"RAW Path    : {raw_path}")
print(f"BRONZE Path : {bronze_path}")

# COMMAND ----------
# Validate RAW access

display(
    dbutils.fs.ls(raw_path)
)

# COMMAND ----------
# Source files

files = {
    "player_standard_stats2026_worldcup":
        "Player_Standard_Stats2026_WorldCup.csv",

    "player_shooting_stats2026_worldcup":
        "Player_Shooting_2026_WorldCup.csv",

    "player_playingtime_stats2026_worldcup":
        "Player_PlayingTime_2026_WorldCup.csv",

    "player_miscellaneous_stats2026_worldcup":
        "Player_Miscellaneous_Stats2026_WorldCup.csv",

    "player_goalkeeping_2026_worldcup":
        "Player_Goalkeeping_2026_WorldCup.csv",

    "scores_fixtures_2026_worldcup":
        "Scores_&_Fixtures_2026_WorldCup.csv"
}

# COMMAND ----------
# Bronze Ingestion

for table_name, file_name in files.items():

    source_path = f"{raw_path}{file_name}"

    target_table = (
        f"{catalog_name}.bronze.{table_name}"
    )

    target_path = (
        f"{bronze_path}{table_name}"
    )

    print("=" * 80)
    print(f"Reading file: {file_name}")

    df = (
        spark.read
            .option("header", "true")
            .option("inferSchema", "false")
            .csv(source_path)
            .withColumn(
                "LoadDate",
                current_timestamp()
            )
            .withColumn(
                "SourceFile",
                lit(file_name)
            )
    )

    record_count = df.count()

    print(f"Records: {record_count}")

    (
        df.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .option(
                "path",
                target_path
            )
            .saveAsTable(
                target_table
            )
    )

    print(
        f"Created table: {target_table}"
    )

    print(
        f"Physical path: {target_path}"
    )

# COMMAND ----------
# Validation

display(
    spark.sql(
        f"""
        SHOW TABLES IN {catalog_name}.bronze
        """
    )
)

# COMMAND ----------
# Counts

for table_name in files.keys():

    count_value = (
        spark.table(
            f"{catalog_name}.bronze.{table_name}"
        )
        .count()
    )

    print(
        f"{table_name}: {count_value}"
    )

print("Bronze layer completed successfully.")

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN worldcup_dev.bronze