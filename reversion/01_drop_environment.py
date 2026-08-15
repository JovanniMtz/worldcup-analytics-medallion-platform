# Databricks notebook source
# Databricks notebook source
# ============================================================
# FIFA World Cup 2026 Analytics Platform
# Reversion Script
# ============================================================

# COMMAND ----------
# Widgets

dbutils.widgets.text(
    "environment",
    "dev"
)

environment = dbutils.widgets.get("environment")

catalog_name = f"worldcup_{environment}"

print(f"Environment : {environment}")
print(f"Catalog     : {catalog_name}")

# COMMAND ----------
# Gold Tables

gold_tables = [

    "fact_player_statistics",
    "top_scorers",
    "top_assists",
    "player_offensive_ranking",
    "player_defensive_discipline",
    "goalkeeper_ranking",
    "team_performance",
    "tournament_summary",
    "match_details"

]

# COMMAND ----------
# Silver Tables

silver_tables = [

    "dim_player",
    "player_standard",
    "player_shooting",
    "player_playing_time",
    "player_miscellaneous",
    "player_goalkeeping",
    "match_fixtures"

]

# COMMAND ----------
# Bronze Tables

bronze_tables = [

    "player_standard_stats2026_worldcup",
    "player_shooting_stats2026_worldcup",
    "player_playingtime_stats2026_worldcup",
    "player_miscellaneous_stats2026_worldcup",
    "player_goalkeeping_2026_worldcup",
    "scores_fixtures_2026_worldcup"

]

# COMMAND ----------
# Drop Gold

for table_name in gold_tables:

    spark.sql(
        f"""
        DROP TABLE IF EXISTS
        {catalog_name}.gold.{table_name}
        """
    )

    print(
        f"Dropped: {catalog_name}.gold.{table_name}"
    )

# COMMAND ----------
# Drop Silver

for table_name in silver_tables:

    spark.sql(
        f"""
        DROP TABLE IF EXISTS
        {catalog_name}.silver.{table_name}
        """
    )

    print(
        f"Dropped: {catalog_name}.silver.{table_name}"
    )

# COMMAND ----------
# Drop Bronze

for table_name in bronze_tables:

    spark.sql(
        f"""
        DROP TABLE IF EXISTS
        {catalog_name}.bronze.{table_name}
        """
    )

    print(
        f"Dropped: {catalog_name}.bronze.{table_name}"
    )

# COMMAND ----------
# Validation

print("Gold")

display(
    spark.sql(
        f"""
        SHOW TABLES IN
        {catalog_name}.gold
        """
    )
)

print("Silver")

display(
    spark.sql(
        f"""
        SHOW TABLES IN
        {catalog_name}.silver
        """
    )
)

print("Bronze")

display(
    spark.sql(
        f"""
        SHOW TABLES IN
        {catalog_name}.bronze
        """
    )
)

print("Environment cleanup completed successfully.")