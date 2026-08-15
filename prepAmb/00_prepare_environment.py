# Databricks notebook source
# Databricks notebook source
# ============================================================
# FIFA World Cup 2026 Analytics Platform
# Environment Preparation
# ============================================================

dbutils.widgets.text(
    "storage_account",
    "stworldcupanalytics"
)

dbutils.widgets.text(
    "credential_name",
    "sc_worldcup_devprod"
)

storage_account = dbutils.widgets.get("storage_account")
credential_name = dbutils.widgets.get("credential_name")

print(f"Storage Account: {storage_account}")
print(f"Credential Name: {credential_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC Creación de external locations

# COMMAND ----------

# COMMAND ----------
# External Locations DEV

spark.sql(f"""
CREATE EXTERNAL LOCATION IF NOT EXISTS el_raw_dev
URL 'abfss://raw-dev@{storage_account}.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL sc_worldcup_devprod)
""")

spark.sql(f"""
CREATE EXTERNAL LOCATION IF NOT EXISTS el_bronze_dev
URL 'abfss://bronze-dev@{storage_account}.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL sc_worldcup_devprod)
""")

spark.sql(f"""
CREATE EXTERNAL LOCATION IF NOT EXISTS el_silver_dev
URL 'abfss://silver-dev@{storage_account}.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL sc_worldcup_devprod)
""")

spark.sql(f"""
CREATE EXTERNAL LOCATION IF NOT EXISTS el_gold_dev
URL 'abfss://gold-dev@{storage_account}.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL sc_worldcup_devprod)
""")

print("DEV External Locations created")


# COMMAND ----------

# COMMAND ----------
# External Locations PROD

spark.sql(f"""
CREATE EXTERNAL LOCATION IF NOT EXISTS el_raw_prod
URL 'abfss://raw-prod@{storage_account}.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL sc_worldcup_devprod)
""")

spark.sql(f"""
CREATE EXTERNAL LOCATION IF NOT EXISTS el_bronze_prod
URL 'abfss://bronze-prod@{storage_account}.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL sc_worldcup_devprod)
""")

spark.sql(f"""
CREATE EXTERNAL LOCATION IF NOT EXISTS el_silver_prod
URL 'abfss://silver-prod@{storage_account}.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL sc_worldcup_devprod)
""")

spark.sql(f"""
CREATE EXTERNAL LOCATION IF NOT EXISTS el_gold_prod
URL 'abfss://gold-prod@{storage_account}.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL sc_worldcup_devprod)
""")

print("PROD External Locations created")

# COMMAND ----------

# MAGIC %md
# MAGIC validacion

# COMMAND ----------

display(
    spark.sql(
        "SHOW EXTERNAL LOCATIONS"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC crear los catálogos DEV y PROD.

# COMMAND ----------


spark.sql("""
CREATE CATALOG IF NOT EXISTS worldcup_dev
MANAGED LOCATION 'abfss://bronze-dev@stworldcupanalytics.dfs.core.windows.net/'
""")



# COMMAND ----------

display(
    spark.sql(
        "SHOW CATALOGS"
    )
)

# COMMAND ----------

spark.sql("""
CREATE CATALOG IF NOT EXISTS worldcup_prod
MANAGED LOCATION 'abfss://bronze-prod@stworldcupanalytics.dfs.core.windows.net/'
""")

# COMMAND ----------

display(
    spark.sql(
        "SHOW CATALOGS"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC schemas
# MAGIC

# COMMAND ----------

# COMMAND ----------
# DEV Schemas

spark.sql("""
CREATE SCHEMA IF NOT EXISTS worldcup_dev.bronze
MANAGED LOCATION 'abfss://bronze-dev@stworldcupanalytics.dfs.core.windows.net/'
""")

spark.sql("""
CREATE SCHEMA IF NOT EXISTS worldcup_dev.silver
MANAGED LOCATION 'abfss://silver-dev@stworldcupanalytics.dfs.core.windows.net/'
""")

spark.sql("""
CREATE SCHEMA IF NOT EXISTS worldcup_dev.gold
MANAGED LOCATION 'abfss://gold-dev@stworldcupanalytics.dfs.core.windows.net/'
""")

print("DEV schemas created")

# COMMAND ----------

display(
    spark.sql(
        "SHOW SCHEMAS IN worldcup_dev"
    )
)

# COMMAND ----------

# COMMAND ----------
# PROD Schemas

spark.sql("""
CREATE SCHEMA IF NOT EXISTS worldcup_prod.bronze
MANAGED LOCATION 'abfss://bronze-prod@stworldcupanalytics.dfs.core.windows.net/'
""")

spark.sql("""
CREATE SCHEMA IF NOT EXISTS worldcup_prod.silver
MANAGED LOCATION 'abfss://silver-prod@stworldcupanalytics.dfs.core.windows.net/'
""")

spark.sql("""
CREATE SCHEMA IF NOT EXISTS worldcup_prod.gold
MANAGED LOCATION 'abfss://gold-prod@stworldcupanalytics.dfs.core.windows.net/'
""")

print("PROD schemas created")

# COMMAND ----------

display(
    spark.sql(
        "SHOW SCHEMAS IN worldcup_prod"
    )
)

# COMMAND ----------

display(
    spark.sql("""
    DESCRIBE SCHEMA EXTENDED worldcup_dev.bronze
    """)
)

# COMMAND ----------

display(
    spark.sql("""
    DESCRIBE SCHEMA EXTENDED worldcup_dev.silver
    """)
)

# COMMAND ----------

display(
    spark.sql("""
    DESCRIBE SCHEMA EXTENDED worldcup_dev.gold
    """)
)