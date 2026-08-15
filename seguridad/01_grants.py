# Databricks notebook source
# Databricks notebook source
# ============================================================
# FIFA World Cup 2026 Analytics Platform
# Security Grants
# ============================================================

# COMMAND ----------
# Widgets

dbutils.widgets.text(
    "environment",
    "dev"
)

dbutils.widgets.text(
    "principal",
    "jlozanom2001@alumno.ipn.mx"
)

environment = dbutils.widgets.get("environment")
principal = dbutils.widgets.get("principal")

catalog_name = f"worldcup_{environment}"

print(f"Environment : {environment}")
print(f"Catalog     : {catalog_name}")
print(f"Principal   : {principal}")

# COMMAND ----------
# Catalog permission

spark.sql(
    f"""
    GRANT USE CATALOG
    ON CATALOG {catalog_name}
    TO `{principal}`
    """
)

print(
    f"USE CATALOG granted on {catalog_name}"
)

# COMMAND ----------
# Schema permissions

for schema_name in [
    "bronze",
    "silver",
    "gold"
]:

    spark.sql(
        f"""
        GRANT USE SCHEMA
        ON SCHEMA {catalog_name}.{schema_name}
        TO `{principal}`
        """
    )

    print(
        f"USE SCHEMA granted on {catalog_name}.{schema_name}"
    )

# COMMAND ----------
# Validation - Catalog Grants

print("Catalog Grants")

display(
    spark.sql(
        f"""
        SHOW GRANTS
        ON CATALOG {catalog_name}
        """
    )
)

# COMMAND ----------
# Validation - Bronze Grants

print("Bronze Grants")

display(
    spark.sql(
        f"""
        SHOW GRANTS
        ON SCHEMA {catalog_name}.bronze
        """
    )
)

# COMMAND ----------
# Validation - Silver Grants

print("Silver Grants")

display(
    spark.sql(
        f"""
        SHOW GRANTS
        ON SCHEMA {catalog_name}.silver
        """
    )
)

# COMMAND ----------
# Validation - Gold Grants

print("Gold Grants")

display(
    spark.sql(
        f"""
        SHOW GRANTS
        ON SCHEMA {catalog_name}.gold
        """
    )
)

# COMMAND ----------

print("Security grants completed successfully.")