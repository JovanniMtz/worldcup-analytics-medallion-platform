# Databricks notebook source
from pyspark.sql.functions import sum, countDistinct, collect_set, collect_list, avg, max, min, count, stddev, variance 

df = spark.read.csv(
    "/Workspace/Users/jovanni.mtz.93@outlook.com/Dia1/prueba.csv",
    header=True,
    inferSchema=True
)

df.show()
#display(df)

df.describe().show()

df.groupBy("edad").agg(
    avg("edad").alias("promedio")
).show()

df.select(avg("edad")).show()

df.agg(
    avg("edad").alias("promedio"),
    stddev("edad").alias("desv_std"),
    variance("edad").alias("varianza"),
    min("edad").alias("minimo"),
    max("edad").alias("maximo"),
    count("edad").alias("total")
).show()



df.summary().show()


# COMMAND ----------

# DBTITLE 1,Example: Reading CSV from DBFS
# Example: Reading CSV from DBFS
# You can use any DBFS path - /FileStore/tables/ is just a common convention

# Option 1: Using dbfs:/ protocol (recommended for Spark)
df_dbfs = spark.read.csv(
    "dbfs:/FileStore/my_csvs/prueba.csv",  # Can use any path you want
    header=True,
    inferSchema=True
)

# Option 2: Using /dbfs/ mount point
# df_dbfs = spark.read.csv(
#     "/dbfs/user/data/prueba.csv",  # Another example path
#     header=True,
#     inferSchema=True
# )

display(df_dbfs)

# COMMAND ----------

# DBTITLE 1,Upload file to DBFS using dbutils
# To copy a file from workspace to DBFS (use any path you want):
# dbutils.fs.cp(
#     "file:/Workspace/Users/jovanni.mtz.93@outlook.com/Dia1/prueba.csv",
#     "dbfs:/FileStore/my_csvs/prueba.csv"  # Your custom path
# )

# To list files in DBFS directory:
dbutils.fs.ls("dbfs:/FileStore/")

# COMMAND ----------

# DBTITLE 1,Opciones de rutas en DBFS (Español)
# OPCIONES DE RUTAS EN DBFS:

# 1. Crear tu propia carpeta en FileStore:
# dbutils.fs.mkdirs("dbfs:/FileStore/mis_datos/")
# dbutils.fs.cp(
#     "file:/Workspace/Users/jovanni.mtz.93@outlook.com/Dia1/prueba.csv",
#     "dbfs:/FileStore/mis_datos/prueba.csv"
# )

# 2. Usar la carpeta por defecto (tables):
# dbutils.fs.cp(
#     "file:/Workspace/Users/jovanni.mtz.93@outlook.com/Dia1/prueba.csv",
#     "dbfs:/FileStore/tables/prueba.csv"
# )

# 3. Crear estructura en /user/:
# dbutils.fs.mkdirs("dbfs:/user/jovanni/datos/")
# dbutils.fs.cp(
#     "file:/Workspace/Users/jovanni.mtz.93@outlook.com/Dia1/prueba.csv",
#     "dbfs:/user/jovanni/datos/prueba.csv"
# )

# Ver qué hay en FileStore:
dbutils.fs.ls("dbfs:/FileStore/")