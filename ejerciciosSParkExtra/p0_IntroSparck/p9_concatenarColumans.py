# Databricks notebook source
from pyspark.sql.functions import col , lit 

datos = [
    (1, "Juan", 25),
    (2, "Ana", 30)
]

df = spark.createDataFrame(
    datos,
    ["id","nombre","edad"]
)
display(df)


df2 = df.withColumn(
    "edad_5_anios",
    col("edad") + 5 #aqui aplica una suma a la columna edad
)

df=df.withColumn(
    "anio",
    lit(2026)
)

display(df2)

display(df)

