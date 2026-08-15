# Databricks notebook source
from pyspark.sql.functions import sum, countDistinct, collect_set, collect_list, avg, max, min, count   

ventas = [
    ("Laptop", 10000),
    ("Laptop", 12000),
    ("Mouse", 500),
    ("Mouse", 700),
    ("Teclado", 800)
]

ventas_df = spark.createDataFrame(
    ventas,
    ["producto", "precio"]
)

ventas_df.groupBy("producto") \
         .agg(sum("precio")) \
         .show()

ventas_df2 = spark.createDataFrame(
    ventas,
    ["producto", "precio"]
)
ventas_df2.groupBy("producto").sum("precio").show()

ventas_df3 = spark.createDataFrame(
    ventas,
    ["producto", "precio"]
)
ventas_df3.groupBy("producto").count().show()

ventas_df3.groupBy("producto").agg(
    sum("precio").alias("total")
).show()


print("avg")
ventas_df3.groupBy("producto").agg(
    avg("precio").alias("promedio")
).show()


ventas_df3.groupBy("producto").agg(
    max("precio").alias("maximo")
).show()

ventas_df3.groupBy("producto").agg(
    min("precio").alias("minimo")
).show()

ventas_df3.groupBy("producto").agg(
    count("*").alias("cantidad"),
    sum("precio").alias("total"),
    avg("precio").alias("promedio"),
    max("precio").alias("maximo")
).show()


print("collect_list")
ventas_df3.groupBy("producto").agg(
    collect_list("precio").alias("precios")
).show(truncate=False)


print("collect_set")
ventas_df3.groupBy("producto").agg(
    collect_set("precio").alias("precios_unicos")
).show(truncate=False)


print("countDistinct")
ventas_df3.groupBy("producto").agg(
    countDistinct("precio").alias("precios_distintos")
).show()
