# Databricks notebook source
datos = [
    (1, "Juan", 25),
    (2, "Ana", 30)
]

df = spark.createDataFrame(
    datos,
    ["id","nombre","edad"]
)

df.printSchema()

df.dtypes

#Por defecto Spark infiere el esquema y normalmente marca las columnas como nullable = true.