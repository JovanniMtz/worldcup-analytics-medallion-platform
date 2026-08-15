# Databricks notebook source
from pyspark.sql.functions import col , lit 

datos = [
    (1, "Juan", 25),
    (2, "Ana", 30),
    (3,"Paco",7)
]

df = spark.createDataFrame(
    datos,
    ["id","nombre","edad"]
)
display(df)

df.orderBy("edad").show()

df.orderBy(df.edad.desc()).show()