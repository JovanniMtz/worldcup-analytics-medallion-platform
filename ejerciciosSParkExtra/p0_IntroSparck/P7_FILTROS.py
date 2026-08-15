# Databricks notebook source
datos = [
    (1, "Juan", 25),
    (2, "Ana", 30)
]

from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    StringType
)

schema = StructType([
    StructField("id", LongType(), False),
    StructField("nombre", StringType(), False),
    StructField("edad", LongType(), False)
])

df = spark.createDataFrame(datos, schema)

df.filter(df.edad > 27).show()

df.where(df.edad > 27).show()

FILTRO=df.where(df.edad > 27)

filtro2=df.filter(df.edad > 27)

display(FILTRO)

display(filtro2)