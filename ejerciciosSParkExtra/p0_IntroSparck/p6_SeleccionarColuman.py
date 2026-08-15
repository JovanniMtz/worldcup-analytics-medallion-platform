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

df.select("nombre").show()

df.select("edad").show()

df.select("nombre", "edad").show()
print("###########")

##display(df.edad)

#df.edad.show()

df.select("id").show()


