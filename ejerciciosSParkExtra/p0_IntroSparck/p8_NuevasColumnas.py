# Databricks notebook source
from pyspark.sql.functions import col , lit , concat

datos = [
    (1, "Juan", 25),
    (2, "Ana", 30)
]

df = spark.createDataFrame(
    datos,
    ["id","nombre","edad"]
)
display(df)


##lit() significa literal.

#Se usa cuando quieres poner un valor fijo en una columna de un DataFrame.

df=df.withColumn(
    "anio",
    lit(2026)
)

df2 = df.withColumn(
    "texto",
    concat(
        col("nombre"),
        lit(": "),
        col("edad").cast("string")
    )
)



display(df2)

display(df)

df.select(
    "nombre",
    "edad",
    concat(
        col("nombre"),
        lit(" tiene "),
        col("edad"),
        lit(" años")
    ).alias("descripcion")
).show()