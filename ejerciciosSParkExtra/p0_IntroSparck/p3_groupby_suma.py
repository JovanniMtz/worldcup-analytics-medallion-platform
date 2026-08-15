# Databricks notebook source

datos = [
    ("2026-06-01","img1.png",2),
    ("2026-06-01","img2.png",5),
    ("2026-06-02","img3.png",1)
]

df = spark.createDataFrame(
    datos,
    ["fecha","archivo","rostros"]
)
agrupacion=df.groupBy("fecha").sum("rostros")

display(datos)
display(agrupacion)

print("hola" agrupacion.show())