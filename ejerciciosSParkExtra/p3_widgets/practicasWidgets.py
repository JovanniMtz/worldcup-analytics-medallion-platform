# Databricks notebook source
from pyspark.sql.functions import col, count, sum

# COMMAND ----------

#df = spark.read.csv("file:/Workspace/Users/jovanni.mtz.93@outlook.com/misPracticasdeWidgets/ventas.csv", header=True)

df = spark.read.csv(
    "file:/Workspace/Users/jovanni.mtz.93@outlook.com/misPracticasdeWidgets/ventas.csv",
    header=True,
    inferSchema=True
)
df.display()
print("Mexico filtrado")
#Let'
df.filter(df["pais"] == "Mexico").display()

# COMMAND ----------

# MAGIC %md
# MAGIC Si quieres que el usuario pueda elegir el país, puedes crear un widget:

# COMMAND ----------

dbutils.widgets.dropdown(
    "paisDisponible", ##Nombre del Widget
    "Mexico", ##valorDefault
    ["Mexico", "USA", "Canada"] ##opciones 
)

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Y después obtener el valor seleccionado:

# COMMAND ----------

pais = dbutils.widgets.get("paisDisponible")

df.filter(df["pais"] == pais).display()

# COMMAND ----------

# MAGIC %md
# MAGIC Ejercicio, seleccionar la categoria y calcular ventas
# MAGIC

# COMMAND ----------

dbutils.widgets.dropdown(
    "categoria",
    "Audio",
    ["Audio", "Computo", "Accesorios"]
)

# COMMAND ----------

df.printSchema()

df = df.withColumn(
    "cantidad",
    col("cantidad").cast("double")
)

df = df.withColumn(
    "precio",
    col("precio").cast("double")
)

# COMMAND ----------



categoria = dbutils.widgets.get("categoria")

df_categoria = df.filter(df.categoria == categoria).display()

df_categoria = df_categoria.withColumn(
    "importe",
    col("cantidad") * col("precio")
)

df.printSchema()

df_categoria.display()

resumen = df_categoria.agg(
    count("*").alias("numero_ventas"),
    sum("cantidad").alias("productos_vendidos"),
    sum("importe").alias("importe_total")
)

display(resumen)

# COMMAND ----------

# MAGIC %md
# MAGIC Ejercico 4 , widget multislect
# MAGIC

# COMMAND ----------

dbutils.widgets.multiselect(
    "region",
    "Centro",
    ["Norte", "Centro", "Sur", "Occidente"]
)

# COMMAND ----------

#usnado widget multiselect

regiones = dbutils.widgets.get("region")
#se obtiene una cadena, por lo tanto se convierte a una lista (vector)
regiones = dbutils.widgets.get("region").split(",")

print(regiones)
 #Filtrar con isin()
#Aquí está la parte importante del ejercicio.
#"¿El valor está dentro de esta lista?"
df_filtrado = df.filter(
    col("region").isin(regiones)
)
df_filtrado.display()

df_categoria = df_filtrado.withColumn(
    "importe",
    col("cantidad") * col("precio")
)

df_categoria.display()

resumen = df_categoria.agg(
    count("*").alias("numero_ventas"),
    sum("cantidad").alias("productos_vendidos"),
    sum("importe").alias("importe_total")
)

display(resumen)


# COMMAND ----------

# MAGIC %md 
# MAGIC Ejercicio 4 extra, muestra datos por region

# COMMAND ----------

#usnado widget multiselect

regiones = dbutils.widgets.get("region")
#se obtiene una cadena, por lo tanto se convierte a una lista (vector)
regiones = dbutils.widgets.get("region").split(",")

print(regiones)
 #Filtrar con isin()
#Aquí está la parte importante del ejercicio.
#"¿El valor está dentro de esta lista?"
df_filtrado = df.filter(
    col("region").isin(regiones)
)
df_filtrado.display()

df_categoria = df_filtrado.withColumn(
    "importe",
    col("cantidad") * col("precio")
)
agrupacion=df_categoria.groupBy("region").sum("importe").display()
df_categoria.display()

resumen = df_categoria.groupBy("region").agg(
    count("*").alias("numero_ventas"),
    sum("cantidad").alias("productos_vendidos"),
    sum("importe").alias("importe_total")
)

display(resumen)

# COMMAND ----------

# MAGIC %md
# MAGIC Ejercicio 5 - widgets dinamicos
# MAGIC

# COMMAND ----------



# COMMAND ----------

#creación del widget dinamico
paises = [
    row["pais"]
    for row in df.select("pais").distinct().orderBy("pais").collect()
]

print(paises)

dbutils.widgets.dropdown(
    "pais",
    paises[0],
    paises
)

categorias = [
    row["categoria"]
    for row in df.select("categoria").distinct().orderBy("categoria").collect()
]

dbutils.widgets.dropdown(
    "categoria",
    categorias[0],
    categorias
)

# COMMAND ----------

from pyspark.sql.functions import year

df = df.withColumn(
    "anio",
    year("fecha")
)

df.display()

anio = [
    str(row["anio"]) ##los widgets trabajan con strings
    for row in df.select("anio").distinct().orderBy("anio").collect()
]

print(anio)

dbutils.widgets.dropdown(
    "anio",
    anio[0],
    anio
)

# COMMAND ----------


df_filtrado = df.filter(
    (col("anio") == int(dbutils.widgets.get("anio"))) &
    (col("pais") == dbutils.widgets.get("pais")) &
    (col("categoria") == dbutils.widgets.get("categoria"))
)
df_filtrado.display()

df_categoria = df_filtrado.withColumn(
    "importe",
    col("cantidad") * col("precio")
)

df_categoria.display()

resumen = df_categoria.groupBy("vendedor").agg(
    count("*").alias("numero_ventas"),
    sum("cantidad").alias("productos_vendidos"),
    sum("importe").alias("importe_total")
)

display(resumen)