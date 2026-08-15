# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import col, count, sum, try_to_date, coalesce, length, year


# COMMAND ----------

datos = [
    ("2026-08-01","MX","ventas_mx.csv",15000,"OK"),
    ("2026-08-01","US","sales_us.csv",22000,"OK"),
    ("2026-08-01","BR","vendas_br.csv",18000,"ERROR"),
    ("2026-08-02","MX","clientes_mx.csv",12000,"OK"),
    ("2026-08-02","US","customers_us.csv",25000,"OK"),
    ("2026-08-02","BR","clientes_br.csv",17000,"OK"),
    ("2026-08-03","MX","inventario_mx.csv",9000,"ERROR"),
    ("2026-08-03","US","inventory_us.csv",14000,"OK"),
    ("2026-08-03","BR","estoque_br.csv",11000,"OK")
]

schema = StructType([
    StructField("fecha", StringType(), True),
    StructField("pais", StringType(),   True),
    StructField("archivo", StringType(), True),
    StructField("cantidad", IntegerType(), True),
    StructField("precio", DoubleType(), True)
])


df = spark.createDataFrame(
    datos,
    ["fecha","pais","archivo","registros","estado"]
)

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC validador
# MAGIC

# COMMAND ----------

# MAGIC %md 
# MAGIC validador Columnas
# MAGIC

# COMMAND ----------

columnas_esperadas = [
    "fecha",
    "pais",
    "archivo",
    "registros",
    "estado"
]

columnas_recibidas = df.columns

faltantes = set(columnas_esperadas) - set(columnas_recibidas)

sobrantes = set(columnas_recibidas) - set(columnas_esperadas)


if len(faltantes) > 0 or len(sobrantes) > 0:

    if len(faltantes) > 0:
        print(f"ERROR: faltan columnas {faltantes}")

    if len(sobrantes) > 0:
        print(f"ERROR: sobran columnas {sobrantes}")

    raise Exception("El archivo no cumple con la estructura esperada")

else:
    print("OK")
  


# COMMAND ----------

# MAGIC %md
# MAGIC validador fecha

# COMMAND ----------



# COMMAND ----------

fecha_Convertida =df.withColumn(
    "fecha",
    coalesce( ##Dame columna1; si es NULL, dame columna2; si también es NULL, dame columna3.
        try_to_date(col("fecha"), "yyyy/MM/dd"),
        try_to_date(col("fecha"), "yyyy-MM-dd")
    )
)

fechas_invalidas = fecha_Convertida.filter(
    col("fecha").isNull()
)

if fechas_invalidas.count() > 0:
    fechas_invalidas.display()
    raise Exception(
        f"ERROR: se encontraron {fechas_invalidas.count()} fechas inválidas"
    )

paises_invalidos = fecha_Convertida.filter(
    (~col("pais").isin(["MX", "US", "BR"])) |
    (length(col("pais")) != 2))

if paises_invalidos.count() > 0:
    paises_invalidos.display()
    raise Exception(
        f"ERROR: se encontraron {paises_invalidos.count()} fechas inválidas"
    )

archivos_invalidos = df.filter(
    ~col("archivo").endswith(".csv")
)

if archivos_invalidos.count() > 0:
    paises_invalidos.display()
    raise Exception(
        f"ERROR: se encontraron {archivos_invalidos.count()} archivos que no terminan en .csv"
    )

registros_invalidos = df.filter(
    col("registros") <= 0
)

if registros_invalidos.count() > 0:
    paises_invalidos.display()
    raise Exception(
        f"ERROR: se encontraron {registros_invalidos.count()} registros "
        "con una cantidad no positiva"
    )

status_invalidos = fecha_Convertida.filter(
    (~col("estado").isin(["OK", "ERROR"])))

if status_invalidos.count() > 0:
    paises_invalidos.display()
    raise Exception(
        f"ERROR: se encontraron {status_invalidos.count()} registros "
        "con una cantidad no positiva"
    )

display(fecha_Convertida)


# COMMAND ----------

# MAGIC %md
# MAGIC validaror de ChatGPT
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------


# =====================================================
# NORMALIZACION DE FECHAS
# =====================================================

df_val = (
    df
    .withColumn(
        "fecha_convertida",
        coalesce(
            try_to_date(col("fecha"), "yyyy/MM/dd"),
            try_to_date(col("fecha"), "yyyy-MM-dd")
        )
    )
)

# =====================================================
# REGLAS DE CALIDAD
# =====================================================

validaciones = [
    {
        "nombre": "Fechas inválidas",
        "df": df_val.filter(
            col("fecha_convertida").isNull()
        )
    },
    {
        "nombre": "Países inválidos",
        "df": df_val.filter(
            (~col("pais").isin(["MX", "US", "BR"])) |
            (length(col("pais")) != 2)
        )
    },
    {
        "nombre": "Archivos inválidos",
        "df": df_val.filter(
            ~col("archivo").endswith(".csv")
        )
    },
    {
        "nombre": "Registros inválidos",
        "df": df_val.filter(
            col("registros") <= 0
        )
    },
    {
        "nombre": "Estados inválidos",
        "df": df_val.filter(
            ~col("estado").isin(["OK", "ERROR"])
        )
    }
]

# =====================================================
# EJECUCION DE VALIDACIONES
# =====================================================

errores = []

for regla in validaciones:

    cantidad = regla["df"].count()

    if cantidad > 0:

        print("\n" + "=" * 50)
        print(f"ERROR: {regla['nombre']}")
        print("=" * 50)

        display(regla["df"])

        errores.append(
            f"{regla['nombre']}: {cantidad}"
        )

# =====================================================
# RESULTADO FINAL
# =====================================================

if errores:

    mensaje_error = "\n".join(errores)

    raise Exception(
        f"""
VALIDACION DE CALIDAD FALLIDA

Se encontraron los siguientes errores:

{mensaje_error}
"""
    )

print("✅ Validación completada correctamente")

display(df_val)


# COMMAND ----------

df_val = df_val.withColumn(
    "anio",
    year("fecha_convertida")
).withColumn(
    "RegistroEnMiles",
    col("registros") / 1000
)

df_val.orderBy("registros").show()

df_val.orderBy(
    col("registros").desc()
).show()

df_val.display()

# COMMAND ----------

# MAGIC %md
# MAGIC mostrar unicamente las columans pais, archivo , estado

# COMMAND ----------

df_val.orderBy("registros").show()

df_val.orderBy(
    col("registros").desc()
).show()
resumen = df_val.select(
    "pais",
    "archivo",
    "estado"
)

resumen2 = df_val.groupBy("pais").agg(
    count("*").alias("numero_ventas"),
    sum("registros").alias("registros_totales"),
)

conteoEstado=resumen.groupBy("estado").count()
conteoEstado.display()

resumen.display()

resumen2.display()

# COMMAND ----------

# MAGIC %md
# MAGIC archivos por dia

# COMMAND ----------

archivosPorDia=df_val.groupBy("fecha_convertida").count()
archivosPorDia.display()

##opcion con columana renosbrada
from pyspark.sql.functions import count

archivosPorDia = df_val.groupBy("fecha_convertida").agg(
    count("*").alias("archivos_procesados")
    ).orderBy("fecha_convertida")

display(archivosPorDia)

# COMMAND ----------

# MAGIC %md
# MAGIC Reto 9 - Caso real de operación D&A
# MAGIC
# MAGIC Encontrar únicamente los países que superaron 20,000 registros acumulados.
# MAGIC
# MAGIC Requisitos:
# MAGIC
# MAGIC Agrupar por país.
# MAGIC Sumar registros.
# MAGIC Filtrar mayores a 20,000.
# MAGIC Ordenar descendente.

# COMMAND ----------

archivosPorDia = df_val.groupBy("pais").agg(
    sum("registros").alias("archivos_procesados")
    ).orderBy(col("pais").desc()
    ).filter(col("archivos_procesados") > 20000)

display(archivosPorDia)