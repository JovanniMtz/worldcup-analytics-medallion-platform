# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import col, count, sum, try_to_date, coalesce, length, year, concat_ws, when, lit


# COMMAND ----------

ventas_data = [
    (1001, 1, "P01", 10, 1500),
    (1002, 2, "P02", 5, 800),
    (1003, 3, "P03", 8, 1200),
    (1004, 4, "P99", 2, 300),
    (1005, 99, "P01", 7, 1000),
    (1006, 2, "P05", 4, 600),
    (1007, 8, "P02", 3, 450),
    (1008, 1, "P10", 1, 150)
]

df_ventas = spark.createDataFrame(
    ventas_data,
    ["venta_id", "cliente_id", "producto_id", "cantidad", "monto"]
)

clientes_data = [
    (1, "Bimbo Mexico"),
    (2, "Barcel"),
    (3, "Ricolino"),
    (4, "Tía Rosa"),
    (5, "El Globo")
]

df_clientes= spark.createDataFrame(
    clientes_data,
    ["cliente_id", "cliente_nombre"]
)
df_ventas.show()
df_clientes.display()

productos_data = [
    ("P01", "Pan Blanco"),
    ("P02", "Donitas"),
    ("P03", "Mantecadas"),
    ("P04", "Roles"),
    ("P05", "Tortillinas")
]

df_productos = spark.createDataFrame(
    productos_data,
    ["producto_id", "producto_nombre"]
)
df_productos.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ¿Qué problemas existen en estos datos?
# MAGIC
# MAGIC Tu trabajo será descubrirlos utilizando JOINs.

# COMMAND ----------

# MAGIC %md
# MAGIC RETO 1
# MAGIC Comprensión de los datos
# MAGIC
# MAGIC Antes de hacer cualquier JOIN:
# MAGIC
# MAGIC Obtén:
# MAGIC
# MAGIC - Total de registros en cada tabla
# MAGIC - Total de clientes
# MAGIC - Total de productos
# MAGIC - Total de ventas
# MAGIC
# MAGIC Preguntas:
# MAGIC
# MAGIC - ¿Cuál es la tabla principal?
# MAGIC - ¿Cuáles son tablas de referencia?
# MAGIC - ¿Cuál sería la llave para relacionarlas?
# MAGIC

# COMMAND ----------

#total de registros en cada tabla
print("Total de registros en df_ventas:", df_ventas.count())
print("Total de registros en df_clientes:", df_clientes.count())
print("Total de registros en df_productos:", df_productos.count())

print("Cada registro en la tabla consiste a un elemnto de la tabla")



# COMMAND ----------

# MAGIC %md
# MAGIC **Cómo pensar las tablas**
# MAGIC Tabla Ventas
# MAGIC Plain Text
# MAGIC 1
# MAGIC venta_id
# MAGIC 2
# MAGIC cliente_id
# MAGIC 3
# MAGIC producto_id
# MAGIC 4
# MAGIC cantidad
# MAGIC 5
# MAGIC monto
# MAGIC Mostrar más líneas
# MAGIC
# MAGIC **VENTAS ES la tabla principal.**
# MAGIC
# MAGIC ¿Por qué?
# MAGIC
# MAGIC Porque contiene el hecho de negocio.
# MAGIC
# MAGIC Cada fila representa una venta.
# MAGIC
# MAGIC En Data Engineering normalmente la llamamos:
# MAGIC **Fact Table
# MAGIC ** o **Tabla transaccional**
# MAGIC
# MAGIC
# MAGIC Tabla Clientes
# MAGIC cliente_id
# MAGIC cliente_nombre
# MAGIC
# MAGIC No contiene ventas.
# MAGIC Solo información descriptiva del cliente.
# MAGIC
# MAGIC Se considera una:
# MAGIC
# MAGIC Dimensión
# MAGIC
# MAGIC Mostrar más líneas
# MAGIC Tabla Productos
# MAGIC
# MAGIC producto_id
# MAGIC
# MAGIC producto_nombre
# MAGIC
# MAGIC Tampoco contiene ventas.
# MAGIC
# MAGIC También es una:
# MAGIC
# MAGIC Dimensión

# COMMAND ----------

# MAGIC %md
# MAGIC                ### Relación entre tablas
# MAGIC
# MAGIC ```text
# MAGIC        +-------------+
# MAGIC        |  Clientes   |
# MAGIC        +-------------+
# MAGIC        | cliente_id  |
# MAGIC        +------+------+
# MAGIC               |
# MAGIC               |
# MAGIC               |
# MAGIC        +------+------+
# MAGIC        |   Ventas   |
# MAGIC        +-------------+
# MAGIC        | cliente_id  |
# MAGIC        | producto_id |------------------+
# MAGIC        +-------------+                  |
# MAGIC                                         |
# MAGIC                                         |
# MAGIC                                +--------+--------+
# MAGIC                                |    Productos    |
# MAGIC                                +-----------------+
# MAGIC                                | producto_id     |
# MAGIC                                +-----------------+
# MAGIC ```
# MAGIC
# MAGIC                        +---------------+

# COMMAND ----------

# MAGIC %md
# MAGIC 1. ¿Cuál es la Fact Table? ventas
# MAGIC
# MAGIC 2. ¿Cuáles son las dimensiones? 
# MAGIC Dimensión 1: Clientes
# MAGIC Dimensión 2: Productos
# MAGIC
# MAGIC 3. ¿Qué columnas usarás para hacer los JOINs? cliente id, produc id

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # **RETO 2**
# MAGIC Primer INNER JOIN
# MAGIC
# MAGIC Realiza un INNER JOIN entre:
# MAGIC
# MAGIC Ventas + Clientes
# MAGIC
# MAGIC Obtén:
# MAGIC
# MAGIC - 1 venta_id
# MAGIC - 2 cliente_nombre
# MAGIC - 3monto
# MAGIC
# MAGIC Analiza:
# MAGIC
# MAGIC - ¿Se conservaron todas las ventas?
# MAGIC     en la tabla inner entre ventas y clientes, se agregaron al final la culumanas de la tabla cliente
# MAGIC - ¿Se perdieron registros?
# MAGIC
# MAGIC - ¿Por qué?
# MAGIC     solo se convervan los registros en donde ambos tienen datos

# COMMAND ----------

# MAGIC %md
# MAGIC ![image_1786174824039.png](./image_1786174824039.png "image_1786174824039.png")
# MAGIC
# MAGIC

# COMMAND ----------


df_clientes.display()

df_ventas.display()
#inner joinn
df_join = df_ventas.join(##dataframe origen
    df_clientes, ##dataframe a hacer match
    df_ventas["cliente_id"] == df_clientes["cliente_id"], ##columnas a hacer match
    "inner" ##tipo de join
)

print("Total de registros en df_ventas:", df_ventas.count())
print("Total de registros en df_join:", df_join.count())
print("Cada registro en la tabla consiste a un elemnto de la tabla")


display(df_join)

# COMMAND ----------


df_join = df_ventas.join(
    df_clientes,
    df_ventas["cliente_id"] == df_clientes["cliente_id"],
    "inner"
).select(
    df_ventas["venta_id"],
    df_clientes["cliente_nombre"],
    df_ventas["monto"]
)
df_join.display()


# COMMAND ----------

# MAGIC %md
# MAGIC # **Reto** 3
# MAGIC Segundo INNER JOIN
# MAGIC
# MAGIC Ahora debes enriquecer las ventas con información de:
# MAGIC
# MAGIC Clientes
# MAGIC Productos

# COMMAND ----------

df_join = df_ventas.join(
    df_clientes,
    df_ventas["cliente_id"] == df_clientes["cliente_id"],
    "inner"
).join(
    df_productos,
    df_ventas["producto_id"] == df_productos["producto_id"],
    "inner"
)
display(df_join.count())
df_join.display()


df_join.select(
    "venta_id",
    "cliente_nombre",
    "producto_nombre",
    "cantidad",
    "monto"
).display()

#muetra unicamente 4 filas por que es donde hacen interseccion los 3

# COMMAND ----------

# MAGIC %md
# MAGIC Reto 4 Left join 
# MAGIC la tabla origen muestra todas sus fila y agraga datos de las demas tablas disponib les, si no hay los deja en nulos
# MAGIC ![image_1786178293173.png](./image_1786178293173.png "image_1786178293173.png")

# COMMAND ----------

# MAGIC %md
# MAGIC **ventas LEFT JOIN clientes**

# COMMAND ----------

df_join = df_ventas.join(
    df_clientes,
    df_ventas["cliente_id"] == df_clientes["cliente_id"],
    "left"
)
print("Total de registros en df_ventas:", df_ventas.count())
print("Total de registros en df_join:", df_join.count())
print("Cada registro en la tabla consiste a un elemnto de la tabla ventas")
df_clientes.display()
df_join.display()


# COMMAND ----------

# MAGIC %md
# MAGIC **RETO** 5
# MAGIC LEFT JOIN DOBLE
# MAGIC
# MAGIC Cruza:
# MAGIC
# MAGIC Ventas
# MAGIC Clientes
# MAGIC Productos

# COMMAND ----------

df_join = df_ventas.join(
    df_clientes,
    df_ventas["cliente_id"] == df_clientes["cliente_id"],
    "left"
).join(
    df_productos,
    df_ventas["producto_id"] == df_productos["producto_id"],
    "left"
)

display(df_join.count())
df_join.display()

df_join.select(
    "venta_id",
    "cliente_nombre",
    "producto_nombre",
    "cantidad",
    "monto"
).display()

#muetra unicamente 4 filas por que es donde

# COMMAND ----------

# MAGIC %md
# MAGIC RETO 6
# MAGIC **LEFT ANTI JOIN (IMPORTANTE)
# MAGIC **Validación de Clientes
# MAGIC **ventas en dond el cliente no existe**

# COMMAND ----------

# MAGIC %md
# MAGIC left outer y lef join son los mismo
# MAGIC left anit join muestra los datos en donde la tabla destino no tiene datos o son  nulos, vea el ejemplo
# MAGIC
# MAGIC

# COMMAND ----------

df_join_left= df_ventas.join(
    df_clientes,
    df_ventas["cliente_id"] == df_clientes["cliente_id"],
    "left"
)
df_join = df_ventas.join(
    df_clientes,
    df_ventas["cliente_id"] == df_clientes["cliente_id"],
    "left_anti"
)
df_join_outer = df_ventas.join(
    df_clientes,
    df_ventas["cliente_id"] == df_clientes["cliente_id"],
    "left_outer"
)


print("Total de registros en df_ventas:", df_ventas.count())
print("Total de registros en df_join:", df_join.count())
print("Cada registro en la tabla consiste a un elemnto de la tabla ventas")
df_join_left.display()
df_join.display()
df_join_outer.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # **Reto 7**
# MAGIC # ventas en donde no se vendio nada
# MAGIC left join con productos

# COMMAND ----------

df_left_productos=df_ventas.join(
    df_productos,
    df_ventas["producto_id"] == df_productos["producto_id"],
    "left"
)
df_left_productos.display()

df_left__anti_productos=df_ventas.join(
    df_productos,
    df_ventas["producto_id"] == df_productos["producto_id"],
    "left_anti"
)
df_left__anti_productos.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **# RETO 8**
# MAGIC Indicador de Ventas por Cliente
# MAGIC
# MAGIC Usando JOIN + GROUP BY

# COMMAND ----------





df_inner = df_ventas.join(
    df_clientes,
    df_ventas["cliente_id"] == df_clientes["cliente_id"],
    "inner"
).groupBy(
    df_clientes["cliente_nombre"]
).agg(
    count("*").alias("ventas"),
    sum("monto").alias("total")
).orderBy(
    col("total").desc()
)
display(df_inner)

# COMMAND ----------

# MAGIC %md
# MAGIC Ventas por Producto
# MAGIC
# MAGIC Similar al Reto 8
# MAGIC
# MAGIC obtener
# MAGIC producto_nombre
# MAGIC unidades_vendidas
# MAGIC monto_total
# MAGIC
# MAGIC ordenado por 
# MAGIC monto_total DESC

# COMMAND ----------


display(df_productos)

df_inner= df_ventas.join(
    df_productos,
    df_ventas["producto_id"] == df_productos["producto_id"],
    "inner"
).groupBy(
    df_productos["producto_nombre"]
).agg(
    count("*").alias("ventas"),
    sum("monto").alias("total")
).orderBy(
    col("total").desc()
)
display(df_inner)

# COMMAND ----------

# MAGIC %md
# MAGIC **# Reto 10
# MAGIC # Top Cliente**
# MAGIC El cliente con mayor monto vendido
# MAGIC Mostrar más líneas

# COMMAND ----------

df_inner = df_ventas.join(
    df_clientes,
    df_ventas["cliente_id"] == df_clientes["cliente_id"],
    "inner"
).groupBy(
    df_clientes["cliente_nombre"]
).agg(
    count("*").alias("ventas"),
    sum("monto").alias("total")
).orderBy(
    col("total").desc()
).limit(1)
display(df_inner)

# COMMAND ----------

# MAGIC %md
# MAGIC # Reto: Calidad de los datos de ventas
# MAGIC
# MAGIC Debes analizar la **calidad de los datos** de las ventas recibidas, validando la información asociada a **clientes** y **productos**.
# MAGIC
# MAGIC ## Calidad de Clientes
# MAGIC
# MAGIC Debes calcular:
# MAGIC
# MAGIC * **Total de ventas:** número total de ventas recibidas.
# MAGIC * **Total de ventas con cliente válido:** ventas cuyo cliente cumple con todos los datos requeridos.
# MAGIC * **Total de ventas con cliente inválido:** ventas cuyo cliente no existe o no cumple con alguno de los datos requeridos.
# MAGIC * **Porcentaje de calidad de clientes:** porcentaje de ventas que cuentan con un cliente válido respecto al total de ventas.
# MAGIC
# MAGIC ## Calidad de Productos
# MAGIC
# MAGIC Debes calcular:
# MAGIC
# MAGIC * **Total de ventas:** número total de ventas recibidas.
# MAGIC * **Total de ventas con producto válido:** ventas cuyo producto cumple con todos los datos requeridos.
# MAGIC * **Total de ventas con producto inválido:** ventas cuyo producto no existe o no cumple con alguno de los datos requeridos.
# MAGIC * **Porcentaje de calidad de productos:** porcentaje de ventas que cuentan con un producto válido respecto al total de ventas.
# MAGIC
# MAGIC ### Resultado esperado
# MAGIC
# MAGIC Presenta los resultados de forma clara, mostrando las métricas de **calidad de clientes** y **calidad de productos**.
# MAGIC

# COMMAND ----------

    Total_ventas=df_ventas.count()
    Total_ventas_cliente_valido = df_ventas.join(
        df_clientes,
        df_ventas["cliente_id"] == df_clientes["cliente_id"],
        "inner"
    ).count()

    Total_ventas_cliente_invavalido = df_ventas.join(
        df_clientes,
        df_ventas["cliente_id"] == df_clientes["cliente_id"],
        "left_anti"
    ).count()

    print("Total de ventas:", Total_ventas)
    print("Total de ventas con cliente valido:", Total_ventas_cliente_valido)
    print("Total de ventas con cliente invalido:", Total_ventas_cliente_invavalido)

    calidadClientes=Total_ventas_cliente_valido*100/Total_ventas
    print(f"Porcentaje calidad clientes: {calidadClientes:.2f}%")

    Total_ventas_producto_valido = df_ventas.join(
        df_productos,
        df_ventas["producto_id"] == df_productos["producto_id"],
        "inner"
    ).count()

    Total_ventas_producto_invavalido = df_ventas.join(
        df_productos,
        df_ventas["producto_id"] == df_productos["producto_id"],
        "left_anti"
    ).count()

    print("Total de ventas:", Total_ventas)
    print("Total de ventas con producto valido:", Total_ventas_producto_valido)
    print("Total de ventas con producto invalido:", Total_ventas_producto_invavalido)

    calidadproductos=Total_ventas_producto_valido*100/Total_ventas
    print(f"Porcentaje calidad productos: {calidadproductos:.2f}%")

# COMMAND ----------

# MAGIC %md
# MAGIC Reto 12 - Construcción de la Capa Silver
# MAGIC Contexto
# MAGIC
# MAGIC Actualmente contamos con una tabla de ventas (Bronze) que contiene tanto registros válidos como registros con problemas de calidad de datos.
# MAGIC
# MAGIC Algunos ejemplos de registros invá lidos son:
# MAGIC
# MAGIC Clientes que no existen en el catálogo maestro de clientes.
# MAGIC Productos que no existen en el catálogo maestro de productos.
# MAGIC
# MAGIC El objetivo es construir una capa Silver que contenga únicamente información confiable y lista para ser utilizada en reportes, análisis y procesos posteriores.
# MAGIC
# MAGIC Objetivo
# MAGIC
# MAGIC Construir un DataFrame llamado: ventas_silver
# MAGIC
# MAGIC Reglas de Negocio
# MAGIC
# MAGIC Una venta será considerada válida cuando:
# MAGIC
# MAGIC Regla 1
# MAGIC
# MAGIC El cliente exista en el catálogo maestro de clientes.
# MAGIC
# MAGIC Regla 2
# MAGIC
# MAGIC El producto exista en el catálogo maestro de productos.
# MAGIC
# MAGIC Estructura Esperada
# MAGIC
# MAGIC El DataFrame final debe contener las siguientes columnas:
# MAGIC
# MAGIC - venta_id
# MAGIC - cliente_id
# MAGIC - cliente_nombre
# MAGIC - producto_id
# MAGIC - producto_nombre
# MAGIC - cantidad
# MAGIC - monto
# MAGIC
# MAGIC ¿Cuántos registros existen en Bronze?
# MAGIC ¿Cuántos registros existen en Silver?
# MAGIC ¿Cuántos registros existen en Bronze?
# MAGIC ¿Cuántos registros existen en Silver?
# MAGIC
# MAGIC ¿Por qué estas ventas no deberían formar parte de Silver?
# MAGIC ¿Qué riesgos generarían en reportes o análisis de negocio?
# MAGIC
# MAGIC ¿Qué diferencias existen entre la capa Bronze y la capa Silver?
# MAGIC ¿Por qué Silver tiene una mejor calidad de datos?
# MAGIC
# MAGIC cliente_id inválidos
# MAGIC
# MAGIC producto_id inválidos
# MAGIC
# MAGIC ¿Es mejor eliminar los registros inválidos o conservarlos en una tabla separada?
# MAGIC
# MAGIC ¿Por qué?
# MAGIC
# MAGIC clientes_invalidos
# MAGIC
# MAGIC productos_invalidos

# COMMAND ----------

ventas_silver = (
    df_ventas.join(
        df_productos, df_ventas["producto_id"] == df_productos["producto_id"], "inner"
    )
    .join(df_clientes, df_ventas["cliente_id"] == df_clientes["cliente_id"], "inner")
    .select(
        "venta_id",
        df_ventas["cliente_id"],
        "cliente_nombre",
        df_productos["producto_id"],
        "producto_nombre",
        "cantidad",
        "monto",
    )
)
display(ventas_silver)

print("Registros existentes en bronce:", df_ventas.count())
print("Registros existentes en silver:", ventas_silver.count())

registrosEliminado=df_ventas.count()-ventas_silver.count()
print("Ventas eliminados:", registrosEliminado)

print("Clientes o productos que causaron eliminacion:")

ventas_silver_eliminados = (
    df_ventas
    .join(
        df_productos,
        df_ventas["producto_id"] == df_productos["producto_id"],
        "left"
    )
    .join(
        df_clientes,
        df_ventas["cliente_id"] == df_clientes["cliente_id"],
        "left"
    ).select(
        "venta_id",
        df_clientes["cliente_id"],
        "cliente_nombre",
        df_productos["producto_id"],
        "producto_nombre",
        "cantidad",
        "monto",
    )
    .withColumn(
        "nulos",
        concat_ws(
            ", ",
            *[
                when(col(c).isNull(), lit(c))
                for c in df_ventas.columns
            ]
        )
    )
    .filter(col("nulos") != "")
)


ventas_silver_eliminados.display()

print("Evaluar si existen registros de ventas eliminado en la capa silver")

df_comparacion=ventas_silver.join(
    ventas_silver_eliminados,
    ventas_silver["venta_id"] == ventas_silver_eliminados["venta_id"],
    "inner"
)
display(df_comparacion)




# COMMAND ----------

# MAGIC %md
# MAGIC Codigo de chat para responder el reto 12
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC from pyspark.sql.functions import *
# MAGIC
# MAGIC # =====================================
# MAGIC # CAPA SILVER
# MAGIC # =====================================
# MAGIC
# MAGIC ventas_silver = (
# MAGIC     df_ventas
# MAGIC     .join(
# MAGIC         df_clientes,
# MAGIC         df_ventas["cliente_id"] == df_clientes["cliente_id"],
# MAGIC         "inner"
# MAGIC     )
# MAGIC     .join(
# MAGIC         df_productos,
# MAGIC         df_ventas["producto_id"] == df_productos["producto_id"],
# MAGIC         "inner"
# MAGIC     )
# MAGIC     .select(
# MAGIC         df_ventas["venta_id"],
# MAGIC         df_ventas["cliente_id"],
# MAGIC         df_clientes["cliente_nombre"],
# MAGIC         df_ventas["producto_id"],
# MAGIC         df_productos["producto_nombre"],
# MAGIC         df_ventas["cantidad"],
# MAGIC         df_ventas["monto"]
# MAGIC     )
# MAGIC )
# MAGIC
# MAGIC display(ventas_silver)
# MAGIC
# MAGIC # =====================================
# MAGIC # COMPARATIVO BRONZE VS SILVER
# MAGIC # =====================================
# MAGIC
# MAGIC total_bronze = df_ventas.count()
# MAGIC total_silver = ventas_silver.count()
# MAGIC
# MAGIC print(f"Registros Bronze: {total_bronze}")
# MAGIC print(f"Registros Silver: {total_silver}")
# MAGIC print(f"Registros descartados: {total_bronze-total_silver}")
# MAGIC
# MAGIC # =====================================
# MAGIC # VENTAS DESCARTADAS
# MAGIC # =====================================
# MAGIC
# MAGIC ventas_descartadas = (
# MAGIC     df_ventas.join(
# MAGIC         ventas_silver.select("venta_id"),
# MAGIC         "venta_id",
# MAGIC         "left_anti"
# MAGIC     )
# MAGIC )
# MAGIC
# MAGIC print("Ventas descartadas")
# MAGIC
# MAGIC display(ventas_descartadas)
# MAGIC
# MAGIC # =====================================
# MAGIC # CLIENTES INVALIDOS
# MAGIC # =====================================
# MAGIC
# MAGIC clientes_invalidos = (
# MAGIC     df_ventas.join(
# MAGIC         df_clientes,
# MAGIC         df_ventas["cliente_id"] == df_clientes["cliente_id"],
# MAGIC         "left_anti"
# MAGIC     )
# MAGIC )
# MAGIC
# MAGIC print("Clientes invalidos")
# MAGIC
# MAGIC display(clientes_invalidos)
# MAGIC
# MAGIC # =====================================
# MAGIC # PRODUCTOS INVALIDOS
# MAGIC # =====================================
# MAGIC
# MAGIC productos_invalidos = (
# MAGIC     df_ventas.join(
# MAGIC         df_productos,
# MAGIC         df_ventas["producto_id"] == df_productos["producto_id"],
# MAGIC         "left_anti"
# MAGIC     )
# MAGIC )
# MAGIC
# MAGIC print("Productos invalidos")
# MAGIC
# MAGIC display(productos_invalidos)

# COMMAND ----------

# MAGIC %md
# MAGIC # 🚀 Siguiente reto: Reto 13 — Múltiples Dimensiones
# MAGIC
# MAGIC Hasta ahora hemos trabajado con:
# MAGIC
# MAGIC **Ventas + Clientes + Productos**
# MAGIC
# MAGIC En este reto agregaremos una nueva dimensión para llevar nuestro análisis un paso más adelante.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC # 🎯 Reto 13 — Análisis por Región
# MAGIC
# MAGIC ## 📌 Contexto
# MAGIC
# MAGIC La empresa quiere analizar sus ventas por **región geográfica**.
# MAGIC
# MAGIC Además de los catálogos de **Clientes** y **Productos**, ahora existe una nueva dimensión llamada **Regiones**.
# MAGIC
# MAGIC El objetivo será construir un DataFrame enriquecido utilizando **múltiples dimensiones** y posteriormente generar un reporte de ventas por región.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC # 🗂️ Nueva tabla: Regiones
# MAGIC
# MAGIC Utiliza los siguientes datos:
# MAGIC
# MAGIC ```python
# MAGIC regiones_data = [
# MAGIC     (1, "Mexico"),
# MAGIC     (2, "LATAM"),
# MAGIC     (3, "USA"),
# MAGIC     (4, "Europa"),
# MAGIC     (5, "Asia")
# MAGIC ]
# MAGIC ```
# MAGIC
# MAGIC Las columnas son:
# MAGIC
# MAGIC ```text
# MAGIC cliente_id
# MAGIC region
# MAGIC ```
# MAGIC
# MAGIC ### Actividad inicial
# MAGIC
# MAGIC Crea el DataFrame:
# MAGIC
# MAGIC ```python
# MAGIC df_regiones
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC # 🎯 Objetivo principal
# MAGIC
# MAGIC Construir un DataFrame enriquecido realizando las siguientes relaciones:
# MAGIC
# MAGIC ```text
# MAGIC Ventas
# MAGIC    │
# MAGIC    ├── Clientes
# MAGIC    │
# MAGIC    ├── Productos
# MAGIC    │
# MAGIC    └── Regiones
# MAGIC ```
# MAGIC
# MAGIC El resultado debe integrar la información de las cuatro fuentes.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC # 🧩 Actividad 1 — DataFrame enriquecido
# MAGIC
# MAGIC Genera un DataFrame que contenga únicamente las siguientes columnas:
# MAGIC
# MAGIC ```text
# MAGIC venta_id
# MAGIC cliente_nombre
# MAGIC producto_nombre
# MAGIC region
# MAGIC cantidad
# MAGIC monto
# MAGIC ```
# MAGIC
# MAGIC 💡 **Pista:** tendrás que realizar múltiples `join()` para relacionar las diferentes dimensiones.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC # 📊 Actividad 2 — Reporte por región
# MAGIC
# MAGIC Genera un reporte agrupado por región.
# MAGIC
# MAGIC El resultado debe contener:
# MAGIC
# MAGIC ```text
# MAGIC region
# MAGIC ventas
# MAGIC monto_total
# MAGIC ```
# MAGIC
# MAGIC Donde:
# MAGIC
# MAGIC ```text
# MAGIC ventas = número de transacciones
# MAGIC ```
# MAGIC
# MAGIC y:
# MAGIC
# MAGIC ```text
# MAGIC monto_total = suma del monto vendido
# MAGIC ```
# MAGIC
# MAGIC 💡 **Pista:** utiliza las operaciones que ya conoces:
# MAGIC
# MAGIC ```python
# MAGIC groupBy()
# MAGIC count()
# MAGIC sum()
# MAGIC agg()
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC # 📈 Actividad 3 — Ordenamiento
# MAGIC
# MAGIC Ordena el resultado de mayor a menor utilizando:
# MAGIC
# MAGIC ```text
# MAGIC monto_total DESC
# MAGIC ```
# MAGIC
# MAGIC El objetivo es identificar rápidamente qué región genera el mayor monto de ventas.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC # 🧠 Actividad 4 — Análisis
# MAGIC
# MAGIC A partir del resultado obtenido, responde:
# MAGIC
# MAGIC 1. ¿Qué región tiene más ventas?
# MAGIC
# MAGIC 2. ¿Qué región tiene el mayor monto vendido?
# MAGIC
# MAGIC 3. ¿Existen regiones sin ventas?
# MAGIC
# MAGIC 4. ¿Por qué podría ocurrir esto en un ambiente real?
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC # 🏗️ Actividad 5 — Pensamiento ETL
# MAGIC
# MAGIC Imagina que mañana llega una nueva venta con:
# MAGIC
# MAGIC ```text
# MAGIC cliente_id = 999
# MAGIC ```
# MAGIC
# MAGIC Sin embargo, ese cliente **no existe** en:
# MAGIC
# MAGIC ```text
# MAGIC Clientes
# MAGIC Regiones
# MAGIC ```
# MAGIC
# MAGIC Analiza el caso desde una perspectiva de calidad de datos.
# MAGIC
# MAGIC ### Responde:
# MAGIC
# MAGIC 1. ¿Debe pasar esta venta a la capa Silver?
# MAGIC
# MAGIC 2. ¿Por qué?
# MAGIC
# MAGIC 3. ¿Qué tipo de problema de calidad de datos representa?
# MAGIC
# MAGIC 💡 **Piensa en lo que trabajaste anteriormente sobre registros válidos, inválidos y datos faltantes.**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC # ⭐ BONUS — Nivel Senior
# MAGIC
# MAGIC Construye un indicador por región con las siguientes columnas:
# MAGIC
# MAGIC ```text
# MAGIC region
# MAGIC clientes_unicos
# MAGIC ventas
# MAGIC monto_total
# MAGIC ```
# MAGIC
# MAGIC Donde:
# MAGIC
# MAGIC ```text
# MAGIC clientes_unicos = número de clientes diferentes que realizaron compras
# MAGIC ```
# MAGIC
# MAGIC Ya conoces:
# MAGIC
# MAGIC ```python
# MAGIC count()
# MAGIC sum()
# MAGIC groupBy()
# MAGIC agg()
# MAGIC ```
# MAGIC
# MAGIC ### 🔎 Pista
# MAGIC
# MAGIC Investiga cómo realizar un **conteo de valores únicos** en PySpark.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC # 🏆 Objetivo del reto
# MAGIC
# MAGIC Al finalizar este reto deberías ser capaz de:
# MAGIC
# MAGIC * Trabajar con **múltiples dimensiones**.
# MAGIC * Realizar varios `join()` en una misma transformación.
# MAGIC * Enriquecer un DataFrame de hechos con diferentes catálogos.
# MAGIC * Utilizar `groupBy()` y `agg()` para generar reportes.
# MAGIC * Contar transacciones.
# MAGIC * Calcular sumas y totales.
# MAGIC * Obtener clientes únicos.
# MAGIC * Identificar problemas de integridad referencial.
# MAGIC * Pensar en términos de **calidad de datos y procesos ETL**.
# MAGIC
# MAGIC Este reto representa un paso más hacia un flujo de datos similar al que encontrarías en un ambiente real de **Data Engineering**.
# MAGIC

# COMMAND ----------

regiones_data = [(1, "Mexico"), (2, "LATAM"), (3, "USA"), (4, "Europa"), (5, "Asia")]

df_regiones = spark.createDataFrame(regiones_data, ["cliente_id", "region"])


reporte_regiones = (
    ventas_silver.join(
        df_regiones, ventas_silver["cliente_id"] == df_regiones["cliente_id"], "inner"
    )
    
)

display(reporte_regiones)
reporte_regiones=reporte_regiones.groupBy("region").agg(
    count("venta_id").alias("ventas"),
    countDistinct("cliente_id").alias("clientes"),
    sum("monto").alias("monto_total")
    ).orderBy(col("monto_total").desc())

display(reporte_regiones)

