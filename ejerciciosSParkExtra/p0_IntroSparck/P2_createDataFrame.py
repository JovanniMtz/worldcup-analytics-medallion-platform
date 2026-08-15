# Databricks notebook source
datos = [
    (1, "Juan", 25),
    (2, "Ana", 30)
]

df = spark.createDataFrame(
    datos,
    ["id","nombre","edad"]
)

print("Un DataFrame es una tabla en memoria, muy parecida a una hoja de Excel o una tabla SQL.")
print("Tiene estructura (columnas).\nTiene tipos de datos.\nPuede optimizar consultas.\nPuede procesar millones o miles de millones de registros.")

display(df)

print("\n\nPuedes ver el esquema con df.printSchema() ,, esta operacion muestra los tipos de datos de las columnas  ")
df.printSchema()

print("\n\nPuedes ver el esquema con df.schema esta operacion muestra los tipos de datos de las columnas")
df.schema



filtroEDAD=df.filter(df.edad > 27)
print("El friltro de edad dice que tenemos los mayores de 27")
#print("El resultado es:"+ filtroEDAD) NO SIRVE
display(filtroEDAD)

edad=filtroEDAD.edad
print("La edad es:"+ str(edad))

edad = filtroEDAD.collect()[0]["edad"]

print("La edad es:",edad)

print("\n\nMas formas de extraer datos\n\n")

filtroEDAD = df.filter(df.edad > 27)

display(filtroEDAD)

fila = filtroEDAD.first()

print("Nombre:", fila["nombre"])
print("Edad:", fila["edad"])
      

