"""
Calidad del aire en Bogotá: procesamiento en batch con Spark.

Fuente de datos: IDEAM, conjunto "Calidad del Aire en Colombia" (g4t8-zkc3),
Portal Nacional de Datos Abiertos (datos.gov.co).

Cómo usarlo en Google Colab: crear un cuaderno nuevo y copiar cada bloque
(los que empiezan con "# %%") en una celda, ejecutándolas en orden.
Las líneas que empiezan con "# !" son comandos de Colab: quita el "# " para ejecutarlas.
"""


# ======================================================================
# Calidad del aire en Bogotá: procesamiento en batch con Spark
# ======================================================================


# ======================================================================
# Parte A. Procesamiento en batch
# ======================================================================

# %%
# Instalar Java 17 y PySpark 3.5.3 (tarda un par de minutos)
# !apt-get -qq update
# !apt-get -qq install -y openjdk-17-jdk-headless > /dev/null
# !pip -q install pyspark==3.5.3

# %%
import os
os.environ['JAVA_HOME'] = '/usr/lib/jvm/java-17-openjdk-amd64'

from pyspark.sql import SparkSession, functions as F

spark = (SparkSession.builder
         .master('local[*]')
         .appName('AireBogota')
         .config('spark.driver.memory', '4g')
         
         .getOrCreate())
spark.sparkContext.setLogLevel('ERROR')
print('Spark', spark.version)


# ======================================================================
# 2. Cargar los datos desde la fuente original
# ======================================================================

# %%
import io, requests, pandas as pd

URL = 'https://www.datos.gov.co/resource/g4t8-zkc3.csv'
MAX_FILAS = 500_000   # <-- cambia este número (50_000 para probar rápido)
POR_PAGINA = 50_000   # el portal entrega máximo 50.000 filas por petición

# Mirar las columnas reales del conjunto de datos
muestra = pd.read_csv(io.StringIO(requests.get(URL, params={'$limit': 5}, timeout=120).text))
print(list(muestra.columns))
muestra.head()

# %%
def buscar(*claves):
    """Devuelve la columna que corresponde a alguna de las claves."""
    columnas = list(muestra.columns)
    for clave in claves:                 # 1) nombre exacto
        if clave in columnas:
            return clave
    for clave in claves:                 # 2) nombre que contenga la clave
        for c in columnas:
            if clave in c.lower():
                return c
    raise ValueError(f'No encontré una columna para {claves}. Escríbela a mano. Columnas: {columnas}')

COL_DEP   = buscar('departamento')
COL_EST   = buscar('nombre_est', 'estacion')
COL_CONT  = buscar('msfl', 'variable', 'contamin')
COL_CONC  = buscar('concentracion_estandar', 'concentracion')
COL_FECHA = buscar('fecha_inicio')

# Si alguna columna quedó mal, corrígela a mano aquí, por ejemplo: COL_CONT = 'nombre_de_la_columna'
print('Departamento :', COL_DEP)
print('Estación     :', COL_EST)
print('Contaminante :', COL_CONT)
print('Concentración:', COL_CONC)
print('Fecha        :', COL_FECHA)

# %%
def pedir(offset):
    """Pide una página de datos de Bogotá a la API."""
    params = {'$where': f"upper({COL_DEP}) like '%BOGOT%'",   # solo Bogotá
              '$order': ':id', '$limit': POR_PAGINA, '$offset': offset}
    for intento in range(3):
        r = requests.get(URL, params=params, timeout=300)
        if r.status_code == 200:
            return r.text
        print('Error', r.status_code, r.text[:200])
    raise RuntimeError('No se pudo descargar la página')

ARCHIVO = '/content/aire_bogota.csv'
filas, offset = 0, 0
with open(ARCHIVO, 'w', encoding='utf-8') as f:
    while filas < MAX_FILAS:
        texto = pedir(offset)
        lineas = texto.splitlines()
        n = len(lineas) - 1                      # la primera línea es el encabezado
        if n <= 0:
            break
        if offset == 0:
            f.write('\n'.join(lineas) + '\n')
        else:
            f.write('\n'.join(lineas[1:]) + '\n')
        filas += n
        offset += POR_PAGINA
        print(f'Descargadas {filas:,} filas')
        if n < POR_PAGINA:
            break

print(f'\nTotal descargado (solo Bogotá): {filas:,} filas')

# %%
crudo = spark.read.csv(ARCHIVO, header=True, escape='"')
print(f'Filas cargadas en Spark: {crudo.count():,}')
crudo.printSchema()


# ======================================================================
# 3. Limpieza y transformación
# ======================================================================

# %%
df = crudo.select(
    F.col(COL_EST).alias('estacion'),
    F.col(COL_CONT).alias('contaminante'),
    F.col(COL_CONC).cast('double').alias('concentracion'),
    F.to_timestamp(F.col(COL_FECHA)).alias('fecha'))

n_inicial = df.count()

# a) quitar filas con datos vacíos
df = df.dropna()

# b) quitar concentraciones negativas (no tienen sentido físico)
df = df.filter(F.col('concentracion') >= 0)

# c) estandarizar el contaminante: columna 'pm' con valores PM2.5 o PM10
df = df.withColumn('pm',
        F.when(F.col('contaminante').rlike(r'(?i)pm\s*2[.,]?5'), 'PM2.5')
         .when(F.col('contaminante').rlike(r'(?i)pm\s*10'), 'PM10'))

# d) en material particulado, valores mayores a 1000 µg/m³ se consideran errores (criterio propio)
df = df.filter(~(F.col('pm').isNotNull() & (F.col('concentracion') > 1000)))

# e) columnas nuevas: hora, día y año
df = (df.withColumn('hora', F.hour('fecha'))
        .withColumn('dia', F.to_date('fecha'))
        .withColumn('anio', F.year('fecha')))
df.cache()

n_final = df.count()
print(f'Filas antes de limpiar : {n_inicial:,}')
print(f'Filas después de limpiar: {n_final:,}  (se eliminaron {n_inicial - n_final:,})')
df.show(5, truncate=False)


# ======================================================================
# 4. Análisis exploratorio (EDA)
# ======================================================================

# %%
print('Mediciones por contaminante:')
df.groupBy('contaminante').count().orderBy(F.desc('count')).show(20, truncate=False)

print('Rango de fechas:')
df.agg(F.min('fecha'), F.max('fecha')).show(truncate=False)

print('Estaciones distintas:', df.select('estacion').distinct().count())

# Nos quedamos con material particulado (PM2.5 y PM10) para el análisis
df_pm = df.filter(F.col('pm').isNotNull())
n_pm = df_pm.count()
print(f'Mediciones de PM2.5 y PM10: {n_pm:,}')
if n_pm == 0:
    raise ValueError('No hay PM2.5 ni PM10. Revisa los nombres de contaminante de la tabla de arriba y ajusta el paso c) de la limpieza.')

# %%
import matplotlib.pyplot as plt

prom_est = (df_pm.groupBy('estacion', 'pm')
            .agg(F.round(F.avg('concentracion'), 1).alias('promedio'), F.count('*').alias('n'))
            .orderBy('pm', F.desc('promedio')))
prom_est.show(40, truncate=False)

p = prom_est.toPandas().pivot(index='estacion', columns='pm', values='promedio')
p.plot.barh(figsize=(9, 6))
plt.xlabel('Concentración promedio (µg/m³)')
plt.title('PM2.5 y PM10 promedio por estación de Bogotá')
plt.tight_layout()
plt.show()

# %%
por_hora = (df_pm.groupBy('hora', 'pm')
            .agg(F.avg('concentracion').alias('promedio'))
            .orderBy('hora'))
ph = por_hora.toPandas().pivot(index='hora', columns='pm', values='promedio')

ph.plot(marker='o', figsize=(9, 4))
plt.xlabel('Hora del día')
plt.ylabel('Concentración promedio (µg/m³)')
plt.title('Promedio por hora del día')
plt.xticks(range(0, 24, 2))
plt.grid(alpha=0.3)
plt.show()

# %%
por_anio = (df_pm.groupBy('anio', 'pm')
            .agg(F.avg('concentracion').alias('promedio'))
            .orderBy('anio'))
pa = por_anio.toPandas().pivot(index='anio', columns='pm', values='promedio')

pa.plot.bar(figsize=(9, 4))
plt.xlabel('Año')
plt.ylabel('Concentración promedio (µg/m³)')
plt.title('Promedio por año')
plt.xticks(rotation=0)
plt.show()

# %%
diario = (df_pm.groupBy('estacion', 'pm', 'dia')
          .agg(F.avg('concentracion').alias('prom_dia'))
          .withColumn('limite', F.when(F.col('pm') == 'PM2.5', 37).otherwise(75)))

exced = (diario.groupBy('estacion', 'pm')
         .agg(F.count('*').alias('dias_medidos'),
              F.sum((F.col('prom_dia') > F.col('limite')).cast('int')).alias('dias_sobre_limite'))
         .withColumn('porcentaje', F.round(100 * F.col('dias_sobre_limite') / F.col('dias_medidos'), 1))
         .orderBy(F.desc('porcentaje')))
exced.show(40, truncate=False)

e = exced.toPandas().pivot(index='estacion', columns='pm', values='porcentaje').fillna(0)
e.plot.barh(figsize=(9, 6))
plt.xlabel('% de días que superan el límite diario')
plt.title('Días sobre el nivel máximo permisible, por estación')
plt.tight_layout()
plt.show()

# %%
conteo = (df.rdd
          .map(lambda fila: (fila['estacion'], 1))   # transformación: (estación, 1)
          .reduceByKey(lambda a, b: a + b))          # transformación: suma por estación

print(conteo.takeOrdered(5, key=lambda x: -x[1]))    # acción: las 5 estaciones con más mediciones


# ======================================================================
# Qué capturar para el informe
# ======================================================================
