# Databricks notebook source
### Bibliotecas de importação
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
spark = SparkSession.builder.appName("Spark DataFrames").getOrCreate()

# COMMAND ----------

### Visualização do DataFrame
df = spark.read.options(header = 'True', inferSchema = 'True').csv('/FileStore/tables/StudentData.csv')
df.show()
df.printSchema()

# COMMAND ----------

### Distinguindo as colunas
#df.select("age", "gender", "course").distinct().count() #Duas soluções possíveis
df.dropDuplicates(['age','gender','course']).count()

# COMMAND ----------

### Visualizar total de estudantes por curso
df.groupBy("course").count().show()

# COMMAND ----------

### Visualizar total de estudantes homens e mulheres por curso
df1 = df.groupBy("course", "gender").count()
df1.orderBy(df1.course.asc()).show()

# COMMAND ----------

### Visualizar o total de notas conseguidas por gênero por curso
df1 = df.groupBy("course", "gender", "marks").count()
df1.orderBy(df1.course.desc()).show()

# COMMAND ----------

### Visualizar o max, min e avg de notas conseguidas por idade por curso
from pyspark.sql.functions import max, min, avg, count

df.groupBy("age", "course").agg(max("marks"), min("marks"), avg("marks")).show()

# COMMAND ----------

### Importação de outro dataset:
df2 = spark.read.text('/FileStore/tables/WordData.txt') #A dica é: não é possível fazer sem que a coluna tenha um título, então, adicione leitura como "text" para ter o título "value"
df2.show()
df2.printSchema()

# COMMAND ----------

df2.groupBy("value").count().show()
