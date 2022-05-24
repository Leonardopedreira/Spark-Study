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

### Criação da nova coluna 'Total Marks'
df = df.withColumn('Total Marks', lit(120))
df.show()

# COMMAND ----------

### Criação da nova coluna 'Average Marks'
df = df.withColumn('Average Marks', (col('marks')/col('Total Marks')) * 100)
df.show()

# COMMAND ----------

### Visualização dos estudantes de cada curso + notas atribuídas. Basta alterar os valores.
df2 = df.filter( (df.course == 'Cloud')  & (df.marks > 60) )
df2.show()

# COMMAND ----------

### Print do nome dos estudantes + notas
df.select('name', 'marks').show()
