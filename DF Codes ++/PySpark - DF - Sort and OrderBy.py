# Databricks notebook source
### Bibliotecas de importação
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
spark = SparkSession.builder.appName("Spark DataFrames").getOrCreate()

# COMMAND ----------

### Visualização do DataFrame
df = spark.read.options(header = 'True', inferSchema = 'True').csv('/FileStore/tables/OfficeData.csv')
df.show()
df.printSchema()

# COMMAND ----------

### Visualização da ordenação das idades + salário
df.sort(df.age.desc(), df.salary.asc()).show() #o salário ficará em ordem apenas se tiverem pessoas da mesma idade

# COMMAND ----------

### Visualização da ordenação das idades + bonus + salário
df.sort(df.age.desc(), df.bonus.desc(), df.salary.asc()).show()

# COMMAND ----------


