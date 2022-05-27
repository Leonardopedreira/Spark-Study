# Databricks notebook source
### Bibliotecas de importação
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, udf
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
spark = SparkSession.builder.appName("Spark DataFrames").getOrCreate()

# COMMAND ----------

### Visualização do DataFrame
df = spark.read.options(header = 'True', inferSchema = 'True').csv('/FileStore/tables/OfficeData.csv')
df.show()
df.printSchema()

# COMMAND ----------

### Função + UDF para resolução: visualizar um 'increment' aos funcionários de cada estado
def new_salary(salary, bonus, state):
    if state == 'NY':
        return 0.1*salary + 0.05*bonus
    else:
        return 0.12*salary + 0.03*bonus
    
totalsalaryUDF = udf(lambda x, y, z: new_salary(x,y,z), DoubleType())

df.withColumn("increment", totalsalaryUDF(df.salary,df.bonus,df.state)).show() # Precisa seguir a ordem correta para funcionar
