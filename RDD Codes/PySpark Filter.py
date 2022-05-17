# Databricks notebook source
from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName("Read File")
sc = SparkContext.getOrCreate(conf=conf)

# COMMAND ----------

rdd = sc.textFile('/FileStore/tables/sample3.txt')
rdd.collect()

# COMMAND ----------

rdd2 = rdd.flatMap(lambda x: x.split(' '))
rdd2.collect()

# COMMAND ----------

# Resultado com 2 linhas de código
rdd3 = rdd2.filter(lambda x: not (x.startswith("a") or x.startswith("c")))
rdd3.collect()

# COMMAND ----------

# Definindo por função
def words(x):
    for i in x:
        if x.startswith("a") or x.startswith("c"):
            return False
        else:
            return True

rdd3 = rdd2.filter(words)
rdd3.collect()
