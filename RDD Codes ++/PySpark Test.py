# Databricks notebook source
from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName("Read File")
sc = SparkContext.getOrCreate(conf=conf)

# COMMAND ----------

rdd = sc.textFile('/FileStore/tables/sample2.txt')
rdd.collect()

# COMMAND ----------

rdd2 = rdd.map(lambda x: x.split(' '))
rdd3 = rdd2.map(lambda x: len(x))

# COMMAND ----------

def tam(x):
    lista = []
    for i in x:
        lista.append(len(i))
    return lista

# COMMAND ----------

rdd3 = rdd2.map(tam)
rdd3.collect()

# COMMAND ----------

# Solução em duas linhas de código
rdd4 = rdd.map(lambda x: [len(i) for i in x.split(' ')])
rdd4.collect()
