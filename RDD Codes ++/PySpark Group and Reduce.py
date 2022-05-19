# Databricks notebook source
from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName("Read File")
sc = SparkContext.getOrCreate(conf=conf)

# COMMAND ----------

rdd = sc.textFile('/FileStore/tables/sample4.txt')
rdd.collect()

# COMMAND ----------

rdd2 = rdd.flatMap(lambda x: x.split(' ')).filter(lambda x: x!='')
rdd2.collect()

# COMMAND ----------

rdd3 = rdd2.map(lambda x: (x,1)).reduceByKey(lambda x,y: x+y)
rdd3.collect()
