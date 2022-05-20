# Databricks notebook source
from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName("MinAndMax")
sc = SparkContext.getOrCreate(conf=conf)

# COMMAND ----------

rdd = sc.textFile('/FileStore/tables/sample5.txt')
rdd.collect()

# COMMAND ----------

rdd2 = rdd.map(lambda x: (x.split(',')[1], (float(x.split(',')[2]))))
rdd2.collect()

# COMMAND ----------

rdd3 = rdd2.reduceByKey(lambda x,y: x if x>y else y)
rdd3.collect()
