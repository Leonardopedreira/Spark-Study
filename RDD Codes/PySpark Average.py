# Databricks notebook source
from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName("Average")
sc = SparkContext.getOrCreate(conf=conf)

# COMMAND ----------

rdd = sc.textFile('/FileStore/tables/sample5.txt')
rdd.collect()

# COMMAND ----------

rdd2 = rdd.map(lambda x: (x.split(',')[0], (float(x.split(',')[2]),1)))
rdd3 = rdd2.reduceByKey(lambda x,y: (x[0]+y[0],x[1]+y[1]))
rdd4 = rdd3.map(lambda x: (x[0],x[1][0]/x[1][1]))
rdd4.collect()
