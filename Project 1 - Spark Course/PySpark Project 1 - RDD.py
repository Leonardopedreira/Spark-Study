# Databricks notebook source
### Importação do SparkContext
from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName("Project")
sc = SparkContext.getOrCreate(conf=conf)

# COMMAND ----------

### Transformação dos dados utilizados em um RDD
rdd = sc.textFile('/FileStore/tables/StudentData.csv')
rdd.collect()

# COMMAND ----------

# OBS: é possível usar um código pequeno para eliminar o cabeçalho: headers = rdd.first()

### Visualizando número de estudantes ou usuários
rdd2 = rdd.map(lambda x: x.split(','))
rdd2.map(lambda x: x[0]).count() - 1 # Retirando o cabeçalho

# COMMAND ----------

### Visualizar o total de estudantes homens e mulheres
rdd2.filter(lambda x: x[1] != 'Female').count() - 1

# COMMAND ----------

### Visualizar número de estudantes aprovados e reprovados (nota 50+)
rdd2.filter(lambda x: x[5] >= '50').count() - 1

# COMMAND ----------

### Visualizar número de estudantes matriculados por curso
rdd2.map(lambda x: x[3]).countByValue()

# COMMAND ----------

### Visualizar a quantidade de notas que os estudantes tiraram por curso
rdd2.map(lambda x: (x[3], x[5] >= '50')).countByValue()

# COMMAND ----------

### Visualizar a média das notas que os estudantes obtiveram por curso
rdd_drop = rdd2.mapPartitionsWithIndex(lambda id_x, iter: list(iter)[1:] if(id_x == 0) else iter) # Opção para remover cabeçalho
rdd3 = rdd_drop.map(lambda x: (x[3], (int(x[5]),1))).reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1]))
rdd4 = rdd3.map(lambda x: (x[0], x[1][0]/x[1][1]))
rdd4.collect()

# COMMAND ----------

### Visualizar valores minimos e máximos entre as notas dos estudantes por curso
rdd_drop = rdd2.mapPartitionsWithIndex(lambda id_x, iter: list(iter)[1:] if(id_x == 0) else iter) # Opção para remover cabeçalho
rdd_drop.map(lambda x: (x[3], x[5])).reduceByKey(lambda x, y: x if x<y else y).collect() # Basta alterar o sinal de '>' ou '<'

# COMMAND ----------

rdd2.collect()

# COMMAND ----------

### Visualizar a média das idades entre estudantes homens e mulheres
rdd_drop = rdd2.mapPartitionsWithIndex(lambda id_x, iter: list(iter)[1:] if(id_x == 0) else iter) # Opção para remover cabeçalho
rdd3 = rdd_drop.map(lambda x: (x[1], (int(x[0]),1))).reduceByKey(lambda x,y: (x[0]+y[0],x[1]+y[1]))
rdd3.map(lambda x: (x[0], x[1][0]/x[1][1])).collect()
