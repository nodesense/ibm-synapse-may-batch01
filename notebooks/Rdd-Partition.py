# Databricks notebook source
print ("minpart", sc.defaultMinPartitions) 
print ("parallism", sc.defaultParallelism) # default.parallelism

# COMMAND ----------

# default partition
data = [1,2,3,4,5,6,7,8,9]
data = range(1, 20) # override data with range
rdd= sc.parallelize(data) # picks the recommendation from sc.defaultParallelism

print (rdd.getNumPartitions()) # prints number of paritions

# will have performance issue, need more more memory driver
print (rdd.collect() ) # action, collect data from all parititions

print (rdd.take(2)) # picks top 2 elements from first parition, better to understand the data

# glom() to pick data from each paritions
partitionData = rdd.glom().collect()

for p in partitionData:
  print ("parition ", p)

# COMMAND ----------

# limit partition at source
data = range(1, 20) # override data with range
rdd= sc.parallelize(data, 4)  # set desired parition in arg

print ("getNumPartitions", rdd.getNumPartitions()) # prints number of paritions

# will have performance issue, need more more memory driver
print (rdd.collect() ) # action, collect data from all parititions

print (rdd.take(2)) # picks top 2 elements from first paritions, better to understand the data

# glom() to pick data from each paritions
partitionData = rdd.glom().collect()

for p in partitionData:
  print ("parition ", p)

# COMMAND ----------

# limit partition at source
data = range(1, 100) # override data with range
rdd= sc.parallelize(data, 2)  # set desired parition in arg

print ("getNumPartitions", rdd.getNumPartitions()) # prints number of paritions

# will have performance issue, need more more memory driver
print (rdd.collect() ) # action, collect data from all parititions

print (rdd.take(2)) # picks top 2 elements from first paritions, better to understand the data

# glom() to pick data from each paritions
partitionData = rdd.glom().collect()

for p in partitionData:
  print ("parition ", p)
  
print ("-" * 50) 
# reparition will help you create partitions programatically based on size, redistribute the elements/shuffling

# try best to equally spilit/distribute the partitions
# use repartitions to increase the partitions
rdd3 = rdd.repartition(8) # TODO: Dynamic..


print ("getNumPartitions", rdd3.getNumPartitions()) # prints number of paritions

# will have performance issue, need more more memory driver
print (rdd3.collect() ) # action, collect data from all parititions

print (rdd3.take(2)) # picks top 2 elements from first paritions, better to understand the data

# glom() to pick data from each paritions
partitionData = rdd3.glom().collect()

for p in partitionData:
  print ("parition ", p)
  

# COMMAND ----------



# limit partition at source
data = range(1, 100) # override data with range
rdd= sc.parallelize(data, 16)  # set desired parition in arg

print ("getNumPartitions", rdd.getNumPartitions()) # prints number of paritions

# will have performance issue, need more more memory driver
print (rdd.collect() ) # action, collect data from all parititions

print (rdd.take(2)) # picks top 2 elements from first paritions, better to understand the data

# coalesce , partition method, typically used to reduce the paritions
# while writing output to file system

rdd3 = rdd.coalesce(2)  


print ("getNumPartitions", rdd3.getNumPartitions()) # prints number of paritions

# will have performance issue, need more more memory driver
print (rdd3.collect() ) # action, collect data from all parititions

print (rdd3.take(2)) # picks top 2 elements from first paritions, better to understand the data

# glom() to pick data from each paritions
partitionData = rdd3.glom().collect()

for p in partitionData:
  print ("parition ", p)

# COMMAND ----------


