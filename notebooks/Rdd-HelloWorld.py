# Databricks notebook source
# notebook, while you run this notebook, this is spark application
# Every Spark app contains a Spark Driver
# Spark Driver contains Spark Context
print("Hello Spark")
print(sc)
# sc - spark context

# COMMAND ----------

data = [1,2,3,4,5,6,7,8,9]
# load the hardcoded/external data into spark RDD
# we create rdd on spark context
# parallelize is for hardcoded data, small examples
# its load the data and distribute the data into paritions
# paritions means subset/chunk of data

rdd = sc.parallelize(data) # lazy
# when executed this statement, no data loaded, no parition created, no cluster assigned, no worker used, no tasks, no stage 

# COMMAND ----------

# transformation, lazy, below statement doesn't execute
# higher order function, a function that accept another function
# when the code is running, lambda is called for each number in the data [1,2,3,4,5,6,7,8,9]
# code we have lambda n: n % 2 == 0 is shipped to worker node
# filter - predicate.. returns True or False
rdd2 = rdd.filter (lambda n: n % 2 == 0) # Filter picks only True return values,  pick only even numbers [2, 4,6,8]

# COMMAND ----------

# derive from rdd2, only even numbers, multiply by 10 each, applied on [2,4,6,8]
# output of rdd2 is passed as input to rdd3, map function
# map returns the output of the expression, can be anything, number, list, tuple, map.. etc
rdd3 = rdd2.map (lambda n: n * 10) # transform method, lazy code [20, 40, 60, 80]

# COMMAND ----------

# this prints RDD lineage
rdd3.toDebugString()# 

# COMMAND ----------

# now let us try an action method..
# collect data from the rdd
# action method, this will create job, convert rdd into DAG, then DAG shall be split and planned as stages, then tasks is created, and task is performed by workers, then finally the result is collected by the driver
# output is python list, not RDD
# as many action methods shall return non RDDs
output = rdd3.collect() 
print(output, type(output))

# COMMAND ----------

# create job, dag, stages, jobs, partitions
print (rdd3.min()) # another action, it triggers all from begining loading data, performing filter, map etc
print (rdd3.max()) # every action, it creates a job

# COMMAND ----------

sc.stop() # stopping spark application

# COMMAND ----------


