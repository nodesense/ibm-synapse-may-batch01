# Databricks notebook source
data = [1,2,3,4,5,6,7,8,9]
rdd = sc.parallelize(data)
sc.setLogLevel("WARN") # suppress INFO, DEBUG messages

# COMMAND ----------

def filterFunc(n):
  print ("**Filter called ", n) # runs on worker,not shown on notebook
  return n % 2 == 0 # True or False

# rmemeber, filterFunc or lambda in previous example, are executed inside worker
rdd2 = rdd.filter(filterFunc)

# COMMAND ----------

def mulBy10Func(n):
  print ("--Mul called", n) # runs on worker,not shown on notebook
  return n * 10

rdd3 = rdd2.map(mulBy10Func)

# COMMAND ----------

output = rdd3.collect()
# runs on driver, or application, shown on notebook
print (output)

# COMMAND ----------

print (rdd3.min())  # does all the transformation again from begining

# COMMAND ----------

print (rdd3.min())  # does all the transformation again from begining

# COMMAND ----------


