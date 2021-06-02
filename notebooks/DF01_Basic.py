# Databricks notebook source
# sparksession, entry point for data frame/sql

print (spark) # spark session

# COMMAND ----------

# Python list of python tuples
products = [ 
          # (product_id, product_name, brand_id)  
         (1, 'iPhone', 100),
         (2, 'Galaxy', 200),
         (3, 'Redme', 300), # orphan record, no matching brand
         (4, 'Pixel', 400),
]

# we have data, schema separately,we need put them  together as dataframe
productDf = spark.createDataFrame(data = products, schema= ['product_id', 'product_name', 'brand_id'])


# COMMAND ----------

# print the dataframe schema
productDf.printSchema()

# COMMAND ----------

# print the data in the dataframe
productDf.show() # first 20 results

# COMMAND ----------

productDf.show(2) # limit the records

# COMMAND ----------

productDf.show(truncate=False) # to show all records

# COMMAND ----------

# DataBricks feature, it products display function
display(productDf)

# COMMAND ----------

# Every data frame has RDD internally
# rdd has partitions
# dataframe RDDs are made as rows, eachrow has column

print(productDf.rdd.collect())

# COMMAND ----------

# rdd paritions
print (productDf.rdd.getNumPartitions())

# COMMAND ----------

# DataFrame APIs using Python
# DataFrame also immutable structure, we cannot update, delete, insert records/rows once created
# any manipulation will create new DataFrame
# creates new dataframe
df1 = productDf.select("product_id", "product_name")

df1.printSchema()
df1.show()

# COMMAND ----------

# applying functions to DF
from pyspark.sql.functions import col, upper

# to derive new columns, rename after an expresion
df1 = productDf.withColumn("name_caps", upper(col("product_name")))
 
df1.printSchema()
df1.show()

# COMMAND ----------

# instead of importing individual functions, 150+ functions 
import pyspark.sql.functions as F
# use F. prefix for all functions, act like alias or namespace
# Python DF APIs
df1 = productDf.withColumn("name_caps", F.upper(F.col("product_name")))

df1.printSchema()
df1.show()

# COMMAND ----------

# create temp view/temp table out of dataframe
# a temp view created, reside only in memory, avaialble as long as we have spark session,. no db linked
spark.sql("SHOW TABLES").show()
productDf.createOrReplaceTempView("products")
spark.sql("SHOW TABLES").show()

# COMMAND ----------

df1 = spark.sql ("select * from products") # returns a DataFrame
df1.printSchema()
df1.show()

# COMMAND ----------

df1 = spark.sql ("select product_id, upper(product_name) from products") # returns a DataFrame
df1.printSchema()
df1.show()

# COMMAND ----------

# pyspark API Filter
# filter & where are same, alias
productDf.filter ( F.col("brand_id") >= 200 ).show()
# filter with AND
productDf.filter (  (F.col("brand_id") >= 200) & (F.col("brand_id") < 400)  ).show()

# with some sql flavor
productDf.filter (" brand_id >= 200 AND brand_id < 400 ").show()

# SQL
spark.sql("SELECT * FROM products WHERE  brand_id >= 200 AND brand_id < 400").show()

# COMMAND ----------


