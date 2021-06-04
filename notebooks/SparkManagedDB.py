# Databricks notebook source
# Spark has Databases
# DataBase = MetaData + Data
# MetaData = (MetaData/DDL/DB,Tables, SCHEMA, COLUMN, DATA TYPES, LOCATION OF DATA), NOT DATA
# DATA is located elsewhere could be datalake/azure, HDFS, ....

# 3 ways, spark manage tables

# temp view/temp table - discussed
# Spark managed table - here example
# spark external tables


# COMMAND ----------

# Spark managed table 
# TABLE MetaData + Data is managed by spark
# Data Manipulation - should be done through Spark API using DF, SQL
# CREATE A TABLE, spark create directories for the table
# while we insert data, insert to directory location

# COMMAND ----------

spark.sql("SHOW DATABASES").show()

# COMMAND ----------

# create database
# permanent DB in side DataBricks
spark.sql("CREATE DATABASE orderdb")

# COMMAND ----------

spark.sql("SHOW DATABASES").show()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- sql comments
# MAGIC -- %sql magic function
# MAGIC -- code here should be sql, this code shall be run inside spark.sql(..) and output is displayed using display
# MAGIC CREATE TABLE orderdb.orders(id INT, amount INT)

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN ORDERDB

# COMMAND ----------

# MAGIC %sql
# MAGIC -- without database prefix
# MAGIC -- option 1: default db, /user/hive/warehouse/products ***
# MAGIC -- option 2, USE DATABASE
# MAGIC 
# MAGIC CREATE TABLE products(id INT, name STRING);

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO products VALUES(1, 'iphone')

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO orderdb.orders VALUES (1,100), (2,200), (3,300)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from orderdb.orders

# COMMAND ----------


