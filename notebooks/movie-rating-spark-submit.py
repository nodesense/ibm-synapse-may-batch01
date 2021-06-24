# Spark submit job 

# Databricks notebook source

import pyspark

from pyspark.sql import SparkSession

# create Spark session with necessary configuration    
spark = (SparkSession
    .builder
    .appName("App2")
    .config("spark.network.timeout", "600s")
    .config("spark.executor.heartbeatInterval", "10s")
    .getOrCreate());


from pyspark.sql.types import StructType, LongType, StringType, IntegerType, DoubleType

# create new schema, add columns and datatypes, the last column is nullable or not..
movieSchema = StructType()\
         .add("movieId", IntegerType(), True)\
         .add("title", StringType(), True)\
         .add("genres", StringType(), True)\

movieDf = spark.read.format("csv")\
               .option("header", True)\
               .schema(movieSchema)\
               .load('abfss://movieset@ibmgksynapse01storage.dfs.core.windows.net/movies')

movieDf.printSchema() 

# COMMAND ----------

# ratingSchema and load ratings.csv
ratingSchema = StructType()\
         .add("userId", IntegerType(), True)\
         .add("movieId", IntegerType(), True)\
         .add("rating", DoubleType(), True)\
         .add("timestamp", StringType(), True)


ratingDf = spark.read.format("csv")\
               .option("header", True)\
               .schema(ratingSchema)\
               .load('abfss://movieset@ibmgksynapse01storage.dfs.core.windows.net/ratings')

ratingDf.printSchema()

# COMMAND ----------

import pyspark.sql.functions as F

popularMoviesDf = ratingDf.groupBy("movieId")\
                .agg ( F.count("userId"), F.avg("rating").alias("avg_rating") )\
                .withColumnRenamed("count(userId)", "total_users")\
                .filter (" total_users >= 100 AND avg_rating >= 3.0 ")\
                .sort(F.desc("total_users"))
                

popularMoviesDf.printSchema()
display(popularMoviesDf)

# COMMAND ----------

finalPopularMoviesWithTitlesDf = popularMoviesDf.join (movieDf, "movieId")\
                                                .drop("genres")
finalPopularMoviesWithTitlesDf.printSchema()
display(finalPopularMoviesWithTitlesDf)
 
# COMMAND ----------



finalPopularMoviesWithTitlesDf.coalesce(1)\
      .write\
      .option("header","true")\
      .option("sep",",")\
      .mode("overwrite")\
      .csv("abfss://movieset@ibmgksynapse01storage.dfs.core.windows.net/popular-movies-synapse-spark")

popularMoviesFromLateDf = spark.read.format("csv")\
               .option("header", True)\
               .option("inferSchema", True)\
               .load('abfss://movieset@ibmgksynapse01storage.dfs.core.windows.net/popular-movies-synapse-spark')

popularMoviesFromLateDf.printSchema()

popularMoviesFromLateDf.show(100)
