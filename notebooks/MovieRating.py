# Databricks notebook source

from pyspark.sql.types import StructType, LongType, StringType, IntegerType, DoubleType

# create new schema, add columns and datatypes, the last column is nullable or not..
movieSchema = StructType()\
         .add("movieId", IntegerType(), True)\
         .add("title", StringType(), True)\
         .add("genres", StringType(), True)\

movieDf = spark.read.format("csv")\
               .option("header", True)\
               .schema(movieSchema)\
               .load('/mnt/movielens/movies/movies.csv')

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
               .load('/mnt/movielens/ratings/ratings.csv')

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
      .csv("/mnt/movielens/popular-movies")

# check blob storage, for directory popular-movies, check content..

# COMMAND ----------

popularMoviesFromLateDf = spark.read.format("csv")\
               .option("header", True)\
               .option("inferSchema", True)\
               .load('/mnt/movielens/popular-movies')

popularMoviesFromLateDf.printSchema()

popularMoviesFromLateDf.show(100)
