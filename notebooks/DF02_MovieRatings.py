# Databricks notebook source
# load csv into data frame
# inferschema to build schema automatically
# create schema yourself
# manipulation with apis
# analytics apis count, avg, groupby
# join movies, rating

# COMMAND ----------

# load data, without schema, spark will build schema based on csv header, all column marked as string type
# by default spark will not scan the content/data
movieDf = spark.read.format("csv")\
               .option("header", True)\
               .load('/FileStore/tables/movies.csv')

movieDf.printSchema() # all columns marked as string

# COMMAND ----------

# inferSchema, spark scan the file content, whole content, find the data types for columns
# not advised to use inferSchema for large files.
movieDf = spark.read.format("csv")\
               .option("header", True)\
               .option("inferSchema", True)\
               .load('/FileStore/tables/movies.csv')

movieDf.printSchema() 

# COMMAND ----------

# define the schema and use that schema
from pyspark.sql.types import StructType, LongType, StringType, IntegerType, DoubleType

# create new schema, add columns and datatypes, the last column is nullable or not..
movieSchema = StructType()\
         .add("movieId", IntegerType(), True)\
         .add("title", StringType(), True)\
         .add("genres", StringType(), True)\

movieDf = spark.read.format("csv")\
               .option("header", True)\
               .schema(movieSchema)\
               .load('/FileStore/tables/movies.csv')

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
               .load('/FileStore/tables/ratings.csv')

ratingDf.printSchema()

# COMMAND ----------

movieDf.show(2)
ratingDf.show(2)

# COMMAND ----------

# aggregate count
import pyspark.sql.functions as F
# get the count of the users who voted for a movie
# SELECT movieId, count(userId) from ratings GROUP BY movieId
aggDf = ratingDf.groupBy("movieId")\
                .agg ( F.count("userId") )

aggDf.printSchema()
aggDf.show(3)

# COMMAND ----------

aggDf = ratingDf.groupBy("movieId")\
                .agg ( F.count("userId"), F.avg("rating").alias("avg_rating") )\
                .withColumnRenamed("count(userId)", "total_users")\
                

aggDf.printSchema()
aggDf.show(3)

# COMMAND ----------

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


