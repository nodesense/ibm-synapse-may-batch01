# Databricks notebook source
inputRdd = sc.textFile("/FileStore/tables/words.txt")
print (inputRdd.collect())

# both trail and leading space shall be removed
strippedRdd = inputRdd.map (lambda line: line.strip())

print("stripped", strippedRdd.collect())

nonEmptyLinesRdd = strippedRdd.filter (lambda line: line != '')
print("nonEmptyLinesRdd", nonEmptyLinesRdd.collect())



# COMMAND ----------


# break the words
# list of list
arrayOfWordsRdd =  nonEmptyLinesRdd.map (lambda line: line.split(" "))
print("arrayOfWordsRdd", arrayOfWordsRdd.collect())

# COMMAND ----------

# convert list of list of words into word, flatten list
wordsRdd = arrayOfWordsRdd.flatMap(lambda wordsList: wordsList) # the output will be flattened array
print("wordsRdd", wordsRdd.collect())

# COMMAND ----------

# structure teh data into tuple with keyed rdd (word, 1), key is word, the value is occurance, no aggregation here
wordsPairRdd = wordsRdd.map (lambda word: (word, 1)) # return tuple
print ("wordsPairRdd", wordsPairRdd.collect())

# COMMAND ----------

# for each word, the count is applied, we will have final word count
wordsCount = wordsPairRdd.reduceByKey( lambda acc, occurance: acc + occurance)
print ("wordsCount", wordsCount.collect())

# COMMAND ----------


