```
sales.csv

id,amount,date
  1,345.34,12-12-2020  
2,345.34,12-12-2020
3,  345.34  ,  12-12-2020

---

RDD / Spark cannot optimize - Resilient Distributed Dataset

.textText("/sales.csv")\
.map (line => line.strip())
.map (line => line.split(","))
.map (array => {
    arr2 = []
    for each word in array:
       word = word.strip()
        arr2.append(word
        
    return arr2
    )
})
.map (array => (int(array[0]), float(array[1], datetime(array(2)))) )
.filter (t => t[1] > 10000) // separately
.filter (t => t[2] > "01-01-2021") // separately





spark.read.csv(filename)
   .schema({id: int, amount: double, date: Datetime})
   .filter ( "amount > 10000" ) /SQL
   .filter ( col("amount") > 10000) / python
   .filter ( col("date") > "01-01-2021") / python


Spark will join all fitler into single filter 

 amount > 10000 AND date > 01-01-2021
 
 ```
