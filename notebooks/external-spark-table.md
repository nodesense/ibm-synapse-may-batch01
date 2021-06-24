CREATE TABLE orderdb.orders (id int, amount int) USING PARQUET OPTIONS(PATH='abfss://movieset@ibmgksynapse01storage.dfs.core.windows.net/spark-ext-orders')


INSERT INTO orderdb.orders VALUES (100, 1000)

Try query the same from serverless pool
