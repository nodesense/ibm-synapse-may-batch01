```sql

-- select db on top right 
-- use statement

USE db1;

-- visit
-- https://github.com/nodesense/deloitte-azure-db-synapse-may-2021/blob/main/serverless-sql/part-00000-2cb993fc-1383-4c09-a452-4749657406ce-c000.snappy.parquet 
-- click on download button

-- movieset container attached to synapse, create a folder/directory called popular-movies
-- upload the downloaded parquet file..


CREATE EXTERNAL DATA SOURCE   popular_movies_ds
WITH ( LOCATION = 'abfss://movieset@ibmgksynapse01storage.dfs.core.windows.net' );

CREATE EXTERNAL FILE FORMAT PARQUET_FORMAT
WITH
(  
    FORMAT_TYPE = PARQUET,
    DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
)
 
/*
the file consistes of below columns

movieId
total_users
avg_rating
title
*/
--   EXTERNAL TABLE  popular_movies_parquet ;
 
CREATE EXTERNAL TABLE popular_movies_parquet (
    movieId INT,
    total_users BIGINT,
    avg_rating FLOAT,
    title VARCHAR(100) COLLATE Latin1_General_100_BIN2_UTF8 
)  WITH (
    LOCATION = '/popular-movies',
    DATA_SOURCE = popular_movies_ds,  
    FILE_FORMAT = PARQUET_FORMAT
);

select * from popular_movies_parquet;


-- ONLY For example using OPENROWSET

SELECT
    TOP 100 *
FROM
    OPENROWSET(
        BULK 'https://ibmgksynapse01storage.dfs.core.windows.net/movieset/popular-movies/part-00000-2cb993fc-1383-4c09-a452-4749657406ce-c000.snappy.parquet',
        FORMAT='PARQUET'
    ) AS [result]

```
