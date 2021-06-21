```sql

-- External tables

-- schema shall be maintained by the dedicated pool
-- the data stored in data lake externally

-- this allows join between external data (data lake data) and internal data warehouse data
-- PolyBase - ability for DW to query other DBs - Storage

-- how to integrate/query with data lake


-- dedicated pool cannot access formats, tables, from sql serverless
-- dedicated pool can have external tables of itself, can be linked to data lake, used for joins, aggregation..

CREATE EXTERNAL FILE FORMAT [ExternalCSVWithHeader] 
                WITH (
                    FORMAT_TYPE = DELIMITEDTEXT,
                    FORMAT_OPTIONS (FIELD_TERMINATOR=',', FIRST_ROW=2)
                )
 
 
CREATE EXTERNAL DATA SOURCE [movieset_ds]
  WITH (LOCATION='abfss://movieset@ibmgksynapse01storage.dfs.core.windows.net', 
        TYPE=HADOOP)


SELECT * from sys.external_data_sources;
SELECT * from sys.external_file_formats;

SELECT * from sys.external_tables;

-- links movieId,imdbId,tmdbId

CREATE EXTERNAL TABLE links (
    movieId INT,
    imdbId INT,
    tmdbId INT
) WITH (
    -- location/path within data source, point to directory, not a specific file
    LOCATION='links/',
    -- data source has container name, storage account
    DATA_SOURCE = [movieset_ds],
    FILE_FORMAT = [ExternalCSVWithHeader]
);

SELECT * from links;


 -- ratings: userId,movieId,rating,timestamp


CREATE EXTERNAL TABLE ratings (
    userId INT,
    movieId INT,
    rating FLOAT,
    timestamp BIGINT 
) WITH (
    -- location/path within data source, point to directory, not a specific file
    LOCATION='ratings/',
    -- data source has container name, storage account
    DATA_SOURCE = [movieset_ds],
    FILE_FORMAT = [ExternalCSVWithHeader]
);

SELECT * FROM ratings;


-- userId,movieId,tag,timestamp

CREATE EXTERNAL TABLE tags (
    userId INT,
    movieId INT,
    tag VARCHAR(256),
    timestamp BIGINT
) WITH (
    -- location/path within data source
    LOCATION='tags/tags.csv',
    -- data source has container name, storage account
    DATA_SOURCE = [movieset_ds],
    FILE_FORMAT = [ExternalCSVWithHeader]
);

SELECT * from tags;

-- create internal/managed table in dedicated pool

CREATE TABLE movies (
    id INT NOT NULL,
    title VARCHAR(500),
    genres VARCHAR(250)
) WITH (
    DISTRIBUTION = REPLICATE,
    CLUSTERED COLUMNSTORE INDEX
);

-- query from dedi table/Internal

SELECT TOP 10 id, title  from movies 

-- POLY BASE Query, multiple flavors, csv/parquet, orc
-- join internal and external tables..

-- https://storageacc.blob.core.windows.net/movieset/movies/movies.csv
-- copy all columns

COPY INTO movies 
    FROM 'https://ibmgksynapse01storage.blob.core.windows.net/movieset/movies/movies.csv'
WITH (
FILE_TYPE = 'CSV',
FIELDTERMINATOR = ',',
FIRSTROW=2
)

SELECT * from movies;



-- GROUP BY, HAVING, ORDER BY 
-- external table with in dedicated sql

SELECT movieId, COUNT(userId) AS total_voting, AVG(rating) AS avg_rating 
FROM ratings 
GROUP BY (movieId)
HAVING COUNT(userId) >= 100
ORDER BY total_voting DESC;


-- join external table from data lake with native dedi sql pool table

SELECT movieId, title, COUNT(userId) AS total_voting, AVG(rating) AS avg_rating 
FROM ratings 
JOIN movies 
ON movies.id = ratings.movieId
GROUP BY movieId, title
HAVING COUNT(userId) >= 100
ORDER BY total_voting DESC;

-- CTAS - Create Table As Select

-- we create a dedi pool table called popular_movies
-- from the query 

CREATE TABLE popular_movies 
WITH(
    DISTRIBUTION=REPLICATE, 
    CLUSTERED COLUMNSTORE INDEX
)
AS
SELECT movieId, title, COUNT(userId) AS total_voting, AVG(rating) AS avg_rating 
    FROM ratings 
    JOIN movies 
    ON movies.id = ratings.movieId
    GROUP BY movieId, title
    HAVING COUNT(userId) >= 100;

SELECT * from popular_movies;



```
