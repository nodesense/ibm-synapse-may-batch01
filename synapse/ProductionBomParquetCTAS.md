```sql
CREATE EXTERNAL FILE FORMAT PARQUET_FORMAT2
WITH
(  
    FORMAT_TYPE = PARQUET,
    DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
)


CREATE EXTERNAL DATA SOURCE   ProductionBomParquet
WITH ( LOCATION = 'abfss://adventureworks@ibmgksynapse01storage.dfs.core.windows.net' );


 

-- CREATE EXTERNAL TABLE AS
-- create parquet from external table/csv


CREATE EXTERNAL TABLE [production_bom_parquet] WITH (
    LOCATION = 'bom-parquet/',
    DATA_SOURCE = [ProductionBomParquet],
    FILE_FORMAT = [PARQUET_FORMAT2]
) AS 
SELECT * from Production_BillOfMaterials;

-- check your container location whether bom-parquet/ created or not

SELECT * FROM production_bom_parquet;

```
