```

CREATE DATABASE db1;

USE db1;

-- Only external table
-- for exploratin, we can use openrowset
-- Views are supported

-- No tables
-- No material views
-- No indexes


-- This is auto-generated code

SELECT
    TOP 100 *
FROM
    OPENROWSET(
        BULK 'https://ibmgksynapse01storage.dfs.core.windows.net/adventureworks/bom/Production_BillOfMaterials.csv',
        FORMAT = 'CSV',
        PARSER_VERSION='2.0',
        HEADER_ROW = TRUE
    ) AS [result]


CREATE EXTERNAL FILE FORMAT [ExternalCSVWithHeaderWithDblQuote] 
                WITH (
                    FORMAT_TYPE = DELIMITEDTEXT,
                    FORMAT_OPTIONS (FIELD_TERMINATOR=',' , 
                                    FIRST_ROW=2, 
                                    STRING_DELIMITER = '0x22' 
                                    )
                );


SELECT * from sys.external_file_formats;

SELECT * from sys.external_data_sources;

  
CREATE EXTERNAL DATA SOURCE [advworks_ds]
  WITH (LOCATION='abfss://adventureworks@ibmgksynapse01storage.dfs.core.windows.net')
 

SELECT * from sys.external_data_sources;
 
-- DROP EXTERNAL TABLE Production_BillOfMaterials;

CREATE EXTERNAL TABLE Production_BillOfMaterials (
    BillOfMaterialsID INT,
    ProductAssemblyID INT,
    ComponentID INT,
    StartDate DATETIME,
    EndDate DATETIME,
    UnitMeasureCode VARCHAR(100),
    BOMLevel INT,
    PerAssemblyQty FLOAT,
    ModifiedDate DATETIME
) WITH (
    -- location/path within data source, point to directory, not a specific file
    LOCATION='/bom',
    -- data source has container name, storage account
    DATA_SOURCE = [advworks_ds],
    FILE_FORMAT = [ExternalCSVWithHeaderWithDblQuote]
);

SELECT COUNT(*) from Production_BillOfMaterials;

SELECT * from Production_BillOfMaterials;

```
