```sql

-- https://github.com/nodesense/deloitte-azure-db-synapse-may-2021/blob/main/serverless-sql/json-customer-line-delimited.json

USE db1;


-- show cols doc with all json content as single column

select top 10 *
from openrowset(
        bulk 'https://ibmgksynapse01storage.blob.core.windows.net/adventureworks/customers/json-customer-line-delimited.json',
        format = 'csv',
        fieldterminator ='0x0b',
        fieldquote = '0x0b'
    ) with (doc nvarchar(max)) as rows
----



 select
   CAST(JSON_VALUE(doc, '$.id') AS INT) as id,
    JSON_VALUE(doc, '$.email') AS email,
  
    doc
from openrowset(
        bulk 'https://ibmgksynapse01storage.blob.core.windows.net/adventureworks/customers/json-customer-line-delimited.json',
        format = 'csv',
        fieldterminator ='0x0b',
        fieldquote = '0x0b'
    ) with (doc nvarchar(max)) as rows
order by JSON_VALUE(doc, '$.id') desc

-----

select
   CAST(JSON_VALUE(doc, '$.id') AS INT) as id,
    JSON_VALUE(doc, '$.email') AS email,
   JSON_VALUE(doc, '$.first') AS first,
    JSON_VALUE(doc, '$.last') AS last,
    JSON_VALUE(doc, '$.company') AS company, 
    JSON_VALUE(doc, '$.created_at') AS created_at, 
    JSON_VALUE(doc, '$.country') AS country
    
from openrowset(
        bulk 'https://ibmgksynapse01storage.blob.core.windows.net/adventureworks/customers/',
        format = 'csv',
        fieldterminator ='0x0b',
        fieldquote = '0x0b'
    ) with (doc nvarchar(max)) as rows
order by JSON_VALUE(doc, '$.id') desc


------------------------

-- we can create view for row set


CREATE VIEW customers_view AS
    select
    CAST(JSON_VALUE(doc, '$.id') AS INT) as id,
        JSON_VALUE(doc, '$.email') AS email,
    JSON_VALUE(doc, '$.first') AS first,
        JSON_VALUE(doc, '$.last') AS last,
        JSON_VALUE(doc, '$.company') AS company, 
        JSON_VALUE(doc, '$.created_at') AS created_at, 
        JSON_VALUE(doc, '$.country') AS country
        
    from openrowset(
            bulk 'https://ibmgksynapse01storage.blob.core.windows.net/adventureworks/customers/',
            format = 'csv',
            fieldterminator ='0x0b',
            fieldquote = '0x0b'
        ) with (doc nvarchar(max)) as rows


SELECT * from customers_view;

SELECT * from customers_view order by id desc;

```
