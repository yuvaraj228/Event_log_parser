--sqlite3 table DDLs

CREATE TABLE IF NOT EXISTS ASSIGNMENT_POC (year INTEGER PRIMARY KEY ASC, season TEXT,countries_with_medals INTEGER,ins_upd_ts TEXT);

CREATE TEMP TABLE IF NOT EXISTS TEST_POC (year INTEGER PRIMARY KEY ASC, season TEXT,countries_with_medals INTEGER,ins_upd_ts TEXT);

--Glue/External table DDL
CREATE EXTERNAL TABLE IF NOT EXISTS TEST_POC (year INT, season STRING,countries_with_medals INT,ins_upd_ts STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE
location 's3://BUCKET-NAME/BUCKET-PREFIX'
TBLPROPERTIES('serialization.null.format' = '', 'skip.header.line.count' = '1');
--we can also declare ins_upd_ts as timestamp

--Redshift TABLE DDL
CREATE TABLE ASSIGNMENT_POC(
year INT not null, season varchar,countries_with_medals INT,ins_upd_ts timestamp)