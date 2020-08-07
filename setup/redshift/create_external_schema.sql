-- this is the schema config script
create external schema spectrum
from data catalog
database 'spectrumdb'
iam_role '''<redacted>'''
create external database if not exists;

-- dota heroes staging table partitioned on the insertion date
drop table if exists spectrum.dota_heroes_staging;
create external table spectrum.dota_heroes_staging (
    ID int,
    NonLocalizedName varchar(50),
    LocalizedName varchar(100),
    PrimaryAttr varchar(10),
    AttackType varchar(10),
    Roles varchar(100),
    QueryDate varchar(20)
)
partitioned by (insert_date date)
row format delimited fields terminated by ','
stored as textfile
location 's3://airflow-project-test/dota_heroes/stage/'
table properties ('skip.header.line.count'='1');

-- dota draft staging table
drop table if exists spectrum.dota_draft_clean_stage;
CREATE EXTERNAL TABLE spectrum.dota_draft_clean_stage (
   match_id varchar(20),
   radiant_win int,
   query_date varchar(20),
   pick_1 int,
   pick_2 int,
   pick_3 int,
   pick_4 int,
   pick_5 int,
   pick_6 int,
   pick_7 int,
   pick_8 int,
   pick_9 int,
   pick_10 int,
   search_col varchar(100),
)
STORED AS PARQUET
LOCATION 's3://airflow-project-test/dotadraft/stage/';

-- creating the report table schema
DROP TABLE IF EXISTS public.dota_draft_report;
CREATE TABLE public.dota_draft_report (
    match_id varchar(20),
   radiant_win int,
   insert_date date,
   pick_1 varchar(50),
   pick_2 varchar(50),
   pick_3 varchar(50),
   pick_4 varchar(50),
   pick_5 varchar(50),
   pick_6 varchar(50),
   pick_7 varchar(50),
   pick_8 varchar(50),
   pick_9 varchar(50),
   pick_10 varchar(50)
);