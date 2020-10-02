--Create the STAGE Country table
create or replace TABLE COUNTRY_STG (
	COUNTRY_ABBV VARCHAR(5),
	COUNTRY_NAME VARCHAR(100),
	STAGE_REC_SRC VARCHAR(20),
	LOAD_UPDATE_DT DATE
);

-- Insert data into STAGE Country table
insert into country_stg values
('US','UNITED STATED OF AMERICA','ISO'),
('CA','CANADA','ISO'),
('BR','BRAZIL','ISO'),
('ZA','SOUTH AFRICA','ISO'),
('AU','AUSTRALIA','ISO'),
('SG','SINGAPORE','ISO'),
('JP','JAPAN','ISO'),
('KR','SOUTH KOREA','ISO'),
('CN','CHINA','ISO'),
('HK','HONG KONG','ISO'),
('IN','INDIA','ISO'),
('GB','UNITED KINGDOM','ISO'),
('IE','IRELAND','ISO'),
('FR','FRANCE','ISO'),
('ES','SPAIN','ISO'),
('IT','ITALY','ISO'),
('SE','SWEDEN','ISO');

--Create the HUB Country table
create or replace TABLE COETESTDB.DATAVAULT.HUB_COUNTRY (
	HUB_COUNTRY_KEY VARCHAR(32),
	COUNTRY_ABBV VARCHAR(5),
	HUB_LOAD_DT DATE,
	HUB_REC_SRC VARCHAR(10)
);

--Create the SAT Country table
create or replace TABLE COETESTDB.DATAVAULT.SAT_COUNTRY (
	HUB_COUNTRY_KEY VARCHAR(32),
	COUNTRY_NAME VARCHAR(100),
	HASH_DIFF VARCHAR(32),
	SAT_LOAD_DT DATE,
	SAT_REC_SRC VARCHAR(10)
);

--Multi Table Insert from COUNTRY stage into HUB COUNTRY and SAT COUNTRY
INSERT ALL  
WHEN (SELECT COUNT(*)     
             FROM coetestdb.datavault.hub_country hc        
             WHERE hc.hub_country_key = hash_key) = 0  
 THEN    
    INTO coetestdb.datavault.hub_country   (hub_country_key, country_abbv,hub_load_dt,hub_rec_src)                  
    VALUES (hash_key, country_abbv,load_dts, rec_src)  
WHEN (SELECT COUNT(*)         
          FROM coetestdb.datavault.sat_country sc        
          WHERE sc.hub_country_key = hash_key 
      AND sc.hash_diff = MD5(country_name)) = 0    
THEN    
INTO coetestdb.datavault.sat_country (hub_country_key, country_name,  
                                            hash_diff, sat_load_dt,sat_rec_src)                    
          VALUES (hash_key, country_name , hash_diff , load_dts,rec_src)  
                      
SELECT MD5(country_abbv) AS hash_key, 
               country_abbv, 
               country_name, 
               MD5(country_abbv,country_name) AS hash_diff, 
               CURRENT_DATE AS load_dts,  
               stage_rec_src AS rec_src  
FROM COETESTDB.DATAVAULT_STG.COUNTRY_STG ; 


-- create Region Stage table 
create or replace TABLE REGION_STG (
	REGION_ID VARCHAR(20),
	REGION_NAME VARCHAR(100),
	STAGE_REC_SRC VARCHAR(20),
	LOAD_UPDATE_DT DATE
);
-- insert into Region Stage table
insert into COETESTDB.DATAVAULT_STG.REGION_STG values
('us-east-1','US East (N. Virginia)',2006,'AWS',CURRENT_DATE),
('us-east-2','US East (Ohio)',2016,'AWS',CURRENT_DATE),
('us-west-1','US West (N. California)',2009,'AWS',CURRENT_DATE),
('us-west-2','US West (Oregon)',2011,'AWS',CURRENT_DATE),
('ca-central-1','Canada (Central)',2016,'AWS',CURRENT_DATE),
('eu-north-1','EU (Stockholm)',2018,'AWS',CURRENT_DATE),
('eu-west-3','EU (Paris)',2017,'AWS',CURRENT_DATE),
('eu-west-2','EU (London)',2016,'AWS',CURRENT_DATE),
('eu-west-1','EU (Ireland)',2007,'AWS',CURRENT_DATE),
('eu-central-1','EU (Frankfurt)',2014,'AWS',CURRENT_DATE),
('eu-south-1','EU (Milan)',2020,'AWS',CURRENT_DATE),
('ap-south-1','Asia Pacific (Mumbai)',2016,'AWS',CURRENT_DATE),
('ap-northeast-1','Asia Pacific (Tokyo)',2011,'AWS',CURRENT_DATE),
('ap-northeast-2','Asia Pacific (Seoul)',2016,'AWS',CURRENT_DATE),
('ap-northeast-3','Asia Pacific (Osaka-Local)',2018,'AWS',CURRENT_DATE),
('ap-southeast-1','Asia Pacific (Singapore)',2010,'AWS',CURRENT_DATE),
('ap-southeast-2','Asia Pacific (Sydney)',2012,'AWS',CURRENT_DATE),
('ap-east-1','Asia Pacific (Hong Kong) SAR',2019,'AWS',CURRENT_DATE),
('sa-east-1','South America (SÃ£o Paulo)',2011,'AWS',CURRENT_DATE),
('cn-north-1','China (Beijing)',2014,'AWS',CURRENT_DATE),
('cn-northwest-1','China (Ningxia)',2017,'AWS',CURRENT_DATE),
('us-gov-east-1','GovCloud (US-East)',2018,'AWS',CURRENT_DATE),
('us-gov-west-1','GovCloud (US-West)',2011,'AWS',CURRENT_DATE),
('us-gov-secret-1','AWS Secret Region (US-Secret)',2017,'AWS',CURRENT_DATE),
('us-gov-topsecret-1','AWS Top Secret Region (US-Secret)',2014,'AWS',CURRENT_DATE),
('me-south-1','Middle East (Bahrain)',2019,'AWS',CURRENT_DATE),
('af-south-1','Africa (Cape Town)',2020,'AWS',CURRENT_DATE)
;

--Create the HUB Region table
create or replace TABLE COETESTDB.DATAVAULT.HUB_REGION(
	HUB_REGION_KEY VARCHAR(32),
	REGION_ID VARCHAR(20),
	HUB_LOAD_DT DATE,
	HUB_REC_SRC VARCHAR(10)
);

--Create the SAT Region table
create or replace TABLE COETESTDB.DATAVAULT.SAT_REGION(
	HUB_REGION_KEY VARCHAR(32),
	REGION_NAME VARCHAR(100),
    LAUNCH_YEAR NUMBER,
	HASH_DIFF VARCHAR(32),
	SAT_LOAD_DT DATE,
	SAT_REC_SRC VARCHAR(10)
);

--Multi Table Insert from REGION stage into HUB REGION and SAT REGION
INSERT ALL  
WHEN (SELECT COUNT(*)     
             FROM coetestdb.datavault.hub_region hr        
             WHERE hr.hub_region_key = hash_key) = 0  
 THEN    
    INTO coetestdb.datavault.hub_region (hub_region_key, region_id,hub_load_dt,hub_rec_src)                  
    VALUES (hash_key, region_id,load_dts, rec_src)  
WHEN (SELECT COUNT(*)         
          FROM coetestdb.datavault.sat_region sc        
          WHERE sc.hub_region_key = hash_key 
      AND sc.hash_diff = MD5(region_name||launch_year)) = 0    
THEN    
INTO coetestdb.datavault.sat_region (hub_region_key, region_name, launch_year,hash_diff,sat_load_dt,sat_rec_src)                    
          VALUES (hash_key, region_name , launch_year, hash_diff , load_dts,rec_src)  
                      
SELECT MD5(region_id) AS hash_key, 
               region_id, 
               region_name, 
			   launch_year,
               MD5(region_name||launch_year) AS hash_diff, 
               CURRENT_DATE AS load_dts,  
               stage_rec_src AS rec_src  
FROM COETESTDB.DATAVAULT_STG.REGION_STG ; 

-- Create Country Region Relationship stage table
create or replace table country_region_stg (
COUNTRY_ABBV VARCHAR(5),
REGION_ID VARCHAR(20),
STAGE_REC_SRC VARCHAR(20),
LOAD_UPDATE_DT DATE
);

-- Load data into Country Region Relationship stage table
insert into country_region_stg values
('US','us-east-1','AWS',CURRENT_DATE),
('US','us-east-2','AWS',CURRENT_DATE),
('US','us-west-1','AWS',CURRENT_DATE),
('US','us-west-2','AWS',CURRENT_DATE),
('US','us-gov-east-1','AWS',CURRENT_DATE),
('US','us-gov-west-1','AWS',CURRENT_DATE),
('US','us-gov-secret-1','AWS',CURRENT_DATE),
('US','us-gov-topsecret-1','AWS',CURRENT_DATE),
('CA','ca-central-1','AWS',CURRENT_DATE),
('BR','sa-east-1','AWS',CURRENT_DATE),
('ZA','af-south-1','AWS',CURRENT_DATE),
('AU','ap-southeast-2','AWS',CURRENT_DATE),
('SG','ap-southeast-1','AWS',CURRENT_DATE),
('JP','ap-northeast-1','AWS',CURRENT_DATE),
('JP','ap-northeast-3','AWS',CURRENT_DATE),
('KR','ap-northeast-2','AWS',CURRENT_DATE),
('CN','cn-north-1','AWS',CURRENT_DATE),
('CN','cn-northwest-1','AWS',CURRENT_DATE),
('HK','ap-east-1','AWS',CURRENT_DATE),
('IN','ap-south-1','AWS',CURRENT_DATE),
('GB','eu-west-2','AWS',CURRENT_DATE),
('IE','eu-west-1','AWS',CURRENT_DATE),
('FR','eu-west-3','AWS',CURRENT_DATE),
('DE','eu-central-1','AWS',CURRENT_DATE),
('IT','eu-south-1','AWS',CURRENT_DATE),
('SE','eu-north-1','AWS',CURRENT_DATE);

-- Create Country Region Link table 
create or replace TABLE COETESTDB.DATAVAULT.LINK_COUNTRY_REGION (
	LINK_COUNTRY_REGION_KEY VARCHAR(32),
	HUB_COUNTRY_KEY VARCHAR(32),
	HUB_REGION_KEY VARCHAR(32),
	COUNTRY_ABBV VARCHAR(5),
	REGION_ID VARCHAR(20),
	LINK_LOAD_DT DATE,
	LINK_REC_SRC VARCHAR(10)
);

-- Load data into Country Region Link table 
INSERT INTO COETESTDB.DATAVAULT.LINK_COUNTRY_REGION
SELECT
MD5(src.country_abbv||src.region_id) AS LINK_COUNTRY_REGION_KEY,
COUNTRY.HUB_COUNTRY_KEY,
REGION.HUB_REGION_KEY ,
COUNTRY.COUNTRY_ABBV,
REGION.REGION_ID,
current_date as LINK_LOAD_DT ,
src.STAGE_REC_SRC as LINK_REC_SRC
FROM COETESTDB.DATAVAULT_STG.country_region_stg src
INNER JOIN COETESTDB.DATAVAULT.HUB_COUNTRY COUNTRY ON COUNTRY.COUNTRY_ABBV = SRC.COUNTRY_ABBV
INNER JOIN COETESTDB.DATAVAULT.HUB_REGION REGION ON REGION.REGION_ID = SRC.REGION_ID
WHERE NOT EXISTS 
(SELECT 1 FROM COETESTDB.DATAVAULT.LINK_COUNTRY_REGION LCR WHERE LCR.HUB_COUNTRY_KEY = COUNTRY.HUB_COUNTRY_KEY
 AND LCR.HUB_REGION_KEY = REGION.HUB_REGION_KEY)

-- updating the stage for region 
update COETESTDB.DATAVAULT_STG.REGION_STG set region_name = 'South America (Sao Paulo)' where region_id = 'sa-east-1'

--running a load script for region sat 

insert into coetestdb.datavault.sat_region 
SELECT         MD5(region_id) AS hash_key, 
               region_name, 
			   launch_year,
               MD5(region_name||launch_year) AS hash_diff, 
               CURRENT_DATE AS load_dts,  
               stage_rec_src AS rec_src  
FROM COETESTDB.DATAVAULT_STG.REGION_STG stg
where not exists (select 1 from COETESTDB.DATAVAULT.sat_region sr where sr.hash_diff = MD5(stg.region_name||stg.launch_year))

create or replace view coetestdb.datavault.country_region_v AS
WITH country_region as
(
  select 
    SC.hub_country_key,
    SC.country_name,
    SR.hub_region_key,
    SR.region_name,
    SR.launch_year,
    SR.sat_rec_src ,
    SR.SAT_LOAD_DT
  from 
    COETESTDB.DATAVAULT.LINK_COUNTRY_REGION LCR 
    INNER JOIN coetestdb.datavault.sat_country SC ON LCR.HUB_COUNTRY_KEY =  SC.HUB_COUNTRY_KEY
    INNER JOIN coetestdb.datavault.sat_region SR  ON LCR.HUB_REGION_KEY =   SR.HUB_REGION_KEY
),
COUNTRY_REGION_VERSION AS 
(
    SELECT 
        ROW_NUMBER() OVER (PARTITION BY HUB_REGION_KEY ORDER BY SAT_LOAD_DT NULLS LAST ) AS VERSION_NUMBER ,
        ROW_NUMBER() OVER (PARTITION BY HUB_REGION_KEY ORDER BY SAT_LOAD_DT DESC NULLS LAST ) AS REC_IND ,
        *
    FROM country_region
),
COUNTRY_REGION_VERSION_IND AS 
(
    SELECT 
        DECODE(REC_IND ,1,'Y','N') AS CURRENT_INDIC,
        *
    FROM COUNTRY_REGION_VERSION
)
SELECT 
CURRENT_INDIC,
VERSION_NUMBER,
hub_country_key,
country_name,
hub_region_key,
region_name,
launch_year,
sat_rec_src AS DATA_SOURCE,
SAT_LOAD_DT AS LOAD_DT
FROM
COUNTRY_REGION_VERSION_IND