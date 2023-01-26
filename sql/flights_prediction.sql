drop table if exists flights_prediction_csv;
CREATE EXTERNAL TABLE flights_prediction_csv(year int, month int, dayofmonth int,
 dayofweek int, deptime int, crsdeptime int, arrtime int,
 crsarrtime int, uniquecarrier string, flightnum int, tailnum string,
 actualelapsedtime int, crselapsedtime int, airtime int, arrdelay int,
 depdelay int, origin string, dest string, distance int, taxiin int,
 taxiout int, cancelled int, cancellationcode string, diverted string,
 carrierdelay int, weatherdelay int, nasdelay int, securitydelay int,
lateaircraftdelay int, origin_lon string, origin_lat string, dest_lon string, dest_lat string, prediction string, proba string, weather_json string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\""
)
STORED AS TEXTFILE LOCATION 's3a://proj-eyjafjallajoekull/streaming/flights' tblproperties("skip.header.line.count"="1");

drop table if exists flights_prediction_ice;
create table flights_prediction_ice
( month int, dayofmonth int,
 dayofweek int, deptime int, crsdeptime int, arrtime int,
 crsarrtime int, uniquecarrier string, flightnum int, tailnum string,
 actualelapsedtime int, crselapsedtime int, airtime int, arrdelay int,
 depdelay int, origin string, dest string, distance int, taxiin int,
 taxiout int, cancelled int, cancellationcode string, diverted string,
 carrierdelay int, weatherdelay int, nasdelay int, securitydelay int,
lateaircraftdelay int, origin_lon float, origin_lat float, dest_lon float, dest_lat float,
prediction float, proba float, prediction_delay int,
temp float, pressure float, humidity integer, wind_speed float, clouds integer)
partitioned by (year int)
stored by
 ICEBERG;


insert overwrite table flights_prediction_ice
select
  month, dayofmonth, dayofweek, deptime, crsdeptime, arrtime, crsarrtime, uniquecarrier, flightnum, tailnum,
 actualelapsedtime, crselapsedtime, airtime, arrdelay, depdelay, origin, dest, cast( distance as integer ) as distance, taxiin, taxiout,
 cancelled, cancellationcode, diverted, carrierdelay, weatherdelay, nasdelay, securitydelay, lateaircraftdelay,
 origin_lon, origin_lat, cast( dest_lon as float) as dest_lon, cast(dest_lat as float) as dest_lat,
cast( translate( substr(prediction, instr(prediction,'prediction=')+11,1 ),'}','') as float) as prediction,
 cast( translate( substr(prediction, instr(prediction,'proba=')+6,3 ),'}','') as float) as proba,
 case translate( substr(prediction, instr(prediction,'prediction=')+11,1 ),'}','')
   when 1 then trunc(rand() * 99 + 1) end as prediction_delay ,
 cast( translate( substr( weather_json, instr(weather_json,'temp=')+5,5 ),',','') as float) as  temp,
 cast( translate( substr( weather_json, instr(weather_json,'pressure=')+9,6 ),',','') as float) as  presssure,
 cast( translate( substr( weather_json, instr(weather_json,'humidity=')+9,2 ),',','') as float) as  humidity,
 cast( translate( substr( weather_json, instr(weather_json,'speed=')+6,5 ),',','') as float) as  wind_speed,
 cast( translate( substr( weather_json, instr(weather_json,'all=')+4,3 ),'}','') as float) as clouds,
 year
from
 flights_prediction_csv;
 
drop table if exists flights_prediction_offset;
create table flights_prediction_offset ( t bigint);


insert overwrite table flights_prediction_offset
select
max( unix_timestamp(concat( year,'-', month, '-', dayofmonth, ' ' ,
      substring(lpad(deptime,4,'0'),1,2),':', substring(lpad(deptime,4,'0'),3,2) ,':00' )))
  from
 flights_prediction_ice;
--
select from_unixtime(t) from flights_prediction_offset;

select
from_unixtime( max( unix_timestamp(concat( year,'-', month, '-', dayofmonth, ' ' ,
      substring(lpad(deptime,4,'0'),1,2),':', substring(lpad(deptime,4,'0'),3,2) ,':00' ))))
  from
 flights_prediction_csv;
 
select
from_unixtime( max( unix_timestamp(concat( year,'-', month, '-', dayofmonth, ' ' ,
      substring(lpad(deptime,4,'0'),1,2),':', substring(lpad(deptime,4,'0'),3,2) ,':00' ))))
  from
 flights_prediction_ice;

select count(1) from flights_prediction_ice;
--
drop view flights_prediction_cve;
create view flights_prediction_cve as
select
  month, dayofmonth, dayofweek, deptime, crsdeptime, arrtime, crsarrtime, uniquecarrier, flightnum, tailnum,
  actualelapsedtime, crselapsedtime, airtime, arrdelay, depdelay, origin, dest, cast( distance as integer ) as distance, taxiin, taxiout,
  cancelled, cancellationcode, diverted, carrierdelay, weatherdelay, nasdelay, securitydelay, lateaircraftdelay,
  origin_lon, origin_lat, cast( dest_lon as float) as dest_lon, cast(dest_lat as float) as dest_lat,
  cast( translate( substr(prediction, instr(prediction,'prediction=')+11,1 ),'}','') as float) as prediction,
  cast( translate( substr(prediction, instr(prediction,'proba=')+6,3 ),'}','') as float) as proba,
  case translate( substr(prediction, instr(prediction,'prediction=')+11,1 ),'}','')
    when 1 then trunc(rand() * 99 + 1) end as prediction_delay ,
  cast( translate( substr( weather_json, instr(weather_json,'temp=')+5,5 ),',','') as float) as  temp,
  cast( translate( substr( weather_json, instr(weather_json,'pressure=')+9,6 ),',','') as float) as  presssure,
  cast( translate( substr( weather_json, instr(weather_json,'humidity=')+9,2 ),',','') as float) as  humidity,
  cast( translate( substr( weather_json, instr(weather_json,'speed=')+6,5 ),',','') as float) as  wind_speed,
  cast( translate( substr( weather_json, instr(weather_json,'all=')+4,3 ),'}','') as float) as clouds,
  year
from
 airlinedata.flights_prediction_csv, airlinedata.flights_prediction_offset offset
where
  unix_timestamp(concat( year,'-', month, '-', dayofmonth, ' ' ,
      substring(lpad(deptime,4,'0'),1,2),':', substring(lpad(deptime,4,'0'),3,2) ,':00' )) > offset.t 
;

select * from flights_prediction_cve;

drop scheduled query flights_prediction_ingest;
create scheduled query flights_prediction_ingest cron '0 */5 * * * ? *' defined as
from ( select * from airlinedata.flights_prediction_cve ) new_rows
insert into airlinedata.flights_prediction_ice select new_rows.*;

alter scheduled query flights_prediction_ingest enable;
alter scheduled query flights_prediction_ingest execute;

drop scheduled query flights_prediction_ingest_offset;
create scheduled query flights_prediction_ingest_offset cron '0 1/5 * * * ? *' defined as
insert overwrite table airlinedata.flights_prediction_offset
select
max( unix_timestamp(concat( year,'-', month, '-', dayofmonth, ' ' ,
      substring(lpad(deptime,4,'0'),1,2),':', substring(lpad(deptime,4,'0'),3,2) ,':00' )))
  from
 airlinedata.flights_prediction_ice;

alter scheduled query flights_prediction_ingest_offset enable;
alter scheduled query flights_prediction_ingest_offset execute;

select * from information_schema.scheduled_queries;
select * from information_schema.scheduled_executions;



select
from_unixtime( unix_timestamp(concat( year,'-', month, '-', dayofmonth, ' ' ,
      substring(lpad(deptime,4,'0'),1,2),':', substring(lpad(deptime,4,'0'),3,2) ,':00' )))
 ,from_unixtime( 
   unix_timestamp( 
    concat( year,'-', month, '-', dayofmonth, ' ' ,
      substring(lpad(deptime,4,'0'),1,2),':', 
      substring(lpad(deptime,4,'0'),3,2) ,':00' )) 
         + cast( round( nvl( distance,0)*10 , 0) 
               + round( nvl( prediction_delay,0) * 60 , 0 ) as integer)
            )
            , distance
            ,prediction_delay
  from
 flights_prediction_ice_cve;
