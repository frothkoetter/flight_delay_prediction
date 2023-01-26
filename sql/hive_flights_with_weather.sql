SELECT DISTINCT
   f.origin as origin,
   oa.lat as origin_lat,
   oa.lon as origin_lon,
   f.dest as dest,
   da.lat as dest_lat,
   da.lon as dest_lon,
   f.uniquecarrier as uniquecarrier,
   count(*) as cnt_flights
FROM
  flights_orc f,
   airports_orc oa,
   airports_orc da  
WHERE
   f.origin = oa.iata
   and f.dest = da.iata
group by f.origin, oa.lat,oa.lon, f.dest,da.lat,da.lon, uniquecarrier
order by cnt_flights desc


drop table if exists flights_with_weather;
CREATE EXTERNAL TABLE flights_with_weather(year int, month int, dayofmonth int,
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

select prediction from flights_with_weather 

select * from flights_with_weather;

select
 dest,
 cast( dest_lat as float) as lat,
 cast(dest_lon as float) as lon,
 cast( translate( substr( weather_json, instr(weather_json,'temp=')+5,5 ),',','') as float) as  temp,
  cast( translate( substr( weather_json, instr(weather_json,'pressure=')+9,6 ),',','') as float) as  presssure,
   cast( translate( substr( weather_json, instr(weather_json,'humidity=')+9,2 ),',','') as float) as  humidity,
  cast( translate( substr( weather_json, instr(weather_json,'speed=')+6,5 ),',','') as float) as  wind_speed,
    cast( translate( substr( weather_json, instr(weather_json,'all=')+4,3 ),'}','') as float) as clouds,
     cast( translate( substr(prediction, instr(prediction,'prediction=')+11,1 ),'}','') as float) as prediction,
     cast( translate( substr(prediction, instr(prediction,'proba=')+6,3 ),'}','') as float) as proba
from
SELECT DISTINCT
   f.origin as origin,
   oa.lat as origin_lat,
   oa.lon as origin_lon,
   f.dest as dest,
   da.lat as dest_lat,
   da.lon as dest_lon,
   f.uniquecarrier as uniquecarrier,
   count(*) as cnt_flights
FROM
  flights_orc f,
   airports_orc oa,
   airports_orc da  
WHERE
   f.origin = oa.iata
   and f.dest = da.iata
group by f.origin, oa.lat,oa.lon, f.dest,da.lat,da.lon, uniquecarrier
order by cnt_flights desc


drop table if exists flights_with_weather;
CREATE EXTERNAL TABLE flights_with_weather(year int, month int, dayofmonth int,
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

select prediction from flights_with_weather 

select * from flights_with_weather;

select
 dest,
 cast( dest_lat as float) as lat,
 cast(dest_lon as float) as lon,
 cast( translate( substr( weather_json, instr(weather_json,'temp=')+5,5 ),',','') as float) as  temp,
  cast( translate( substr( weather_json, instr(weather_json,'pressure=')+9,6 ),',','') as float) as  presssure,
   cast( translate( substr( weather_json, instr(weather_json,'humidity=')+9,2 ),',','') as float) as  humidity,
  cast( translate( substr( weather_json, instr(weather_json,'speed=')+6,5 ),',','') as float) as  wind_speed,
    cast( translate( substr( weather_json, instr(weather_json,'all=')+4,3 ),'}','') as float) as clouds,
     cast( translate( substr(prediction, instr(prediction,'prediction=')+11,1 ),'}','') as float) as prediction,
     cast( translate( substr(prediction, instr(prediction,'proba=')+6,3 ),'}','') as float) as proba
from
 flights_with_weather;


drop table if exists flights_with_weather;
CREATE EXTERNAL TABLE flights_with_weather(year int, month int, dayofmonth int,
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

select * from flights_streaming_csv limit 10;

drop table if exists flights_streaming_offset;
create table flights_streaming_offset ( t timestamp);

drop table if exists flights_streaming_orc;
create table flights_streaming_orc 
( month int, dayofmonth int,
 dayofweek int, deptime int, crsdeptime int, arrtime int,
 crsarrtime int, uniquecarrier string, flightnum int, tailnum string,
 actualelapsedtime int, crselapsedtime int, airtime int, arrdelay int,
 depdelay int, origin string, dest string, distance int, taxiin int,
 taxiout int, cancelled int, cancellationcode string, diverted string,
 carrierdelay int, weatherdelay int, nasdelay int, securitydelay int,
lateaircraftdelay int, origin_lon float, origin_lat float, dest_lon float, dest_lat float, 
prediction float, proba float,
temp float, pressure float, humidity integer, wind_speed float, clouds integer)
partitioned by (year int) ;

insert into 
 flights_streaming_orc 
select 
 year, month, dayofmonth, dayofweek, deptime, crsdeptime, arrtime, crsarrtime, uniquecarrier, flightnum, tailnum, 
 actualelapsedtime, crselapsedtime, airtime, arrdelay, depdelay, origin, dest, cast( distance as integer ) as distance, taxiin, taxiout, 
 cancelled, cancellationcode, diverted, carrierdelay, weatherdelay, nasdelay, securitydelay, lateaircraftdelay,
 origin_lon, origin_lat, cast( dest_lat as float) as dest_lat, cast(dest_lon as float) as dest_lon,
cast( translate( substr(prediction, instr(prediction,'prediction=')+11,1 ),'}','') as float) as prediction,
 cast( translate( substr(prediction, instr(prediction,'proba=')+6,3 ),'}','') as float) as proba,
 cast( translate( substr( weather_json, instr(weather_json,'temp=')+5,5 ),',','') as float) as  temp,
 cast( translate( substr( weather_json, instr(weather_json,'pressure=')+9,6 ),',','') as float) as  presssure,
 cast( translate( substr( weather_json, instr(weather_json,'humidity=')+9,2 ),',','') as float) as  humidity,
 cast( translate( substr( weather_json, instr(weather_json,'speed=')+6,5 ),',','') as float) as  wind_speed,
 cast( translate( substr( weather_json, instr(weather_json,'all=')+4,3 ),'}','') as float) as clouds
from 
 flights_streaming_csv;

select * from flights_streaming_orc;

insert into flights_streaming_offset 
select 
 max( cast(concat( year,'.', month, '.', dayofmonth ,':', deptime) as timestamp format 'YYYY.MM.DD:HH24MI')  )
from 
 flights_streaming_orc;
 
