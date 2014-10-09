CREATE EXTERNAL TABLE duanyou_hour_data(
  peerid string, 
  id int, 
  value1 string, 
  version string)
PARTITIONED BY ( 
  ds string, 
  dt string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
  STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.SequenceFileInputFormat' 
  OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat'
;

alter table duanyou_hour_data add columns(order string,other string);

