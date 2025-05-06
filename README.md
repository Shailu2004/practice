CREATE EXTERNAL TABLE flight_info_hive(
    flight_id STRING,
    departure_delay INT,
    departure_time STRING,
    arrival_time STRING
)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key,delay:departure_delay,schedule:departure_time,schedule:arrival_time")
TBLPROPERTIES ("hbase.table.name" = "flight_info");
