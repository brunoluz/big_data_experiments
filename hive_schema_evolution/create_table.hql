DROP TABLE avro_schema_evolution;

CREATE EXTERNAL TABLE avro_schema_evolution
PARTITIONED BY (ordem int)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
STORED AS AVRO
LOCATION 'hdfs:///playground/schema_evolution_table'
TBLPROPERTIES ('avro.schema.url'='hdfs:///playground/schema_evolution_table/schemas/first_version.avsc');

ALTER TABLE avro_schema_evolution
SET TBLPROPERTIES ('avro.schema.url'='hdfs:///playground/schema_evolution_table/schemas/first_version.avsc');

ALTER TABLE avro_schema_evolution
SET TBLPROPERTIES ('avro.schema.url'='hdfs:///playground/schema_evolution_table/schemas/second_version.avsc');

ALTER TABLE avro_schema_evolution
SET TBLPROPERTIES ('avro.schema.url'='hdfs:///playground/schema_evolution_table/schemas/third_version.avsc');

ALTER TABLE avro_schema_evolution 
ADD PARTITION (ordem=1) 
LOCATION 'hdfs:///playground/schema_evolution_table/ordem=1/';

ALTER TABLE avro_schema_evolution
ADD PARTITION (ordem=2)
LOCATION 'hdfs:///playground/schema_evolution_table/ordem=2/';

ALTER TABLE avro_schema_evolution
ADD PARTITION (ordem=3)
LOCATION 'hdfs:///playground/schema_evolution_table/ordem=3/';


SELECT * FROM avro_schema_evolution;
