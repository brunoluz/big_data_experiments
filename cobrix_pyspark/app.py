import os
import pathlib
import pyspark
from datetime import datetime
from pyspark.sql import SparkSession
import json

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages za.co.absa.cobrix:spark-cobol_2.12:2.2.1 pyspark-shell'

current_dir = pathlib.Path().absolute()
copybook_file = os.path.join(current_dir, 'data_samples', 'test1_copybook.cob')
ebcdic_file = os.path.join(current_dir, 'data_samples', 'test1_data.bin')
output_file = os.path.join(current_dir, 'output', f"{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")

# tratativa para spark usar arquivos locais em vez do HDFS
if os.name == 'nt':  # Windows
    copybook_file = 'file:\\' + copybook_file
    ebcdic_file = 'file:\\' + ebcdic_file
else:  # Linux
    copybook_file = 'file://' + copybook_file
    ebcdic_file = 'file://' + copybook_file

sc = pyspark.SparkContext('local[*]')
spark = SparkSession(sc)

df = spark.read \
    .format('cobol') \
    .option("copybook", copybook_file) \
    .load(ebcdic_file)

items = df.toJSON().collect()
result = []
for i in items:
    result.append(json.loads(i))

with open(output_file, 'w') as outfile:
    json.dump(result, outfile, indent=3)
