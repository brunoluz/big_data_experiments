import os
import pathlib
import pyspark
from pyspark.sql import SparkSession


os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages za.co.absa.cobrix:spark-cobol_2.12:2.2.1 pyspark-shell'

current_dir = pathlib.Path().absolute()
data_dir = os.path.join(current_dir, 'data_samples')
copybook_file = os.path.join(data_dir, 'test1_copybook.cob')
ebcdic_file = os.path.join(data_dir, 'test1_data.bin')

sc = pyspark.SparkContext('local[*]')
spark = SparkSession(sc)

df = spark.read.format('cobol').option("copybook", 'file://' + copybook_file).load('file://' + ebcdic_file)
df.show()