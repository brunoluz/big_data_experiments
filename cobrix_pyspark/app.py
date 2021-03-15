import os
import pathlib
import pyspark
from pyspark.sql import SparkSession


if __name__ == '__main__':
    current_dir = pathlib.Path().absolute()
    out_dir = os.path.join(current_dir, 'output')
    data_dir = os.path.join(current_dir, 'data_samples')
    copybook_file = os.path.join(data_dir, 'test1_copybook.cob')
    ebcdic_file = os.path.join(data_dir, 'test1_data.bin')
    
    sc = pyspark.SparkContext('local[*]')
    spark = SparkSession(sc)


 #   df = spark.read.format('cobol').option("copybook", 'file://' + copybook_file).load('file://' + ebcdic_file)
 #   df.show()