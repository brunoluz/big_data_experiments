import os
import pathlib
import pyspark
from pyspark.sql import SparkSession


if __name__ == '__main__':
    current_dir = pathlib.Path().absolute()
    out_dir = os.path.join(current_dir, 'data_samples', 'bruno')
    sc = pyspark.SparkContext('local[*]')
    spark = SparkSession(sc)
    spark.read.format('avro').load(out_dir).show()
