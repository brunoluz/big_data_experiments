import pyspark
import pathlib
import os

from pyspark.sql import SparkSession
import shutil

current_dir = pathlib.Path().absolute()
sc = pyspark.SparkContext('local[*]')
spark = SparkSession(sc).builder.getOrCreate()
spark.conf.set("spark.sql.avro.compression.codec", "deflate")


def execute(file):
    out_dir = os.path.join(current_dir, 'data_samples', 'output')
    if os.path.isdir(out_dir):
        shutil.rmtree(out_dir)

    rdd = spark.read.format('csv').options(header='true', inferSchema='true').load(file)
    df = getattr(rdd, 'toDF')(*rdd.columns)
    df.write.format("avro").save(out_dir)


if __name__ == '__main__':
    # execute(os.path.join(current_dir, 'data_samples', 'dataset.csv'))
    execute(os.path.join(current_dir, 'data_samples', 'netflix_titles.csv'))
