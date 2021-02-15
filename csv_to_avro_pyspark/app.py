import pyspark
import pathlib
import os
from pyspark.sql import SparkSession
import shutil


current_dir = pathlib.Path().absolute()
sc = pyspark.SparkContext('local[*]')
spark = SparkSession(sc)
spark.conf.set("spark.sql.avro.compression.codec", "deflate")


def execute(file):

    out_dir = os.path.join(current_dir, 'data_samples', 'bruno')
    if os.path.isdir(out_dir):
        shutil.rmtree(out_dir)

    # rdd = sc.textFile(file) \
    #    .map(lambda line: line.split(";"))

    rdd = sc.parallelize([{'id': 1}, {'id': 2}, {'id': 3}])

    df = rdd.toDF()
    # df.coalesce(1).write.format("avro").save(out_dir)
    df.write.format("avro").save(out_dir)


if __name__ == '__main__':
    execute(os.path.join(current_dir, 'data_samples', 'dataset.csv'))
