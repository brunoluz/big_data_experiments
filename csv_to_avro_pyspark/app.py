import json

import pyspark
import pathlib
import os
from pyspark.sql import SparkSession

current_dir = pathlib.Path().absolute()
sc = pyspark.SparkContext('local[*]')
spark = SparkSession(sc)


def execute(file):

    out_dir = os.path.join(current_dir, 'data_samples', 'bruno')

    rdd = sc.textFile(file) \
        .map(lambda line: line.split(";"))

    df = rdd.toDF()
    df.write.save(out_dir)



    # rdd.saveAsTextFile(os.path.join(current_dir, 'data_samples', 'bruno'))

    # rdd.saveAsHadoopFile(os.path.join(current_dir, 'data_samples', 'bruno'))
    # json_rdd.saveAsNewAPIHadoopFile(os.path.join(current_dir, 'data_samples', 'bruno'), "org.apache.avro.mapreduce.AvroKeyOutputFormat")


if __name__ == '__main__':
    execute(os.path.join(current_dir, 'data_samples', 'dataset.csv'))
