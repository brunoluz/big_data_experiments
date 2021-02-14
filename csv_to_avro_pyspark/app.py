import pyspark
import pathlib
import os

current_dir = pathlib.Path().absolute()


def convert_csv_to_avro():
    sc = pyspark.SparkContext('local[*]')

    csv_file = os.path.join(current_dir, 'dataset.csv')

    txt = sc.textFile(csv_file)
    print(txt.count())


if __name__ == '__main__':
    convert_csv_to_avro()
