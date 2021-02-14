import pyspark
import pathlib
import os

current_dir = pathlib.Path().absolute()


def teste():
    sc = pyspark.SparkContext('local[*]')

    csv_file = os.path.join(current_dir, 'dataset.csv')

    txt = sc.textFile(csv_file)
    print(txt.count())


if __name__ == '__main__':
    teste()