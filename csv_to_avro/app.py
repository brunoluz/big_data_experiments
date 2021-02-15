import json

import avro.schema
from avro.datafile import DataFileWriter
from avro.io import DatumWriter
import csv
from collections import namedtuple

csv_file = "dataset.csv"
fields = ("Id", "Nome", "Idade")
csv_file_record = namedtuple("csv_record", fields)


def read_csv_data():
    with open(csv_file, newline='') as csv_data:
        csv_data.readline()  # ignore first line.
        reader = csv.reader(csv_data, delimiter=';')
        for row in map(csv_file_record._make, reader):
            yield row


schema = avro.schema.parse(json.dumps({
    "namespace": "dataset.avro",
    "type": "record",
    "name": "dataset",
    "fields": [
        {"name": "Id", "type": "string"},
        {"name": "Nome", "type": "string"},
        {"name": "Idade", "type": "string"},
    ]
}))


def serialize_records(records, outpath="dataset.avro"):
    with open(outpath, 'wb') as out:
        writer = DataFileWriter(out, DatumWriter(), schema)
        for record in records:
            record2 = dict((f, getattr(record, f)) for f in record._fields)
            record3 = record._asdict()
            writer.append(record2)
        writer.close()


if __name__ == '__main__':
    serialize_records(read_csv_data())
