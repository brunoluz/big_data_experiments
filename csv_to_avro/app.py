import json

import avro.schema
from avro.datafile import DataFileWriter
from avro.io import DatumWriter
import csv
from collections import namedtuple

csv_file = "netflix_titles.csv"
fields = ("show_id", "type", "title", "director", "cast", "country", "date_added", "release_year", "rating", "duration", "listed_in", "description")
csv_file_record = namedtuple("csv_record", fields)


def read_csv_data():
    with open(csv_file, newline='', encoding='utf-8') as csv_data:
        csv_data.readline()  # ignore first line.
        reader = csv.reader(csv_data, delimiter=',')
        for row in map(csv_file_record._make, reader):
            yield row


schema = avro.schema.parse(json.dumps({
    "namespace": "dataset.avro",
    "type": "record",
    "name": "dataset",
    "fields": [
        {"name": "show_id", "type": "string"},
        {"name": "type", "type": "string"},
        {"name": "title", "type": "string"},
        {"name": "director", "type": "string"},
        {"name": "cast", "type": "string"},
        {"name": "country", "type": "string"},
        {"name": "date_added", "type": "string"},
        {"name": "release_year", "type": "string"},
        {"name": "rating", "type": "string"},
        {"name": "duration", "type": "string"},
        {"name": "listed_in", "type": "string"},
        {"name": "description", "type": "string"},
    ]
}))


def serialize_records(records, outpath="ouput.avro"):
    with open(outpath, 'wb') as out:
        writer = DataFileWriter(out, DatumWriter(), schema)
        for record in records:
            record2 = dict((f, getattr(record, f)) for f in record._fields)
            record3 = record._asdict()
            writer.append(record2)
        writer.close()


if __name__ == '__main__':
    serialize_records(read_csv_data())
