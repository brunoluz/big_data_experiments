import json

import avro.schema
from avro.datafile import DataFileWriter
from avro.io import DatumWriter


def execute():
    schema = {
        "namespace": "example.avro",
        "type": "record",
        "name": "User",
        "fields": [
            {"name": "name", "type": "string"},
            {"name": "age", "type": ["int", "null"]},
        ]}

    with open('first_version.avsc', 'w') as f1:
        f1.write(json.dumps(schema))
        f1.close()

    # Write data to an avro file
    with open('first_version.avro', 'wb') as f2:
        avro_schema = avro.schema.make_avsc_object(schema)
        writer = DataFileWriter(f2, DatumWriter(), avro_schema)
        writer.append({'name': 'Jorge Damasceno', 'age': 77})
        writer.close()


if __name__ == '__main__':
    execute()
