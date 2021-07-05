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
            {"name": "country", "type": ["string", "null"], "default": ""}
        ]}

    with open('third_version.avsc', 'w') as f1:
        f1.write(json.dumps(schema))
        f1.close()

    # Write data to an avro file
    with open('third_version.avro', 'wb') as f2:
        writer = DataFileWriter(f2, DatumWriter(), avro.schema.make_avsc_object(schema))
        writer.append({'name': 'Barbara', 'country': 'Italy'})
        writer.close()


if __name__ == '__main__':
    execute()
