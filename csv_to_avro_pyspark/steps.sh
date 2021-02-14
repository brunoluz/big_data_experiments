#!/bin/bash
/root/csv_to_avro_pyspark
source venv/bin/activate
spark-submit app.py
