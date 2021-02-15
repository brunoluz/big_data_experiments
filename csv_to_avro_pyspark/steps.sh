#!/bin/bash
/root/csv_to_avro_pyspark
source venv/bin/activate
./venv_wsl/bin/spark-submit --packages org.apache.spark:spark-avro_2.12:3.0.1 app.py 
./venv_wsl/bin/spark-submit --packages com.databricks:spark-avro_2.11:4.0.0 app.py 