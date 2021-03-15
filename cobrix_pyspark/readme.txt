source venv/bin/activate
./venv/bin/spark-submit --packages za.co.absa.cobrix:spark-cobol_2.12:2.2.1 app.py


Environment variables:
PYSPARK_SUBMIT_ARGS="--packages za.co.absa.cobrix:spark-cobol_2.12:2.2.1"