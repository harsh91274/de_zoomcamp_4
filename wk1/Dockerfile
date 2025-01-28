FROM python:3.9

RUN pip install pandas sqlalchemy psycopg2 pyarrow

WORKDIR /app
COPY yellow_tripdata_2021-01.parquet yellow_tripdata_2021-01.parquet
COPY ingest_data.py ingest_data.py

ENTRYPOINT ["python", "ingest_data.py"]