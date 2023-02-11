FROM python:3.10-buster

RUN apt-get install wget
RUN pip install pandas sqlaclchemy psycopg2

WORKDIR /app
COPY ingest_data.py ingest_data.py

ENTRYPOINT [ "python", "ingest_data.py" ]