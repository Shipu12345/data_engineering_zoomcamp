import os
import argparse
from time import time
from datetime import timedelta
import pandas as pd
from sqlalchemy import create_engine
from prefect import flow, task
from prefect.tasks import task_input_hash
from ingest_types import IIngestDataParams
from ingest_arguments import IngestParams


@task(log_prints=True, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def extract_data(params: IIngestDataParams) -> pd.DataFrame:
    file_link = params.file_link

    # parquet file name
    file_name = file_link.split("/")[-1]

    # Download the file
    os.system(f"wget {file_link}")

    return pd.read_parquet(file_name)


@task(log_prints=True)
def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    print(f"pre: missing count:  { df['passenger_count'].isin([0]).sum() }")
    df = df[df["passenger_count"] != 0]
    print(f"post: missing count:  { df['passenger_count'].isin([0]).sum() }")

    return df


@task(log_prints=True, retries=3)
def ingest_data(client: IIngestDataParams, df: pd.DataFrame) -> None:
    engine = create_engine(
        f"postgresql://{client.user}:{client.password}@{client.host}:{client.port}/{client.database}"
    )

    df.head(n=0).to_sql(name=client.table, con=engine, if_exists="replace")

    CHUNK_SIZE = 50000
    for chunk_num in range(len(df) // CHUNK_SIZE + 1):
        s_time = time()

        start_index = chunk_num * CHUNK_SIZE
        end_index = min(chunk_num * CHUNK_SIZE + CHUNK_SIZE, len(df))
        chunk = df[start_index:end_index]

        chunk.to_sql(
            name=client.table,
            con=engine,
            if_exists="append",
            index=False,
            method="multi",
        )

        e_time = time()
        print(f"inserted {chunk_num+1} no. chunk, total time spent: {e_time-s_time:.3f} seconds")


@flow(name="Ingest Flow")
def main_flow():
    parser = argparse.ArgumentParser(description="Ingest Parquet Data to Postgres")
    parser.add_argument("--user", help="user name for postgres")
    parser.add_argument("--password", help="password for postgres")
    parser.add_argument("--host", help="host name for postgres")
    parser.add_argument("--port", help="port for postgres")
    parser.add_argument("--database", help="database name for postgres")
    parser.add_argument("--table", help="table name for postgres")
    parser.add_argument("--file_link", help="file path of parquet file")

    args = parser.parse_args()
    ingest_data_client = IngestParams(args)

    df = extract_data(ingest_data_client)
    data = transform_data(df)

    ingest_data(ingest_data_client, data)


if __name__ == "__main__":
    main_flow()


# python ingest_data.py \
#     --user=root  \
#     --password=root  \
#     --port=5432  \
#     --database=ny_taxi  \
#     --table=yellow_taxi_trips  \
#     --host=localhost  \
#     --file_link='https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-01.parquet'  \
