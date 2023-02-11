import pandas as pd
from sqlalchemy import create_engine
from time import time
import argparse
import os

def main(params):
    user = params.user
    password = params.password
    port = params.port
    database = params.database
    table = params.table
    host = params.host
    file_link = params.file_link

    # parquet file name
    file_name = file_link.split('/')[-1]

    # Download the file
    os.system(f"wget {file_link}")

    df = pd.read_parquet(file_name)

    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{database}")

    df.head(n=0).to_sql(name=table, con=engine, if_exists="replace")

    CHUNK_SIZE = 100000
    for chunk_num in range(len(df) // CHUNK_SIZE + 1):
        s_time = time()

        start_index = chunk_num * CHUNK_SIZE
        end_index = min(chunk_num * CHUNK_SIZE + CHUNK_SIZE, len(df))
        chunk = df[start_index:end_index]

        chunk.to_sql(
            name=table,
            con=engine,
            if_exists="append",
            index=False,
            method="multi"
        )

        e_time = time()
        print(f"inserted {chunk_num+1} no. chunk, total time spent: {e_time-s_time:.3f} seconds")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest Parquet Data to Postgres")

    parser.add_argument("--user", help="user name for postgres")
    parser.add_argument("--password", help="password for postgres")
    parser.add_argument("--host", help="host name for postgres")
    parser.add_argument("--port", help="port for postgres")
    parser.add_argument("--database", help="database name for postgres")
    parser.add_argument("--table", help="table name for postgres")
    parser.add_argument("--file_link", help="file path of parquet file")

    args = parser.parse_args()
    main(args)





# python ingest_data.py \
#     --user=root  \
#     --password=root  \
#     --port=5432  \
#     --database=ny_taxi  \
#     --table=yellow_taxi_trips  \
#     --host=localhost  \
#     --file_link='https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-01.parquet'  \