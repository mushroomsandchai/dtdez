import os
import time
import pandas as pd
import pyarrow.parquet as pq
from sqlalchemy import create_engine, Table, MetaData, text
from sqlalchemy.exc import OperationalError

def establish_connection(server):
    engine = create_engine(server)
    tries = 0
    while tries < 3:
        try:
            engine.connect().close() # test engine connection and immediately close it
            return engine
        except OperationalError:
            print("couldn't connect to postgres. trying again in 30 sec")
            tries += 1
            time.sleep(30)
    raise OperationalError("couldn't connect to the server after 3 attempts")


def parquet_writer(conn, filename):
    engine = create_engine(conn)
    file_path = f"ny_taxi/{filename}"
    table = file_exists(file_path)
    
    if 'zone' in filename:
        table_name = "zone"
    else:
        table_name = f'{filename.replace('.parquet', '')}'
    
    try:
        with engine.connect() as conn:
            conn.execute(text(f'DROP TABLE "{table_name}"'))
            conn.commit()
    except:
        pass
        
    total_time = time.time()
    for batch in table.iter_batches(batch_size = 50000):
        batch_time = time.time()
        (batch.to_pandas()).to_sql(con = engine, name = table_name, if_exists = 'append', index = False)
        print(f'batch time: {time.time() - batch_time:.2f}')
    print(f'total time: {time.time() - total_time:.2f}')

def csv_to_parquet(filename):
    file_path = f"ny_taxi/{filename}"
    df = pd.read_csv(file_path)
    df.to_parquet(path = file_path.replace(".csv", ".parquet"))

def file_exists(file_path):
    if os.path.exists(file_path):
        return(pq.ParquetFile(file_path))
    else:
        raise FileNotFoundError("couldn't find the file")