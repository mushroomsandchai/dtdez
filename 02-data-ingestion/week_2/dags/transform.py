import os
import time
import pandas as pd
import pyarrow.parquet as pq
from sqlalchemy import create_engine, Table, MetaData, text
from sqlalchemy.exc import OperationalError

# postgresql+psycopg2://root:root@db:5432/ny_taxi

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


def write_to_postgres(conn, filename):
    engine = create_engine(conn)
    file_path = f"ny_taxi/{filename}"
    if os.path.exists(file_path):
        yellow_jan_21 = pq.ParquetFile(file_path)
    else:
        raise FileNotFoundError("couldn't find the file")
    
    table_name = f'yellow_{filename.replace('.parquet', '').replace('-', '_')}'
    
    try:
        with engine.connect() as conn:
            conn.execute(text(f'DROP TABLE "{table_name}"'))
            conn.commit()
    except:
        pass
        
    total_time = time.time()
    for batch in yellow_jan_21.iter_batches(batch_size = 50000):
        batch_time = time.time()
        (batch.to_pandas()).to_sql(con = engine, name = table_name, if_exists = 'append', index = False)
        print(f'batch time: {time.time() - batch_time:.2f}')
    print(f'total time: {time.time() - total_time:.2f}')