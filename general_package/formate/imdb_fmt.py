from ..s3.get_object import get_object
from ..s3.upload_object import upload_object
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
from io import BytesIO
import gzip
#import datetime


# first version not working with title.basics
def imdb_fmt1(table_name, **kwargs):
    date = kwargs['execution_date'].strftime("%Y%m%d")
    response = get_object("raw", "imdb", table_name, f"{table_name}.tsv.gz", date)
    file_content =response['Body'].read()
    file_content = gzip.decompress(file_content)
    df = pd.read_csv(BytesIO(file_content), sep='\t')
    table = pa.Table.from_pandas(df)
    buf = BytesIO()
    pq.write_table(table, buf, compression='snappy')
        
    upload_object("formated", "imdb", table_name, f"{table_name}.snappy.parquet", buf.getvalue(), **kwargs)

# second version working with all the datasets
def imdb_fmt(table_name, **kwargs):
    print(table_name)
    date = kwargs['execution_date'].strftime("%Y%m%d")
    response = get_object("raw", "imdb", table_name, f"{table_name}.tsv.gz", date)
    file_content = response['Body'].read()
    file_content = gzip.decompress(file_content)
    # Read the file into a dataframe
    df = pd.read_csv(BytesIO(file_content), sep='\t')
    # List of problematic columns
    bool_cols = ['isAdult', 'endYear', 'deathYear']
    # Drop the problematic columns if they exist
    for col in bool_cols:
        if col in df.columns:
            df = df.drop(col, axis=1)
    if 'originalTitle' in df.columns:
        # remplace le nom de la colonne originalTitle par original_title
        df = df.rename(columns={'originalTitle': 'original_title'})
    table = pa.Table.from_pandas(df)
    buf = BytesIO()
    pq.write_table(table, buf, compression='snappy')
    
    upload_object("formated", "imdb", table_name, f"{table_name}.snappy.parquet", buf.getvalue(), **kwargs)