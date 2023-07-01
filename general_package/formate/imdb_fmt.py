from ..s3.get_object import get_object
from ..s3.upload_object import upload_object
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
from io import BytesIO
import gzip
#import datetime


# first version not working with some of the datasets (title.akas, title.principals, title.basics)
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
    print("###########################AVANT-DECOMPRESS###########################")
    file_content = gzip.decompress(file_content)
    print("###########################APRES-DECOMPRESS/AVANT-CONVERSION-DF###########################")
    # Read the file into a dataframe
    df = pd.read_csv(BytesIO(file_content), sep='\t')
    print("###########################APRES-CONVERSION-DF/AVANT-SUPPRESSION-COLUMN###########################")
    # List of problematic columns
    bool_cols = ['isOriginalTitle', 'isAdult', 'endYear', 'job', 'endYear', 'characters', 'deathYear']
    # Drop the problematic columns if they exist
    for col in bool_cols:
        if col in df.columns:
            df = df.drop(col, axis=1)
    print("###########################APRES-SUPPRESSION-COLUMN/AVANT-CONVERSION-TABLE###########################")
    table = pa.Table.from_pandas(df)
    buf = BytesIO()
    pq.write_table(table, buf, compression='snappy')
    
    upload_object("formated", "imdb", table_name, f"{table_name}.snappy.parquet", buf.getvalue(), **kwargs)