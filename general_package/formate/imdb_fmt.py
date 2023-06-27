from ..s3.get_object import get_object
from ..s3.upload_object import upload_object
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
from io import BytesIO
import gzip
#import datetime



def imdb_fmt(table_name, **kwargs):
    date = kwargs['execution_date'].strftime("%Y%m%d")
    response = get_object("raw", "imdb", table_name, f"{table_name}.tsv.gz", date)
    file_content =response['Body'].read()
    file_content = gzip.decompress(file_content)
    df = pd.read_csv(BytesIO(file_content), sep='\t')
    table = pa.Table.from_pandas(df)
    buf = BytesIO()
    pq.write_table(table, buf, compression='snappy')
        
    upload_object("formated", "imdb", table_name, f"{table_name}.snappy.parquet", buf.getvalue(), **kwargs)


"""
if kwargs.get("date") is None:
        date = datetime.datetime.now().strftime("%Y%m%d")
    else:
        date = kwargs.get("date")
"""

"""
if 'isAdult' in df.columns: 
        # cherche les valeurs non numeriques et les change en NaN
        # puis remplace les NaN par 0 puis converti la colone en int
        df['isAdult'] = pd.to_numeric(df['isAdult'], errors='coerce')
        df['isAdult'].fillna(0, inplace=True)
        df['isAdult'] = df['isAdult'].astype(int)
"""