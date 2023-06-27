from ..s3.get_object import get_object
from ..s3.upload_object import upload_object
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
from io import BytesIO
import json

def tmdb_fmt(year, page_nbr, **kwargs):
    date = kwargs['execution_date'].strftime("%Y%m%d")
    response = get_object("raw", "tmdb", f"top_{page_nbr*20}", f"top_{page_nbr*20}_{year}.json" , date)
    file_content = response['Body'].read()
    # Lire le fichier json
    data = json.loads(file_content)
    df = pd.DataFrame(data) # Converting JSON to pandas DataFrame
    
    # Convertir le dataframe en parquet
    table = pa.Table.from_pandas(df)
    buf = BytesIO()
    pq.write_table(table, buf, compression='snappy')

    upload_object("formated", "tmdb", f"top_{page_nbr*20}", f"top_{page_nbr*20}_{year}.snappy.parquet", buf.getvalue(), **kwargs)





"""if kwargs['execution_date']!= None:
        date = kwargs['execution_date'].strftime("%Y%m%d")
    else:
        date = "20230611" #date menant a de la data pour page_nbr = 1 et year = 2001"""