from ..s3.upload_object import upload_object
import requests
"""
this function is fetching data from the imdb api
and store it in s3 bucket (defined in the .env file)
"""

def imdb_fetcher(endpoint_name, **kwargs):
   #endpoint_name must be one of the following:
   #name.basics, title.akas, title.basics, title.crew, title.episode, title.principals, title.ratings
   url = f'https://datasets.imdbws.com/{endpoint_name}.tsv.gz'
   r = requests.get(url, allow_redirects=True)
   upload_object("raw", "imdb", f"{endpoint_name}", f"{endpoint_name}.tsv.gz", r.content, **kwargs)
