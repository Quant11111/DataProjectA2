from ..s3.get_object import get_object
from ..s3.upload_object import upload_object

def crew_rating_usage(**kwargs):
    response = get_object("formated", "imdb", "title.crew", "title.crew.snappy.parquet", **kwargs)
    crew_content =response['Body'].read()
    response = get_object("formated", "imdb", "title.rating", "title.rating.snappy.parquet", **kwargs)
    rating_content =response['Body'].read()

    "todo"

    upload_object("usage", "imdb", "crew_rating", "crew_rating.snappy.parquet", "todo", **kwargs)