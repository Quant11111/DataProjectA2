from ..s3.get_object import get_object
from ..s3.upload_object import upload_object

def crew_rating_usage(**kwargs):
    res_title_basics = get_object("formated", "imdb", "title.basics", "title.basics.snappy.parquet", **kwargs)
    res_title_rating = get_object("formated", "imdb", "title.rating", "title.rating.snappy.parquet", **kwargs)
    
    years = range(2000, 2023)

    tmdb_data = []
    for year in years:
        res_top_20 = get_object("formated", "tmdb", f"top_20_{year}", f"top_20_{year}.snappy.parquet", **kwargs)
        tmdb_data.append(res_top_20['Body'])

    """
        TODO :
        crée trois dataframes avec les données de tmdb, title.basics et title.rating puis grace a pyspark 
        effectue une commande sql pour obtenir un nouveau dataframe contenant tout les films de tmdb mais 
        avec les notes de imdb

        tmdb colums names :
            "adult"
            "backdrop_path"
            "genre_ids"
            "id"
            "original_language"
            "original_title"
            "overview"
            "popularity"
            "poster_path"
            "release_date"
            "title"
            "video"
            "vote_average"
            "vote_count"
        
        title.basics columns names :
            "tconst"
            "titleType"
            "primaryTitle"
            "original_title"
            "isAdult"
            "startYear"
            "endYear"
            "runtimeMinutes"
            "genres"

        title.rating columns names :
            "tconst"
            "averageRating"
            "numVotes"

    """
