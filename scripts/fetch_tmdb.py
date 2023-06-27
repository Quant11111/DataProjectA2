from dotenv import load_dotenv
import sys
from pathlib import Path
# Ajoute le chemin absolu du répertoire parent au chemin de recherche des modules
sys.path.append(str(Path(__file__).resolve().parent.parent))
load_dotenv( dotenv_path=Path(__file__).resolve().parent.parent / ".env")
from general_package.fetch.tmdb_fetcher import tmdb_fetcher

"""
endpoint_names = ["name.basics", "title.akas", "title.basics", "title.crew", "title.episode", "title.principals", "title.ratings"]

for name in endpoint_names:
    fetcher.fetch_data_from_imdb(endpoint_name= name)
"""

years = [2000, 2001, 2002]

for year in years:
    tmdb_fetcher(year, 1)    #   kwarg possible  : date in order to use the dag execution date (default = None >> date is obtained form datetime)



