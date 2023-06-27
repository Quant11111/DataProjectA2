from dotenv import load_dotenv
import sys
from pathlib import Path
# Ajoute le chemin absolu du répertoire parent au chemin de recherche des modules
sys.path.append(str(Path(__file__).resolve().parent.parent))
load_dotenv( dotenv_path=Path(__file__).resolve().parent.parent.parent / "config/.env")
from general_package.formate.tmdb_fmt import tmdb_fmt

tmdb_fmt(2001, 2) #default date = "20230611" this data does not exist
tmdb_fmt(2001, 2, execution_date="20210612") #this data exists
tmdb_fmt(2001, 1) #this data exist