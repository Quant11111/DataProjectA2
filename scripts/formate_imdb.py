from dotenv import load_dotenv
import sys
from pathlib import Path
# Ajoute le chemin absolu du répertoire parent au chemin de recherche des modules
sys.path.append(str(Path(__file__).resolve().parent.parent))
load_dotenv( dotenv_path=Path(__file__).resolve().parent.parent.parent / "config/.env")
from general_package.formate.imdb_fmt import imdb_fmt

imdb_fmt("title.basics", execution_date="2023-06-11T00:00:00+00:00")
