import sys
from datetime import datetime, timedelta
from airflow import DAG
from pathlib import Path
from airflow.operators.python import PythonOperator
from dotenv import load_dotenv
sys.path.append(str(Path(__file__).resolve().parent.parent.parent))
from general_package.fetch.imdb_fetcher import imdb_fetcher
from general_package.fetch.tmdb_fetcher import tmdb_fetcher
from general_package.formate.imdb_fmt import imdb_fmt
from general_package.formate.tmdb_fmt import tmdb_fmt
load_dotenv(dotenv_path=Path(__file__).resolve().parent.parent.parent / ".env")

with DAG(
        'dag_project',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(seconds=15),
        },
        description='Amy data project',
        schedule_interval=None,
        start_date=datetime(2021, 1, 1),
        catchup=False,
        tags=['tmdb', 'imdb', 'data', 'spark', 'elastic', 'kibana'],
) as dag:
    dag.doc_md = """
        This is my first DAG in airflow.
        I can write documentation in Markdown here with **bold text** or __bold text__.
    """
    # issue with title.basic title.akas title.principals
    imdb_args = ['title.basics', 'title.akas', 'title.crew', 'title.episode', 'title.principals', 'title.ratings']
    tasks = []
    number_of_pages = 1

    def create_tmdb_fetcher_task(year):
        return lambda **kwargs: tmdb_fetcher(year, number_of_pages, **kwargs)

    def create_tmdb_fmt_task(year):
        return lambda **kwargs: tmdb_fmt(year, number_of_pages, **kwargs)

    def create_imdb_fetcher_task(arg):
        return lambda **kwargs: imdb_fetcher(arg, **kwargs)

    def create_imdb_fmt_task(arg):
        return lambda **kwargs: imdb_fmt(arg, **kwargs)

    for year in range(2000, 2023):
        task = PythonOperator(
            task_id=f'tmdbFetch-{year}',
            python_callable=create_tmdb_fetcher_task(year),
            provide_context=True,
        )
        tasks.append(task)
        task = PythonOperator(
            task_id=f'tmdbFmt-{year}',
            python_callable=create_tmdb_fmt_task(year),
            provide_context=True,
        )
        tasks.append(task)
        tasks[len(tasks)-2] >> tasks[len(tasks)-1]

    for arg in imdb_args:
        task = PythonOperator(
            task_id=f'imdbFetch-{arg}',
            python_callable=create_imdb_fetcher_task(arg),
            provide_context=True,
        )
        tasks.append(task)
        task = PythonOperator(
            task_id=f'imdbFmt-{arg}',
            python_callable=create_imdb_fmt_task(arg),
            provide_context=True,
        )
        tasks.append(task)
        tasks[len(tasks)-2] >> tasks[len(tasks)-1]

