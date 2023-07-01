import sys
from datetime import datetime, timedelta
from airflow import DAG
from pathlib import Path
from airflow.operators.python import PythonOperator
from dotenv import load_dotenv
sys.path.append(str(Path(__file__).resolve().parent.parent.parent))
from general_package.formate.imdb_fmt import imdb_fmt
from general_package.fetch.imdb_fetcher import imdb_fetcher
load_dotenv(dotenv_path=Path(__file__).resolve().parent.parent.parent / ".env")

with DAG(
        'test_dag',
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
    imdb_args = ['title.akas', 'title.basics', 'title.principals']
    tasks = []

    def create_imdb_fmt_task(arg):
        return lambda **kwargs: imdb_fmt(arg, **kwargs)

    def create_imdb_fetcher_task(arg):
        return lambda **kwargs: imdb_fetcher(arg, **kwargs)

    """task1 = PythonOperator(
        task_id='task1',
        python_callable=create_imdb_fetcher_task('title.basics'),
        provide_context=True,
    )"""

    task2 = PythonOperator(
        task_id='task2',
        python_callable=create_imdb_fmt_task('title.basics'),
        provide_context=True,
    )


    """task1 >> task2"""

    task2

