# here are the dependencies to install into the airflow_venv so the project works properly:


pip install python-dotenv
pip install boto3
pip install pyarrow
pip install pandas
pip install 'apache-airflow==2.6.2' \
 --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.6.2/constraints-3.10.txt"
# warning: replace 3.10 with your python version

# .env 

TMDB_API_KEY = "" 
TMDB_API_ACCESS_TOKEN = ""

AWS_ACCESS_KEY_ID= "" 
AWS_SECRET_ACCESS_KEY=""

BUCKET_NAME = "best-french-movies-tmdb"    
# this is the name (bad name choosen before knowing the meaning of "bucket") of the s3 bucket i use. replace it with your own and add your aws credentials

# before running airflow standalone
export AIRFLOW_HOME=~/isep/data_module/airflow_project/airflow 
# replace this path with the absolute path of your airflow home directory
