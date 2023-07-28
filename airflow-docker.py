from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from bs4 import BeautifulSoup
from datetime import datetime
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
import requests
import json
import boto3



default_args = {
    'start_date' : datetime(2020,1,1)
}

query = [
     """create or replace temporary stage imdb_stage
        file_format = (type = json)
        credentials = (
        aws_key_id = '',
        aws_secret_key = ''
        )
        url = 's3://imdb-json-data/imdb.json';
        
        create or replace table movies as
        select index, movie.value as Movies
        from
        (select $1 data
        from @imdb_stage) 
        imdb, 
        lateral flatten(input=>imdb.data['Movies']) as movie;
        
        create or replace table dates as
        select index, date.value as Release_dates
        from
        (select $1 data
        from @imdb_stage) 
        imdb,
        lateral flatten(input=>imdb.data['Release dates']) as date;
        
        create or replace table ratings as
        select index, rating.value as Ratings
        from
        (select $1 data
        from @imdb_stage) 
        imdb,
        lateral flatten(input=>imdb.data['Ratings']) as rating;
        
        create or replace table reviews as
        select index, replace(review.value,',', '') as Reviews
        from
        (select $1 data
        from @imdb_stage) 
        imdb,
        lateral flatten(input=>imdb.data['Reviews']) as review;
        
        select m.Movies, to_varchar(d.release_dates) as Release_dates, cast(r.Ratings as float) as Ratings, cast(re.Reviews as int) as Reviews, (Reviews*0.4+ Ratings*0.6) as Weighted_average
        from movies m
        join dates d
        on m.index = d.index
        join ratings r
        on m.index = r.index
        join reviews re
        on m.index = re.index;"""
]

def scrape():
    page = requests.get('https://www.imdb.com/chart/top/?ref_=nv_mv_250')
    # Send a GET request to the IMDb website's top 250 movies page

    html = BeautifulSoup(page.content, 'html.parser')
    # Create a BeautifulSoup object to parse the HTML content of the page

    movies_list = html.find_all(class_='titleColumn')
    # Find all elements with the class 'titleColumn', which represent the movie titles and release dates

    movies = [movie.find('a').get_text() for movie in movies_list]
    # Extract the movie titles from the 'a' tags within the 'titleColumn' elements

    dates = [movie.find('span').get_text().strip('()') for movie in movies_list]
    # Extract the release dates from the 'span' tags within the 'titleColumn' elements and remove parentheses

    rating_list = html.find_all(class_='ratingColumn imdbRating')
    # Find all elements with the class 'ratingColumn imdbRating', which represent the movie ratings

    ratings_titles = [rating.find('strong')['title'] for rating in rating_list]
    # Extract the rating titles from the 'strong' tags within the 'ratingColumn imdbRating' elements

    ratings = [rating[:3] for rating in ratings_titles]
    # Extract the first three characters from each rating title, representing the movie's rating

    reviews = [review[13:-13] for review in ratings_titles]
    # Extract the review count from each rating title, removing the leading and trailing characters

    file = {
        'Movies' : movies,
        'Release dates' : dates,
        'Ratings' : ratings,
        'Reviews' : reviews
    }
    #Create a dictionary with movie-related information

    json_file = json.dumps(file)
    #Convert the dictionary to a JSON string


    with open(r"/opt/airflow/files/imdb.json", 'w') as file:
        file.write(json_file)
    #Write the JSON string to a file named 'imdb.json'

def upload():
    client = boto3.client('s3', aws_access_key_id = '', region_name = '', aws_secret_access_key='')

    with open(r'/opt/airflow/files/imdb.json','rb') as file:
        client.upload_fileobj(file, 'imdb-json-data', 'imdb.json')


with DAG('airflow_imdb', schedule_interval='@daily', default_args=default_args, catchup=False) as dag:

    task_1 = PythonOperator(
        task_id = 'scraping',
        python_callable = scrape
    )

    task_2 = PythonOperator(
        task_id = 'upload_file',
        python_callable = upload
    )

    task_3 = SnowflakeOperator(
        task_id="create_table",
		sql=query[0],
		snowflake_conn_id="snowflake_conn"
    )

    task_1>>task_2>>task_3
