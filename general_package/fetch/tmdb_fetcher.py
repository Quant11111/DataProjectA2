import os
import json
import requests
from ..s3.upload_object import upload_object

"""
this function is fetching the top french movies
released during a specific year and store them in a s3 bucket
the number of pages to fetch is a parameter that can be changed
for each page added we fetch 20 movies

Args:
        year (int): released year.
        number_of_pages (int): number of pages to fetch (page1 = 20 best, page2 = the next 20,...).
"""

def tmdb_fetcher(year, number_of_pages, **kwargs):
    result = get_movies_by_year_tmdb(year, 1)
    for i in range(2, number_of_pages):
        result += get_movies_by_year_tmdb(year, i) #loop that all pages between 2 and number_of_pages and add them to the result
    object = json.dumps(result).encode()            #convert the result to json and encode it  
    upload_object("raw","tmdb",f"top_{number_of_pages * 20}",f"top_{number_of_pages*20}_{year}.json", object, **kwargs) 

TOKEN = os.environ.get("TMDB_API_ACCESS_TOKEN")


#request function
def get_movies_by_year_tmdb(year, page):
    url = f"https://api.themoviedb.org/3/discover/movie?include_adult=false&include_video=false&page={page}&primary_release_year={year}&sort_by=popularity.desc&with_original_language=fr"
    headers = {
       "accept": "application/json",
       "Authorization": "bearer " + str(TOKEN),
    }

    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        data = response.json().get("results")
        return data
    else:
        print("An error occurred while fetching the data.")
        return None


