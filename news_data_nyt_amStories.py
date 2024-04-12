# -*- coding: utf-8 -*-
"""
Created on Thu Apr 11 18:17:03 2024

@author: Gowtham S
"""
import pickle
import time
import requests

#### Base Code

api_key = 'API_KEY' # Fill the API key here

start = time.time()
for year in range(1928, 2024):  # Adjusted to include up to 2023
    print(f'---------------- {year} Start ----------------') # Just some indicator
    articles = []
    for month in range(1, 13): # Each month of every year
        query_url = f"https://api.nytimes.com/svc/archive/v1/{year}/{month}.json?api-key={api_key}"
        response = requests.get(query_url).json()
        articles.extend(response['response']['docs']) # Gets the article of that month
        print(f'--------{year}---Month:{month}------------')
        time.sleep(13)
    # Adjusted to save at the end of the range or when a decade ends
    if year % 10 == 8 or year == 2023:
        with open(f'articles_{year - (year % 10) + 1}_to_{year}.pkl', 'wb') as f:
            pickle.dump(articles, f)
        print(f'---------------- Data saved for {year - (year % 10) + 1} to {year} ----------------')
    print(f'---------------- {year} End ----------------')
end = time.time()
print('--' * 70)
print(f'Total Time taken: {end - start}')


####### American Stories Dataset

from datasets import load_dataset

#  Download data for the year 1809 at the associated article level (Default)
datasets = load_dataset("dell-research-harvard/AmericanStories",
    "subset_years",
    year_list=["1939", "1942", "1945", "1948",  "1955"]
)

####### Parallelization

import pickle
import time
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

# Your API keys
api_keys = ['API_KEY_1', 'API_KEY_2']

def fetch_articles(start_year, end_year, api_key):
    """
    Parameters
    ----------
    start_year : The year that is given below, it starts from 1850s (See Doc).
    
    end_year : The End Year of extraction
    
    api_key : The API Key from New York

    Returns
    -------
    None But saves files in the pickle which is faster to save as external data than CSV
    Does not max out the CPU file size.

    """
    all_articles = []
    for year in range(start_year, end_year + 1):
        print(f'---------------- {year} Start ----------------')
        for month in range(1, 13):
            query_url = f"https://api.nytimes.com/svc/archive/v1/{year}/{month}.json?api-key={api_key}"
            response = requests.get(query_url).json()
            all_articles.extend(response['response']['docs'])
            print(f'--------{year}---Month:{month}------------')
            time.sleep(13)  # Sleep to avoid hitting rate limits
        if year % 10 == 8 or year == end_year:
            with open(f'articles_{year - (year % 10) + 1}_to_{year}.pkl', 'wb') as f:
                pickle.dump(all_articles, f)
            print(f'---------------- Data saved for {year - (year % 10) + 1} to {year} ----------------')
        print(f'---------------- {year} End ----------------')
    return f'Completed fetching from {start_year} to {end_year}'

def main():
    start = time.time()
    # Define year ranges for each API key
    ranges = [(1928, 1978), (1979, 2023)]
    with ThreadPoolExecutor(max_workers=2) as executor:
        # Parallelization: Map the executor to the fetch_articles function with different ranges and API keys
        futures = {executor.submit(fetch_articles, r[0], r[1], api_keys[i]): i for i, r in enumerate(ranges)}
        for future in as_completed(futures):
            print(future.result())
    
    end = time.time()
    print('--' * 70)
    print(f'Total Time taken: {end - start}')

if __name__ == "__main__":
    main()




