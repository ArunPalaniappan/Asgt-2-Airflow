from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin

import random
import subprocess
import os
import zipfile

default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

def fetch_links(base_url, year):
    url = f"{base_url}{year}/"

    response = requests.get(url)

    base_url_year = base_url + year + '/'
    list_of_links = []
    if response.status_code == 200:
        soup = BeautifulSoup(response.content, 'html.parser')
        links = soup.find_all('a')
        
        for link in links:
            href = link.get('href')
            if href:
                absolute_url = urljoin(base_url_year, href)
                if ".csv" in absolute_url:
                    list_of_links.append(absolute_url)

        return list_of_links

base_url = "https://www.ncei.noaa.gov/data/local-climatological-data/access/"
year = "2023"
no_of_links = 5
folder = "/home/arun/airflow/dags/fetched_files/" + year

def random_links(list_of_links, no_of_links):
    print(list_of_links)
    links_to_fetch = random.sample(eval(list_of_links), no_of_links)
    #links_to_fetch = eval(list_of_links)[:2]
    return links_to_fetch

def download_files(links_to_fetch, folder):
    print(links_to_fetch)
    print(folder)
    os.makedirs(folder, exist_ok=True)
        
    for url in eval(links_to_fetch):
        print(url)
        subprocess.run(["wget", url, "-P", folder])

def zip_files(year):
    folder_path = f"/home/arun/airflow/dags/fetched_files/{year}/"
    zip_folder_path = f"/home/arun/airflow/dags/zipped_files/{year}/"
    zip_file_name = f"{year}_files.zip"
    
    zip_file_path = os.path.join(zip_folder_path, zip_file_name)

    if not os.path.exists(zip_folder_path):
        os.makedirs(zip_folder_path)
    
    files = [os.path.join(folder_path, f) for f in os.listdir(folder_path) if os.path.isfile(os.path.join(folder_path, f))]
    
    with zipfile.ZipFile(zip_file_path, 'w') as zipf:
        for file in files:
            arcname = os.path.relpath(file, folder_path)
            zipf.write(file, arcname=arcname)

with DAG('fetch_files', 
         default_args=default_args,
         start_date=datetime(2024, 3, 1, 9),
        schedule='*/2 * * * *',
         catchup=False) as dag:

    fetch_links_obj = PythonOperator(
        task_id='fetch_links',
        python_callable=fetch_links,
        op_kwargs={'base_url':base_url, 'year':year}
    )

    random_links_obj = PythonOperator(
        task_id='random_links',
        python_callable=random_links,
        op_kwargs={'list_of_links':"{{ ti.xcom_pull(task_ids='fetch_links') }}",'no_of_links':no_of_links}
    )

    download_files_obj = PythonOperator(
        task_id='download_files',
        python_callable=download_files,
        op_kwargs={'links_to_fetch':"{{ ti.xcom_pull(task_ids='random_links') }}",'folder':folder}
    )

    zip_files_obj = PythonOperator(
        task_id='zip_files',
        python_callable=zip_files,
        op_kwargs={'year':year}
    )

    fetch_links_obj >> random_links_obj >> download_files_obj >> zip_files_obj




























