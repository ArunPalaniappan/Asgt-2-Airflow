from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import random
import os
import zipfile
import urllib.request
from inscriptis import get_text
import random
import subprocess
import os
from glob import glob

# Define DAG arguments
default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# Base URL and year

# # Function to fetch location wise datasets for the given year
# def fetch_location_datasets(year, **kwargs):
#     url = base_url + year
#     response = requests.get(url)
#     datasets = response.text.split('\n')
#     return datasets

# # Function to select random data files from available list
# def select_random_files(datasets, num_files, **kwargs):
#     selected_files = random.sample(datasets, num_files)
#     return selected_files

# # Function to fetch individual data files
# def fetch_data_files(files, **kwargs):
#     for file in files:
#         os.system(f"wget {file}")

# # Function to zip the data files into an archive
# def zip_files(files, **kwargs):
#     with zipfile.ZipFile('archive.zip', 'w') as zipf:
#         for file in files:
#             zipf.write(file)

# # Function to place the archive at a required location
# def move_archive(**kwargs):
#     os.system("mv archive.zip /path/to/required/location/")


def get_all_links():
    years = list(range(2023, 2024))
    all_links = []
    for year in years:
        baseurl = f"https://www.ncei.noaa.gov/data/local-climatological-data/access/" + str(year) + "/"
        html = urllib.request.urlopen(baseurl).read().decode('utf-8')
        text = get_text(html)
        for i in range(len(text)-15):
            if ".csv" in text[i+11:i+15]:
                # filename = wget.download(url+text[i:i+15])
                url = baseurl + text[i:i+15]
                # subprocess.run(f"wget {url} -P downloads/{year}/",shell=True)
                all_links.append(url)
    return all_links

def select_urls(csvs_list):
    return random.sample(csvs_list, 5)

def fetch_csvs_from_urls(list_of_urls):
    for url in list_of_urls:
        subprocess.run(f"wget {url} -P downloads/2023/", shell=True)

def zipdir(path, ziph):
    # ziph is zipfile handle
    for root, dirs, files in os.walk(path):
        for file in files:
            ziph.write(os.path.join(root, file), 
                       os.path.relpath(os.path.join(root, file), 
                                       os.path.join(path, '..')))

def zipallfiles_and_placeatrequiredlocation():
    os.makedirs("downloads/", exist_ok=True)
    zipfilename = "download_2023_csvs.zip"
    with zipfile.ZipFile(zipfilename, 'w', zipfile.ZIP_DEFLATED) as zipf:
        zipdir("downloads/", zipf)

    required_location = "final_location/"
    subprocess.run([f"mv download_2023_csvs.zip {required_location}"], shell=True)

# print("Code Started")
# all_links = get_all_links()
# print("All Links Fetched")
# sampled_links = select_urls(all_links)
# print("Selection of URLs Completed")
# fetch_csvs_from_urls(sampled_links)
# print("Downloaded the Sampled URLs")

# # Zipping all CSVS

# zipallfiles_and_placeatrequiredlocation()
# print("Zipped and Moved to a location")
    

# # Define the DAG
with DAG('datapipeline_Take4', 
         default_args=default_args,
         start_date=datetime(2024, 3, 1, 4),
         schedule='*/2 * * * *',
         catchup=False) as dag:

    fetch_datasets = PythonOperator(
        task_id='get_all_links',
        python_callable=get_all_links,
        
        
    )

    select_files = PythonOperator(
        task_id='select_urls',
        python_callable=select_urls,
        op_kwargs={'csvs_list':"{{ ti.xcom_pull(task_ids='get_all_links') }}"}
    )

    fetch_files = PythonOperator(
        task_id='fetch_csvs_from_urls',
        python_callable=fetch_csvs_from_urls,
        op_kwargs={'list_of_urls':"{{ ti.xcom_pull(task_ids='select_urls') }}"}
    )

    zip_files_task = PythonOperator(
        task_id='zipallfiles_and_placeatrequiredlocation',
        python_callable=zipallfiles_and_placeatrequiredlocation,
    )

    fetch_datasets >> select_files >> fetch_files >> zip_files_task