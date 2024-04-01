from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.contrib.sensors.file_sensor import FileSensor

import random
import subprocess
import os
import zipfile
import json
import re

import geopandas as gpd
import geodatasets as gds
import matplotlib.pyplot as plt

from shapely.geometry import Point
from geopandas import GeoDataFrame
import matplotlib.gridspec as gridspec
import seaborn as sns

import pandas as pd
import numpy as np

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import apache_beam.runners.interactive.interactive_beam as ib

#---------------------------------------------------------------------------------------------#

default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

#---------------------------------------------------------------------------------------------#

def tuple_dataframe(df):
    df = pd.read_csv(df)
    
    df_temp = df[['LATITUDE','LONGITUDE','HourlyDryBulbTemperature','HourlyWindSpeed']]
    df_temp = df_temp.apply(pd.to_numeric,errors='coerce')
    df_temp = df_temp.fillna(df_temp.mean())
    df_temp['STATION'] = df['STATION']
    df_temp['DATE'] = pd.to_datetime(df['DATE']).dt.month
    
    tuple_list = []
    for i in range(len(df_temp)):
        month = df_temp.iloc[i]['DATE']
        lat = df_temp.iloc[i]['LATITUDE']
        lon = df_temp.iloc[i]['LONGITUDE']
        dry = df_temp.iloc[i]['HourlyDryBulbTemperature']
        wind = df_temp.iloc[i]['HourlyWindSpeed']
        tuple_list.append((month, lat, lon, dry, wind))

    return tuple_list

def compute_monthly_average(tuple_list):
    data_df = pd.DataFrame(tuple_list, columns=['month', 'latitude', 'longitude', 'HourlyDryBulbTemperature', 'HourlyWindSpeed'])
    
    avg_df = data_df.groupby(['month', 'latitude', 'longitude']).agg({'HourlyDryBulbTemperature': 'mean', 'HourlyWindSpeed': 'mean'}).reset_index()
    
    average_data = []
    for i in range(len(avg_df)):
        month = avg_df.iloc[i]['month']
        lat = avg_df.iloc[i]['latitude']
        lon = avg_df.iloc[i]['longitude']
        dry = avg_df.iloc[i]['HourlyDryBulbTemperature']
        wind = avg_df.iloc[i]['HourlyWindSpeed']
        
        average_data.append([month, lat, lon, dry, wind])
    
    return json.dumps(average_data)

def beam_pipeline(unzip_path): 
    with beam.Pipeline(runner='DirectRunner') as pipeline:
        csv_files = [os.path.join(unzip_path,file) for file in os.listdir(unzip_path) if file.endswith('.csv')]
        result = (
            pipeline 
            | beam.Create(csv_files)
            | beam.Map(tuple_dataframe)
            | beam.Map(compute_monthly_average)
            | beam.io.WriteToText('/home/arun/airflow/dags/average_tuple_data.txt')
        )

def plot_temp_wind(tuple_text_path, images_folder):
    average_data_list = []
    with open(tuple_text_path, 'r') as file:
        for line in file:
            data = json.loads(line)
            average_data_list.extend(data)
    
    df = pd.DataFrame(average_data_list, columns=['month', 'latitude', 'longitude', 'DryBulbTemperature', 'WindSpeed'])
    
    for i in range(1,13):
        dfi = df[df['month']==i]
        geometry = [Point(xy) for xy in zip(dfi['longitude'], dfi['latitude'])]
        gdf = GeoDataFrame(dfi, geometry=geometry)   
        
        plt.figure(dpi=600)
        world = gpd.read_file(gpd.datasets.get_path('naturalearth_lowres'))
        vmin = df['DryBulbTemperature'].min()
        vmax = df['DryBulbTemperature'].max()
        norm = plt.Normalize(vmin=vmin, vmax=vmax)
        cmap = plt.get_cmap('coolwarm')
        sm = plt.cm.ScalarMappable(cmap=cmap, norm=norm)
        sm.set_array([])
        gdf.plot(ax=world.plot(color='white', edgecolor='black', figsize=(20, 20)),    marker='o', column='DryBulbTemperature', markersize=500, alpha=0.95, cmap=cmap, norm=norm)
        cbar = plt.colorbar(sm, ax=plt.gca(), orientation='vertical', pad=0.02, shrink=0.5)
        cbar.set_label('Dry Bulb Temperature', fontsize=20)
        cbar.ax.tick_params(labelsize=20)
        plt.title(f'Average Dry Bulb Temperature - month-{i}', fontsize=30)
        plt.savefig(f'{images_folder}/Average_Dry_Bulb_Temperature-month-{i}.png')
        
        plt.figure(dpi=600)
        vmin = df['WindSpeed'].min()
        vmax = df['WindSpeed'].max()
        norm = plt.Normalize(vmin=vmin, vmax=vmax)
        cmap = plt.get_cmap('viridis')
        sm = plt.cm.ScalarMappable(cmap=cmap, norm=norm)
        sm.set_array([])
        gdf.plot(ax=world.plot(color='white', edgecolor='black', figsize=(20, 20)), marker='o', column='WindSpeed', markersize=500, alpha=0.95, cmap=cmap, norm=norm)
        cbar = plt.colorbar(sm, ax=plt.gca(), orientation='vertical', pad=0.02, shrink=0.5)
        cbar.set_label('Wind Speed', fontsize=20)
        cbar.ax.tick_params(labelsize=20)
        plt.title(f'Average Wind Speed - month-{i}', fontsize=30)
        plt.savefig(f'{images_folder}/Average_Wind_Speed-month-{i}.png')

#---------------------------------------------------------------------------------------------#

zip_path="/home/arun/airflow/dags/zipped_files/2023/2023_files.zip"
unzip_path="/home/arun/airflow/dags/unzipped_files"

tuple_text_path='/home/arun/airflow/dags/average_tuple_data.txt-00000-of-00001'
images_folder = '/home/arun/airflow/dags/images_folder'
videos_folder = '/home/arun/airflow/dags/videos_folder'

with DAG('Visualization_DAG', 
         default_args=default_args,
         start_date=datetime(2024, 3, 1, 9),
         schedule='*/3 * * * *',
         catchup=False) as dag:

    zipfile_check=FileSensor(
        task_id="zipfile_check",
        filepath=zip_path,
        timeout=5,
        poke_interval=1
    )

    unzip_task=BashOperator(
        task_id='unzip_archive',
        bash_command=f'unzip -o {zip_path} -d {unzip_path}',
        trigger_rule='all_success'
    )

    average_data_tuple=PythonOperator(
        task_id="beam_pipeine",
        python_callable=beam_pipeline,
        op_kwargs={"unzip_path":unzip_path}
    )

    plot_data=PythonOperator(
        task_id="plot_temp_wind",
        python_callable=plot_temp_wind,
        op_kwargs={"tuple_text_path":tuple_text_path, "images_folder":images_folder}
    )

    animation_video=BashOperator(
        task_id='animation_video',
        bash_command=f'ffmpeg -framerate 1 -i {images_folder}/Average_Dry_Bulb_Temperature-month-%d.png -c:v libx264 -preset slow -crf 20 {videos_folder}/Animation_Dry_Bulb_Temperature.mp4 ; ffmpeg -framerate 1 -i {images_folder}/Average_Wind_Speed-month-%d.png -c:v libx264 -preset slow -crf 20 {videos_folder}/Average_Wind_Speed.mp4'
    )  

    delete_csv_files=BashOperator(
        task_id="delete_csv_files",
        bash_command=f'rm -r {unzip_path}',
        trigger_rule='all_success'
    )

    zipfile_check >> unzip_task >> average_data_tuple >> plot_data >> animation_video >> delete_csv_files
























