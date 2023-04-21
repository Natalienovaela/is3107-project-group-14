#!/usr/bin/env python
# coding: utf-8

# To to upgrade the Google API Python client and its dependencies
import time
import re

from google.cloud import bigquery
from google.cloud.bigquery.schema import SchemaField
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
import requests
from airflow.operators.python import PythonOperator
from airflow import DAG
from textwrap import dedent
from datetime import datetime
import json
from googleapiclient.discovery import build
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import os
import nltk
from nltk.corpus import stopwords
nltk.download('words')
nltk.download('punkt')
from nltk.corpus import words

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 4, 17),
    'catchup': False,
    'schedule_interval': '@daily',
}

english_words = set(words.words())
# edit later when migrating to own device
API_KEY = "PUT_YOUR_API_KEY_HERE"
youtube = build('youtube', 'v3', developerKey=API_KEY)

def is_english(text):
    words_list = nltk.word_tokenize(text.lower())
    english_word_count = sum([1 for word in words_list if word in english_words])
    total_word_count = len(words_list)
    english_word_ratio = english_word_count / total_word_count
    return english_word_ratio > 0.5

def get_country_list_inReg(youtube):
        country_ids = []
        request = youtube.i18nRegions().list(
            part="snippet",
            hl="en_US"
        )
        response = request.execute()
        data = response['items']

        for item in data:
            country_ids.append(item["id"])
        return country_ids

# get top category ids based on the top videos in Singapore
def get_top_category_ids_inReg(youtube):
    next_page_token = None
    category_ids = []
    video_id = []
    country_ids = get_country_list_inReg(youtube)

    for country_id in country_ids:
        while True:
            request = youtube.videos().list(
                part="id, snippet",
                chart="mostPopular",
                regionCode=country_id,
                maxResults=50,
                pageToken=next_page_token
            )

            response = request.execute()
            data = response['items']

            for item in data:
                if item["id"] not in video_id and is_english(item["snippet"]["title"]) and item["snippet"]["categoryId"] not in category_ids:
                    category_ids.append(item["snippet"]["categoryId"])
                    video_id.append(item["id"])

            next_page_token = response.get('nextPageToken')

            if not 'nextPageToken' in response.keys():
                break

    return category_ids

# get category list based on the top videos in Singapore
def extract_top_category_data(youtube, **kwargs):
    category_ids = get_top_category_ids_inReg(youtube)
    category_id = []
    title = []

    for category_idss in category_ids:
        request = youtube.videoCategories().list(
            part="id, snippet",
            id=category_idss,
        )

        response = request.execute()
        data = response['items']

        for item in data:
            if item["id"] not in category_id:
                category_id.append(item['id'])
                title.append(item['snippet']['title'])

    category_dataframe = pd.DataFrame(
        {'category_id': category_id, 'title': title})

    category_dataframe = category_dataframe.astype({
        "category_id": str,
        "title": str
    })

    ti = kwargs['ti']
    category_dataframe_json = category_dataframe.to_json(orient='split')
    ti.xcom_push(key='category_dataframe', value=category_dataframe_json)
    print("Category dataframe: ", category_dataframe.shape)
    return category_dataframe_json

def gcp_load_category(**kwargs):
    client = bigquery.Client()

    ti = kwargs['ti']
    category_dataframe = ti.xcom_pull(key='category_dataframe',
                                    task_ids='extract_top_category_data')
    category_df = json.loads(category_dataframe)
    category_df_normalize = pd.json_normalize(
        category_df, record_path=['data'])
    category_df_normalize.columns = ['category_id', 'title']
    print(category_df_normalize)

    table_id = "is3107-project-group-14.is3107_dataset.categories"

    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")

    job = client.load_table_from_dataframe(
        category_df_normalize,
        table_id,
        job_config=job_config
    )

    job.result()
    table = client.get_table(table_id)

    print(
        "Loaded {} rows and {} columns to {}".format(
            table.num_rows, len(table.schema), table_id
        )
    )

with DAG(
    dag_id='category_dag',
    default_args=default_args,
) as dag:

    extract_top_category_task = PythonOperator(
        task_id='extract_top_category_data',
        python_callable=extract_top_category_data,
        op_kwargs={'youtube':youtube},
        dag=dag
    )

    gcp_load_category_task = PythonOperator(
        task_id='gcp_load_category',
        python_callable=gcp_load_category,
        dag=dag
    ) 

    extract_top_category_task >> gcp_load_category_task

