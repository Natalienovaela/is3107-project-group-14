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

 # extract the top video in Singapore
def extract_top_video_data(youtube, **kwargs):
    next_page_token = None
    video_id = []
    published_at = []
    channel_id = []
    title = []
    description = []
    tags = []
    category_id = []
    duration = []
    licensed_content = []
    view_count = []
    like_count = []
    favorite_count = []
    comment_count = []

    country_ids = get_country_list_inReg(youtube)

    for country_id in country_ids:
        while True:
            request = youtube.videos().list(
                part="id, snippet, contentDetails, statistics",
                chart="mostPopular",
                regionCode=country_id,
                maxResults=50,
                pageToken=next_page_token
            )

            response = request.execute()
            data = response['items']
            for item in data:
                if item["id"] not in video_id and is_english(item["snippet"]["title"]):
                    channel_id.append(item["snippet"]["channelId"])
                    video_id.append(item["id"])
                    published_at.append(item["snippet"]["publishedAt"])
                    title.append(item["snippet"]["title"])
                    description.append(item["snippet"]["description"])
                    tags.append(item["snippet"].get("tags", []))
                    category_id.append(item["snippet"]["categoryId"])
                    duration.append(item["contentDetails"]["duration"])
                    licensed_content.append(item["contentDetails"]["licensedContent"])
                    view_count.append(item["statistics"].get("viewCount", 0))
                    like_count.append(item["statistics"].get("likeCount", 0))
                    favorite_count.append(item["statistics"]["favoriteCount"])
                    comment_count.append(item["statistics"].get("commentCount", 0))


            next_page_token = response.get('nextPageToken')

            if 'nextPageToken' in response:
                next_page_token = response['nextPageToken']
            else:
                break

    
    video_dataframe = pd.DataFrame({"video_id": video_id, "published_at": published_at, "channel_id": channel_id,
    "title":title, "description": description, "tags": tags, "category_id": category_id, "duration": duration,
    "licensed_content": licensed_content, "view_count": view_count, "like_count": like_count, "favorite_count": favorite_count, 
    "comment_count": comment_count})


    video_dataframe = video_dataframe.astype({
        "video_id": str,
        "published_at": str,
        "channel_id": str,
        "title": str,
        "description": str,
        "tags": object,
        "category_id": str,
        "duration": str,
        "licensed_content": bool,
        "view_count": str,
        "like_count": str,
        "favorite_count": str,
        "comment_count": str
        })

    ti = kwargs['ti']
    video_dataframe_json = video_dataframe.to_json(orient='split')
    ti.xcom_push(key='video_dataframe', value=video_dataframe_json)
    print("Video dataframe: ", video_dataframe.shape)
    return video_dataframe_json

def gcp_load_video(**kwargs):
    client = bigquery.Client()

    ti = kwargs['ti']
    video_dataframe = ti.xcom_pull(task_ids='extract_top_video_data')
    video_df = json.loads(video_dataframe)
    video_df_normalize = pd.json_normalize(video_df, record_path=['data'])
    video_df_normalize.columns = ['video_id', 'published_at', 'channel_id', 'title', 'description','tags','category_id',
    'duration','licensed_content', 'view_count', 'like_count', 'favorite_count', 'comment_count']
    print(video_df_normalize)


    table_id = "is3107-project-group-14.is3107_dataset.videos"

    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")

    job = client.load_table_from_dataframe(
        video_df_normalize,
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
    dag_id='video_dag',
    default_args=default_args,
) as dag:
    
    extract_top_video_task = PythonOperator(
        task_id='extract_top_video_data',
        python_callable=extract_top_video_data,
        op_kwargs={'youtube': youtube},
        dag=dag
    )

    gcp_load_task = PythonOperator(
        task_id='gcp_load',
        python_callable=gcp_load_video,
        dag=dag
    )


    extract_top_video_task >> gcp_load_task

