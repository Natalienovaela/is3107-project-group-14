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

# get top channel ids based on the top videos in Singapore
def get_top_channel_ids_inReg(youtube):
    next_page_token = None
    channel_ids = []
    country_ids = get_country_list_inReg(youtube)
    video_id = []

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
                if item["id"] not in video_id and is_english(item["snippet"]["title"]) and item["snippet"]["channelId"] not in channel_ids:
                    channel_ids.append(item["snippet"]["channelId"])
                    video_id.append(item["id"])

            next_page_token = response.get('nextPageToken')

            if not 'nextPageToken' in response.keys():
                break

    return channel_ids

# extract the top video in Singapore
def extract_top_channel_data(youtube, **kwargs):
    channel_ids = get_top_channel_ids_inReg(youtube)
    channel_id = []
    title = []
    description = []
    custom_url = []
    published_at = []
    thumbnails = []
    view_count = []
    subscriber_count = []
    hidden_subscriber_count = []
    video_count = []

    for channel_idss in channel_ids:
        request = youtube.channels().list(
            part="id, snippet, statistics",
            id=channel_idss,
        )

        response = request.execute()
        data = response['items']

        for item in data:
            if item["id"] not in channel_id:
                channel_id.append(item['id'])
                title.append(item['snippet']['title'])
                description.append(item['snippet']['description'])
                custom_url.append(item['snippet'].get("customUrl", None))
                published_at.append(item['snippet']['publishedAt'])
                thumbnails.append(item['snippet']['thumbnails'])
                view_count.append(item['statistics']['viewCount'])
                subscriber_count.append(item['statistics']['subscriberCount'])
                hidden_subscriber_count.append(
                    item['statistics']['hiddenSubscriberCount'])
                video_count.append(item['statistics']['videoCount'])

    channel_dataframe = pd.DataFrame({'channel_id': channel_id, 'title': title, 'description': description,
                                    'custom_url': custom_url, 'published_at': published_at, 'view_count': view_count,
                                    'subscriber_count': subscriber_count, 'hidden_subscriber_count': hidden_subscriber_count, 'video_count': video_count})

    channel_dataframe = channel_dataframe.astype({
        "channel_id": str,
        "title": str,
        "description": str,
        "custom_url": str,
        "published_at": str,
        "view_count": 'int64',
        "subscriber_count": 'int64',
        "hidden_subscriber_count": bool,
        "video_count": 'int64',
        })

    ti = kwargs['ti']
    channel_dataframe_json = channel_dataframe.to_json(orient='split')
    ti.xcom_push(key='channel_dataframe', value=channel_dataframe_json)
    print("Channel dataframe: ", channel_dataframe.shape)
    return channel_dataframe_json

def gcp_load_channel(**kwargs):
    client = bigquery.Client()

    ti = kwargs['ti']
    channel_dataframe = ti.xcom_pull(task_ids='extract_top_channel_data')
    channel_df = json.loads(channel_dataframe)
    channel_df_normalize = pd.json_normalize(channel_df, record_path=['data'])
    channel_df_normalize.columns = ['channel_id', 'title', 'description', 'custom_url', 'published_at','view_count','subscriber_count',
    'hidden_subscriber_count','video_count']
    print(channel_df_normalize)


    table_id = "is3107-project-group-14.is3107_dataset.channels"

    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")

    job = client.load_table_from_dataframe(
        channel_df_normalize,
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
    dag_id='channel_dag',
    default_args=default_args,
) as dag:

    extract_top_channel_task = PythonOperator(
        task_id='extract_top_channel_data',
        python_callable=extract_top_channel_data,
        op_kwargs={'youtube': youtube},
        dag=dag
    )

    gcp_load_channel_task = PythonOperator(
        task_id='gcp_load_channel',
        python_callable=gcp_load_channel,
        dag=dag
    )


    extract_top_channel_task >> gcp_load_channel_task

