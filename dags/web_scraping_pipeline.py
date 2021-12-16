import os
import re
import time
import warnings
from datetime import datetime, timedelta
from io import StringIO
from time import perf_counter

import boto3
import pandas as pd
import requests
from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python import PythonOperator, PythonVirtualenvOperator
from airflow.utils.dates import days_ago
from bs4 import BeautifulSoup
from requests import get

BUCKET_NAME = Variable.get("bucket_name")
AWS_REGION = Variable.get("region")
AWS_ACCESS_KEY_ID = Variable.get("key_id")
AWS_SECRET_ACCESS_KEY = Variable.get("secret_key")


def get_page():
    url = "https://ptax.bcb.gov.br/ptax_internet/consultarTodasAsMoedas.do?method=consultaTodasMoedas"
    try:
        user = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.72 Safari/537.36"
        }
        response = requests.get(url, headers=user, timeout=30, verify=False)

    except requests.exceptions.RequestException:
        return None

    bs = BeautifulSoup(response.content, "html.parser")
    string = [s.text for s in bs.find("div", {"align": "center"}).find_all("strong")][0]

    return string


def regex_dates(strings):
    try:
        match = re.search(r"(\d+/\d+/\d+)", strings)
    except:
        match = None
    return match.group()


def read_table():
    url = "https://ptax.bcb.gov.br/ptax_internet/consultarTodasAsMoedas.do?method=consultaTodasMoedas"

    try:
        dataframe = pd.read_html(url, decimal=",", thousands=".")[0]

        return dataframe

    except:
        return None


def scraping_and_process():
    dataframe = read_table()
    strings = get_page()

    dataframe_transformed = dataframe[dataframe.Tipo != "Tipo"]
    dataframe_transformed["Data"] = regex_dates(strings)
    df = dataframe_transformed[
        [
            "Data",
            "Cod Moeda",
            "Tipo",
            "Moeda",
            "Taxa Compra",
            "Taxa Venda",
            "Paridade Compra",
            "Paridade Venda",
        ]
    ]

    df.drop(
        ["Cod Moeda", "Tipo", "Paridade Compra", "Paridade Venda"], axis=1, inplace=True
    )

    df.rename(
        columns={
            "Data": "date",
            "Moeda": "currency",
            "Taxa Compra": "bid",
            "Taxa Venda": "ask",
        },
        inplace=True,
    )

    df["ask"] = pd.to_numeric(df["ask"], downcast="float")
    df["bid"] = pd.to_numeric(df["bid"], downcast="float")

    df["spread"] = df["ask"] - df["bid"]

    csv_buffer = StringIO()
    df.to_csv(path_or_buf=csv_buffer, index=False)

    s3 = boto3.client(
        service_name="s3",
        region_name=AWS_REGION,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )

    s3.put_object(Body=csv_buffer.getvalue(), Bucket=BUCKET_NAME, Key="ptax.csv")

    return df


default_args = {
    "owner": "airflow",
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "admin@localhost.com",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "web_scraping_pipeline",
    start_date=datetime(2021, 1, 1),
    schedule_interval=None,
    default_args=default_args,
    catchup=False,
) as dag:

    get_page_task = PythonOperator(
        task_id="get_page_task",
        python_callable=get_page,
        do_xcom_push=False,
        dag=dag,
    )

    scraping_and_process_data = PythonOperator(
        task_id="scraping_and_process_data",
        python_callable=scraping_and_process,
        do_xcom_push=False,
        dag=dag,
    )

    get_page_task >> scraping_and_process_data
