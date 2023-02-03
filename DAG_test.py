from datetime import datetime
from pymongo import MongoClient
from suds.client import Client

from airflow import DAG
from airflow.decorators import task
from airflow.operators.python_operator import PythonOperator

import socket

def get_ip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        # n'importe quelle adresse IP externe accessible fera l'affaire
        s.connect(('10.255.255.255', 1))
        IP = s.getsockname()[0]
    except:
        IP = '127.0.0.1'
    finally:
        s.close()
    return IP

def get_id():
    client = MongoClient(f"mongodb://{get_ip()}:27017")
    db = client["SOA_TD2"]
    coll = db["tweets"]
    ids = []

    tweets = coll.find({})

    for tweet in tweets:
        ids.append(tweet["_id"])

    client.close()
    return ids


def get_author(ti):
    ids = ti.xcom_pull(task_ids='get_id')

    authors = []

    for id_tweet in ids:
        Get_Author_Service_client = Client(f"http://{get_ip()}:8000/getAuthorService?wsdl")
        authors.append(Get_Author_Service_client.service.get_author(id_tweet))

    return authors


def get_hashtags(ti):
    ids = ti.xcom_pull(task_ids='get_id')

    clean_hashtags = []

    for id_tweet in ids:
        Get_Hashtags_Service_client = Client(f"http://{get_ip()}:8000/getHashtagsService?wsdl")
        hashtags = Get_Hashtags_Service_client.service.get_hashtags(id_tweet)
        array_hashtags = [hashtag for hashtag in hashtags]
        clean_hashtags.append(array_hashtags[0][1] if len(array_hashtags) != 0 else None)

    return clean_hashtags


def get_sentiments(ti):
    ids = ti.xcom_pull(task_ids='get_id')

    sentiments = []

    for id_tweet in ids:
        Get_Sentiment_Service_client = Client(f"http://{get_ip()}:8000/getSentimentService?wsdl")
        sentiments.append(Get_Sentiment_Service_client.service.get_sentiment(id_tweet))
    
    return sentiments


def get_topics(ti):
    ids = ti.xcom_pull(task_ids='get_id')

    clean_topics = []

    for id_tweet in ids:
        Get_Topics_Service_client = Client(f"http://{get_ip()}:8000/getTopicsService?wsdl")
        topics = Get_Topics_Service_client.service.get_topics(id_tweet)
        clean_topics.append([topic for topic in topics][0][1])
        
    return clean_topics


def data_processing(ti):
    ids = ti.xcom_pull(task_ids='get_id')
    authors = ti.xcom_pull(task_ids='get_author')
    hashtags = ti.xcom_pull(task_ids='get_hashtags')
    sentiments = ti.xcom_pull(task_ids='get_sentiments')
    topics = ti.xcom_pull(task_ids='get_topics')

    for index, id_tweet in enumerate(ids):
        data = {'_id': id_tweet, 'author': authors[index], 'hashtags': hashtags[index], 'sentiment': sentiments[index],
                'topics': topics[index]}

        client = MongoClient(f"mongodb://{get_ip()}:27017")
        db = client["SOA_TD2"]
        coll = db["tweets_processing"]

        if coll.find_one({"_id": id_tweet}) is None:
            coll.insert_one(data)

        client.close()
        

# A DAG represents a workflow, a collection of tasks
with DAG(dag_id="demo", start_date=datetime(2022, 1, 1), schedule="0 0 * * *") as dag:

    # Define a task for get_id()
    get_id_task = PythonOperator(task_id='get_id', python_callable=get_id)

    # Define a task for get_author()
    get_authors_task = PythonOperator(task_id='get_author', python_callable=get_author)

    # Define a task for get_hashtags()
    get_hashtags_task = PythonOperator(task_id='get_hashtags', python_callable=get_hashtags)

    # Define a task for get_sentiments()
    get_sentiments_task = PythonOperator(task_id='get_sentiments', python_callable=get_sentiments)

    # Define a task for get_topics()
    get_topics_task = PythonOperator(task_id='get_topics', python_callable=get_topics)

    # Define a task for data_processing()
    data_processing_task = PythonOperator(task_id='data_processing', python_callable=data_processing)

    # Set dependencies between tasks
    get_id_task >> get_authors_task >> get_hashtags_task >> get_sentiments_task >> get_topics_task >> data_processing_task
