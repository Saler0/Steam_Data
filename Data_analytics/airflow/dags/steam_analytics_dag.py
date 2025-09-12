"""
Ejemplo de DAG de Airflow que orquesta el pipeline vía DVC.
Requiere tener DVC y el repo disponible en el worker.
"""
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'steam-analytics',
    'start_date': datetime(2025, 1, 1),
}

with DAG(
    dag_id='steam_analytics_dvc_pipeline',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:
    preagg_reviews = BashOperator(task_id='preagg_reviews', bash_command='dvc repro preagg_reviews')
    preagg_players = BashOperator(task_id='preagg_players', bash_command='dvc repro preagg_players')
    embeddings = BashOperator(task_id='embeddings', bash_command='dvc repro embeddings')
    clustering = BashOperator(task_id='clustering', bash_command='dvc repro clustering')
    events = BashOperator(task_id='events', bash_command='dvc repro events')
    topics = BashOperator(task_id='topics', bash_command='dvc repro topics')
    news = BashOperator(task_id='news', bash_command='dvc repro news_classifier')
    enrich = BashOperator(task_id='enrich', bash_command='dvc repro enrich')
    ccf = BashOperator(task_id='ccf', bash_command='dvc repro ccf')
    report = BashOperator(task_id='report', bash_command='dvc repro report editor_view')

    [preagg_reviews, preagg_players] >> embeddings >> clustering
    clustering >> [events, ccf]
    events >> topics >> news >> enrich
    [enrich, ccf] >> report

