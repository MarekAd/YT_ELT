from airflow import DAG
from pendulum import timezone
from datetime import datetime, timedelta
from api.video_stats import get_playlists_id, get_video_ids, extract_video_data, save_to_json
from datawarehouse.dwh import staging_table, core_table

local_tz = timezone("Europe/Warsaw")

default_args = {
    'owner': 'dataengineers',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'email': "data@engineers.com",
    'max_active_runs': 1,
    'dagrun_timeout': timedelta(hours=1),
    'start_date': datetime(2026, 1, 1, tzinfo=local_tz)
}

with DAG(
    dag_id='produce_json',
    default_args=default_args,
    description='DAG to produce JSON file with raw data',
    schedule='0 14 * * *',
    catchup=False
) as dag:

    # Tworzymy taski TaskFlow
    playlist_task = get_playlists_id()
    videos_task = get_video_ids(playlist_task)
    extract_task = extract_video_data(videos_task)
    save_task = save_to_json(extract_task)

    # Definiujemy zależności
    playlist_task >> videos_task >> extract_task >> save_task


with DAG(
    dag_id ='update_db',
    default_args=default_args,
    description='DAG to process JSON file and insert data into both staging and core shemas',
    schedule='0 15 * * *',
    catchup=False
) as dag:
    
    # Define tasks
    update_staging = staging_table()
    update_core = core_table()

    # Define dependencies
    update_staging >> update_core