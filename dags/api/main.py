from airflow import DAG
import pendulum
from datetime import datetime, timedelta
from api.video_stats import get_playlists_id, get_video_ids, extract_video_data, save_to_json

# Define the local timezone
local_tz = pendulum.timezone("Europe/Warsaw")

#Default Args
default_args = {
    'owner': 'dataengineers',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'email': "data@engineers.com",
    # 'retries': 1,
    # 'retry_delay': timedelta(minutes=5),
    'max_active_runs':1,
    'dagrun_timeout': timedelta(hours=1),
    'start_date': datetime(2026, 1, 1, tzinfo=local_tz)
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}

with DAG(
    dag_id ='produce_json',
    default_args=default_args,
    description='DAG to produce JSON file with raw data',
    schedule='0 14 * * *',
    catchup=False
) as dag:
    
    # Define tasks
    playlist_id = get_playlists_id()
    video_ids = get_video_ids(playlist_id)
    extract_data = extract_video_data(video_ids)
    save_to_json_task = save_to_json(extract_data)

    # Define dependencies
    playlist_id >> video_ids >> extract_data >> save_to_json_task