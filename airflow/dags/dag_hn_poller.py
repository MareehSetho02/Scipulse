"""HN toutes les cinq minutes : Bronze immuable puis Silver, sans écriture ES directe."""
from datetime import datetime,timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
with DAG('scipulse_hn_poller',start_date=datetime(2026,1,1),schedule='*/5 * * * *',catchup=False,max_active_runs=1,default_args={'owner':'scipulse','retries':2,'retry_delay':timedelta(minutes=1)},tags=['scipulse','bronze','silver']) as dag:
    bronze=BashOperator(pool='scipulse_medallion',task_id='bronze_hn',bash_command='cd /opt/airflow && python -c "from src.pipeline.bronze import hn; hn(limit=100,history_pages=0)"')
    silver=BashOperator(pool='scipulse_medallion',task_id='silver_hn',bash_command='cd /opt/airflow && python -m src.pipeline.silver hn')
    monitoring=BashOperator(pool='scipulse_medallion',task_id='refresh_monitoring',trigger_rule='all_done',bash_command='cd /opt/airflow && python -m src.pipeline.dashboards --monitoring-only')
    bronze >> silver >> monitoring
