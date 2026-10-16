"""Orchestration réelle Bronze -> Silver -> dbt -> Spark -> Gold -> Kibana."""
from datetime import datetime,timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
BASE='cd /opt/airflow && '
SPARK='/opt/spark/bin/spark-submit --master spark://spark:7077 --deploy-mode client --driver-memory 1g --executor-memory 768m --executor-cores 1 --total-executor-cores 2 --conf spark.driver.host=airflow-scheduler --conf spark.driver.bindAddress=0.0.0.0 --conf spark.driver.port=39001 --conf spark.blockManager.port=39002 --conf spark.default.parallelism=8 --jars /opt/spark-extra/hadoop-aws.jar,/opt/spark-extra/aws-sdk.jar,/opt/spark-extra/elasticsearch-spark.jar /opt/airflow/src/analytics/spark_gold.py'
with DAG('scipulse_arxiv_pipeline',start_date=datetime(2026,1,1),schedule=None,catchup=False,max_active_runs=1,max_active_tasks=1,default_args={'owner':'scipulse','retries':1,'retry_delay':timedelta(minutes=1)},tags=['scipulse','medallion','spark']) as dag:
    def task(name,command):return BashOperator(pool='scipulse_medallion',task_id=name,bash_command=BASE+command,execution_timeout=timedelta(hours=3))
    bronze_hn=task('bronze_hn','python -m src.pipeline.bronze hn')
    bronze_arxiv=task('bronze_arxiv','python -m src.pipeline.bronze arxiv --limit 100000')
    bronze_citations=task('bronze_citations','python -m src.pipeline.bronze citations --max-dois 5000')
    silver_arxiv=task('silver_arxiv','python -m src.pipeline.silver arxiv')
    silver_hn=task('silver_hn','python -m src.pipeline.silver hn')
    silver_citations=task('silver_citations','python -m src.pipeline.silver citations')
    dbt=task('transform_dbt','python -m src.pipeline.dbt_mirror && /home/airflow/dbt-env/bin/dbt build --project-dir dbt --profiles-dir dbt')
    gold=task('spark_gold',SPARK)
    validate=task('validate_gold','python -m src.pipeline.validate_gold')
    dashboards=task('publish_dashboards','python -m src.pipeline.dashboards')
    bronze_hn >> bronze_arxiv >> bronze_citations
    bronze_arxiv >> silver_arxiv
    bronze_hn >> silver_hn
    bronze_citations >> silver_citations
    silver_arxiv >> dbt
    [dbt,silver_hn,silver_citations] >> gold >> validate >> dashboards
