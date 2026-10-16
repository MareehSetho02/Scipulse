"""Miroir analytique dbt ; MinIO reste la source Silver autoritative."""
import csv
import io
import os
import pandas as pd
import psycopg2
from src.pipeline.config import ensure_buckets

def main():
    body=ensure_buckets().get_object(Bucket='arxiv-clean',Key='current/data.parquet')['Body']
    try:df=pd.read_parquet(io.BytesIO(body.read()))
    finally:body.close()
    cols=['arxiv_id','doi','title','primary_category','date_published']
    buf=io.StringIO();df[cols].to_csv(buf,index=False,header=False);buf.seek(0)
    with psycopg2.connect(host=os.getenv('PGHOST','postgres' if os.getenv('SCIPULSE_ROOT')=='/opt/airflow' else 'localhost'),dbname='scipulse',user=os.getenv('PGUSER','scipulse'),password=os.getenv('PGPASSWORD','scipulse')) as conn:
        with conn.cursor() as cur:
            cur.execute('CREATE SCHEMA IF NOT EXISTS staging')
            cur.execute('CREATE TABLE IF NOT EXISTS staging.project_arxiv (arxiv_id text PRIMARY KEY,doi text,title text,primary_category text,date_published date)')
            cur.execute('TRUNCATE staging.project_arxiv')
            cur.copy_expert('COPY staging.project_arxiv (arxiv_id,doi,title,primary_category,date_published) FROM STDIN WITH CSV',buf)
    print('Miroir PostgreSQL Silver :',len(df))
if __name__=='__main__':main()
