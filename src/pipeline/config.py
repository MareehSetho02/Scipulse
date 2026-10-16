"""Configuration commune du pipeline Medallion."""
import json
import os
from datetime import datetime,timezone
from pathlib import Path
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
ROOT=Path(os.getenv('SCIPULSE_ROOT',Path(__file__).resolve().parents[2]))
DATA=ROOT/'data'/'project'
REPORTS=ROOT/'reports'
for p in [DATA,REPORTS]:p.mkdir(parents=True,exist_ok=True)
BUCKETS=['arxiv-raw','hn-raw','citations-raw','arxiv-clean','hn-clean','citations-clean','pipeline-metrics']

def s3():
    return boto3.client('s3',endpoint_url=os.getenv('MINIO_ENDPOINT','http://localhost:9000'),aws_access_key_id=os.getenv('MINIO_ACCESS_KEY','minioadmin'),aws_secret_access_key=os.getenv('MINIO_SECRET_KEY','minioadmin'),region_name='us-east-1',config=Config(signature_version='s3v4',retries={'max_attempts':3},s3={'addressing_style':'path'}))

def ensure_buckets():
    client=s3()
    for bucket in BUCKETS:
        try:client.head_bucket(Bucket=bucket)
        except ClientError as exc:
            if str(exc.response['Error']['Code']) not in ('404','NoSuchBucket','NotFound'):raise
            client.create_bucket(Bucket=bucket)
    return client

def save_report(name,value):
    path=REPORTS/f'{name}.json'
    path.write_text(json.dumps(value,ensure_ascii=False,indent=2,default=str),encoding='utf-8')
    client=ensure_buckets();client.upload_file(str(path),'pipeline-metrics',path.name)
    if os.getenv('LOGSTASH_URL'):
        import requests
        try:
            requests.post(os.environ['LOGSTASH_URL'],json={'report':name,'timestamp':now(),'summary':value},timeout=5).raise_for_status()
        except requests.RequestException as exc:
            print('Supervision Logstash indisponible :',type(exc).__name__)
    return value

def now():return datetime.now(timezone.utc).isoformat()

def normalize_doi(value):
    import re
    match=re.search(r'10\.\d{4,9}/[^\s,;|]+',str(value or ''),flags=re.I)
    return match.group(0).rstrip('.').lower() if match else None
