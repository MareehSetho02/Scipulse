"""Nettoyage Silver, validation GX et publication Parquet par source dans MinIO."""
import argparse
import io
import json
from datetime import datetime,timezone
from email.utils import parsedate_to_datetime
import re
import pandas as pd
import great_expectations as gx
from src.pipeline.config import DATA,ROOT,ensure_buckets,save_report,normalize_doi,now
from src.quality.common import run_suite
from src.ingestion.hn_poller import extract_arxiv_id

def text(value):return re.sub(r'\s+',' ',str(value or '')).strip()
def arxiv_id(value):return re.sub(r'v\d+$','',str(value or '')) or None

def read_rows(bucket,prefix):
    client=ensure_buckets()
    for page in client.get_paginator('list_objects_v2').paginate(Bucket=bucket,Prefix=prefix):
        for obj in page.get('Contents',[]):
            if not obj['Key'].endswith('.jsonl'):continue
            body=client.get_object(Bucket=bucket,Key=obj['Key'])['Body']
            try:
                for line in body.iter_lines():
                    if line:yield json.loads(line)
            finally:body.close()

def clean_arxiv(raw):
    ident=arxiv_id(raw.get('id'))
    categories=raw.get('categories') or ''
    if isinstance(categories,str):categories=categories.split()
    published=None
    try:published=parsedate_to_datetime(raw['versions'][0]['created']).date().isoformat()
    except (KeyError,IndexError,TypeError,ValueError):pass
    date=raw.get('update_date')
    return dict(arxiv_id=ident,doi=normalize_doi(raw.get('doi')),title=text(raw.get('title')),abstract=text(raw.get('abstract')),authors_flat=text(raw.get('authors')),categories=categories,primary_category=categories[0] if categories else None,date_published=published,date_updated=date,abstract_short=len(text(raw.get('abstract')))<50)

def clean_hn(raw):
    algolia='objectID' in raw
    ident=raw.get('objectID') if algolia else raw.get('id')
    stamp=raw.get('created_at_i') if algolia else raw.get('time')
    url=raw.get('url')
    return dict(hn_id=str(ident) if ident else None,type='story' if algolia else raw.get('type'),title=text(raw.get('title')),url=url,score=raw.get('points') if algolia else raw.get('score'),comments_count=raw.get('num_comments') if algolia else raw.get('descendants'),time=datetime.fromtimestamp(stamp,timezone.utc).isoformat() if stamp else None,arxiv_id_linked=extract_arxiv_id(url),source='algolia' if algolia else 'firebase',deleted=bool(raw.get('dead') or raw.get('deleted')))

def clean_citation(raw):
    topic=raw.get('primary_topic') or {}
    field=topic.get('field') or {}
    yearly=raw.get('counts_by_year') or []
    current=datetime.now(timezone.utc).year
    recent=sum(int(x.get('cited_by_count') or 0) for x in yearly if x.get('year',0)>=current-1)
    return dict(openalex_id=raw.get('id'),doi=normalize_doi(raw.get('doi')),citation_count=raw.get('cited_by_count'),publication_year=raw.get('publication_year'),academic_field=field.get('display_name'),recent_citations=recent,counts_by_year_json=json.dumps(yearly),citations_observed_at=now())

def expectations(source):
    e=gx.expectations
    if source=='arxiv':return [e.ExpectColumnValuesToNotBeNull(column='arxiv_id'),e.ExpectColumnValuesToBeUnique(column='arxiv_id'),e.ExpectColumnValuesToNotBeNull(column='primary_category'),e.ExpectColumnValueLengthsToBeBetween(column='title',min_value=1),e.ExpectColumnValuesToMatchRegex(column='arxiv_id',regex=r'^(\d{4}\.\d{4,5}|[a-z-]+(?:\.[A-Z]{2})?/\d{7})$'),e.ExpectColumnValuesToNotBeNull(column='date_published'),e.ExpectTableRowCountToBeBetween(min_value=1)]
    if source=='hn':return [e.ExpectColumnValuesToNotBeNull(column='hn_id'),e.ExpectColumnValuesToBeUnique(column='hn_id'),e.ExpectColumnValuesToBeInSet(column='type',value_set=['story','comment','job','poll','pollopt']),e.ExpectColumnValuesToBeBetween(column='score',min_value=0),e.ExpectColumnValuesToNotBeNull(column='time'),e.ExpectTableRowCountToBeBetween(min_value=1)]
    return [e.ExpectColumnValuesToNotBeNull(column='openalex_id'),e.ExpectColumnValuesToBeUnique(column='doi'),e.ExpectColumnValuesToNotBeNull(column='doi'),e.ExpectColumnValuesToBeBetween(column='citation_count',min_value=0),e.ExpectTableRowCountToBeBetween(min_value=1)]

def run(source):
    specs={'arxiv':('arxiv-raw','project/',clean_arxiv,'arxiv_id'),'hn':('hn-raw','',clean_hn,'hn_id'),'citations':('citations-raw','openalex/',clean_citation,'doi')}
    bucket,prefix,fn,key=specs[source]
    rows=list(read_rows(bucket,prefix))
    if not rows:raise RuntimeError(f'Aucune donnée Bronze pour {source}')
    df=pd.DataFrame(fn(row) for row in rows)
    count_before=len(df);missing=int(df[key].isna().sum());duplicates=int(df[key].duplicated().sum())
    df=df.dropna(subset=[key]).drop_duplicates(key,keep='last')
    if source=='hn':df=df[~df.deleted].drop(columns='deleted')
    for c in ['score','comments_count','citation_count','publication_year','recent_citations']:
        if c in df:df[c]=pd.to_numeric(df[c],errors='coerce').astype('Int64')
    _,result=run_suite(df,f'project_{source}',expectations(source),root=str(ROOT/'gx'))
    stats=[r.statistics for r in result.run_results.values()]
    report=dict(timestamp=now(),source=source,bronze_rows=count_before,missing_ids=missing,duplicate_ids=duplicates,silver_rows=len(df),quality_success=bool(result.success),statistics=stats,short_abstracts=int(df.abstract_short.sum()) if 'abstract_short' in df else None)
    save_report(f'silver_{source}',report)
    if not result.success:raise RuntimeError(f'Qualité Silver échouée : {source}; voir Data Docs')
    path=DATA/f'{source}.parquet';df.to_parquet(path,index=False)
    ensure_buckets().upload_file(str(path),f'{source}-clean','current/data.parquet')
    print(report,flush=True);return df

def main():
    p=argparse.ArgumentParser();p.add_argument('source',choices=['arxiv','hn','citations','all']);a=p.parse_args()
    for source in (['arxiv','hn','citations'] if a.source=='all' else [a.source]):run(source)
if __name__=='__main__':main()
