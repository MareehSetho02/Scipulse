"""Ingestion Bronze : HN Firebase + recherche historique Algolia, ArXiv, OpenAlex."""
import argparse
import hashlib
import json
import os
import random
import time
from pathlib import Path
from urllib.parse import urlparse
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from src.pipeline.config import DATA,ROOT,ensure_buckets,save_report,now,normalize_doi
from src.ingestion.hn_poller import extract_arxiv_id
SESSION=requests.Session()
SESSION.mount('https://',HTTPAdapter(max_retries=Retry(total=4,backoff_factor=2,status_forcelist=[429,500,502,503,504],respect_retry_after_header=True)))
SESSION.headers['User-Agent']='SciPulse-academic-project/1.0'

def get(url,params=None):
    r=SESSION.get(url,params=params,timeout=(10,60));r.raise_for_status();return r.json()

def write_jsonl(path,records):
    with path.open('w',encoding='utf-8') as out:
        for row in records:out.write(json.dumps(row,ensure_ascii=False)+'\n')

def hn(limit=200,history_pages=5):
    if limit < 0 or history_pages < 0:
        raise ValueError('limit et history_pages doivent être positifs ou nuls')
    client=ensure_buckets();records=[]
    ids=get('https://hacker-news.firebaseio.com/v0/newstories.json')[:limit]
    for ident in ids:
        item=get(f'https://hacker-news.firebaseio.com/v0/item/{ident}.json')
        if item:records.append(item)
    stamp=now().replace(':','-')
    path=DATA/'hn_firebase.jsonl';write_jsonl(path,records)
    client.upload_file(str(path),'hn-raw',f'firebase/{stamp}.jsonl')

    history=[]
    if history_pages > 0:
        for page in range(history_pages):
            payload=get('https://hn.algolia.com/api/v1/search',{'query':'arxiv.org','tags':'story','hitsPerPage':100,'page':page})
            history.extend(payload['hits'])
            if page+1>=payload['nbPages']:break
        path=DATA/'hn_algolia.jsonl';write_jsonl(path,history)
        client.upload_file(str(path),'hn-raw','algolia/arxiv_history.jsonl')
    report=save_report('bronze_hn',{'timestamp':now(),'firebase_count':len(records),'historical_count':len(history) if history_pages > 0 else None,'history_refreshed':history_pages > 0,'history_source':'HN Algolia search API','bronze':'hn-raw/'})
    print(report,flush=True);return records,history

def arxiv(limit=100000):
    prepared=DATA/'arxiv.jsonl'
    if prepared.exists() and os.getenv('SCIPULSE_RESAMPLE','0')!='1':
        ensure_buckets().upload_file(str(prepared),'arxiv-raw','project/arxiv.jsonl')
        print('Instantané ArXiv préparé réutilisé; SCIPULSE_RESAMPLE=1 pour rééchantillonner',flush=True)
        return
    source=Path(os.getenv('ARXIV_SOURCE',ROOT/'data/arxiv-raw/arxiv-metadata-oai-snapshot.json'))
    if not source.exists() and (DATA/'arxiv.jsonl').exists():
        ensure_buckets().upload_file(str(DATA/'arxiv.jsonl'),'arxiv-raw','project/arxiv.jsonl')
        print('Sous-ensemble fourni restauré dans Bronze',flush=True)
        return
    if not source.exists():
        source.parent.mkdir(parents=True,exist_ok=True)
        import zipfile
        archive=DATA/'arxiv.zip'
        with SESSION.get('https://www.kaggle.com/api/v1/datasets/download/Cornell-University/arxiv',stream=True,timeout=(15,120)) as r:
            r.raise_for_status()
            with archive.open('wb') as f:
                for chunk in r.iter_content(1024*1024):f.write(chunk)
        with zipfile.ZipFile(archive) as z:
            member=next(n for n in z.namelist() if n.endswith('.json'))
            with z.open(member) as inp,source.open('wb') as out:
                import shutil;shutil.copyfileobj(inp,out)
    linked=set()
    hp=DATA/'hn_algolia.jsonl'
    if hp.exists():
        for line in hp.open(encoding='utf-8'):
            ident=extract_arxiv_id(json.loads(line).get('url'))
            if ident:linked.add(ident)
    rng=random.Random(42);sample=[];targeted={};seen=0
    with source.open(encoding='utf-8') as inp:
        for line in inp:
            raw=json.loads(line)
            ident=raw.get('id')
            if ident in linked:targeted[ident]=raw
            if not any(c.startswith('cs.') or c=='stat.ML' for c in raw.get('categories','').split()):continue
            seen+=1
            if len(sample)<limit:sample.append(raw)
            else:
                pos=rng.randrange(seen)
                if pos<limit:sample[pos]=raw
    by_id={r['id']:r for r in sample};extra=0
    for ident,raw in targeted.items():
        if ident not in by_id:by_id[ident]=raw;extra+=1
    path=DATA/'arxiv.jsonl';write_jsonl(path,by_id.values())
    ensure_buckets().upload_file(str(path),'arxiv-raw','project/arxiv.jsonl')
    save_report('bronze_arxiv',{'timestamp':now(),'source':source.name,'eligible_cs_ml':seen,'random_sample':len(sample),'hn_linked_added':extra,'total':len(by_id),'seed':42,'sampling_bias':'Reservoir CS/stat.ML plus intentional HN-linked supplement; descriptive comparisons are not causal.'})
    print('ArXiv Bronze :',len(by_id),flush=True)

def citations(max_dois=5000):
    seen=set();dois=[]
    for line in (DATA/'arxiv.jsonl').open(encoding='utf-8'):
        doi=normalize_doi(json.loads(line).get('doi'))
        if doi and doi not in seen:seen.add(doi);dois.append(doi)
    # Requêtes groupées (50 DOI) ; les absences resteront explicitement inconnues.
    selected=dois[:max_dois] if max_dois else dois
    linked=set()
    history=DATA/'hn_algolia.jsonl'
    if history.exists():
        linked={extract_arxiv_id(json.loads(line).get('url')) for line in history.open(encoding='utf-8')}
    extra=[]
    for line in (DATA/'arxiv.jsonl').open(encoding='utf-8'):
        raw=json.loads(line);doi=normalize_doi(raw.get('doi'))
        if raw.get('id') in linked and doi and doi not in selected:extra.append(doi)
    selected+=list(dict.fromkeys(extra))
    rows=[];requests_count=0
    cache=DATA/'openalex_cache';cache.mkdir(exist_ok=True)
    for start in range(0,len(selected),50):
        batch=selected[start:start+50]
        key=hashlib.sha256('|'.join(batch).encode()).hexdigest()
        path=cache/f'{key}.json'
        if path.exists():payload=json.loads(path.read_text(encoding='utf-8'))
        else:
            params={'filter':'doi:'+'|'.join(batch),'per-page':100,'cursor':'*'}
            if os.getenv('OPENALEX_API_KEY'):params['api_key']=os.environ['OPENALEX_API_KEY']
            try:
                payload=get('https://api.openalex.org/works',params)
            except requests.HTTPError as exc:
                if exc.response.status_code!=400:raise
                print('Lot OpenAlex non valide, reprise DOI par DOI',flush=True)
                recovered=[]
                for doi in batch:
                    try:recovered.extend(get('https://api.openalex.org/works',{'filter':'doi:'+doi,'per-page':100}).get('results',[]))
                    except requests.HTTPError as single:
                        if single.response.status_code!=400:raise
                payload={'results':recovered}
            path.write_text(json.dumps(payload),encoding='utf-8');requests_count+=1;time.sleep(.15)
        rows.extend(payload.get('results',[]))
    path=DATA/'citations.jsonl';write_jsonl(path,rows)
    ensure_buckets().upload_file(str(path),'citations-raw','openalex/works.jsonl')
    save_report('bronze_citations',{'timestamp':now(),'provider':'OpenAlex','doi_available':len(dois),'doi_queried':len(selected),'works_returned':len(rows),'network_requests':requests_count,'coverage_limit':max_dois,'hn_targeted_dois':len(extra),'missing_policy':'No citation record is unknown, not zero citations.'})
    print('OpenAlex Bronze :',len(rows),flush=True)

def main():
    p=argparse.ArgumentParser();p.add_argument('source',choices=['hn','arxiv','citations','all']);p.add_argument('--limit',type=int,default=100000);p.add_argument('--max-dois',type=int,default=5000);a=p.parse_args()
    if a.source in ('hn','all'):hn()
    if a.source in ('arxiv','all'):arxiv(a.limit)
    if a.source in ('citations','all'):citations(a.max_dois)
if __name__=='__main__':main()
