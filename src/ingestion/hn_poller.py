"""Poller HN : un cycle par défaut, déduplication par identifiant."""
import argparse
import os
import re
import time
from urllib.parse import urlparse
import requests
from elasticsearch import Elasticsearch, helpers
API='https://hacker-news.firebaseio.com/v0'

def extract_arxiv_id(url):
    parsed=urlparse(url or '')
    if parsed.hostname not in ('arxiv.org','www.arxiv.org','export.arxiv.org'): return None
    match=re.match(r'/(?:abs|pdf)/((?:\d{4}\.\d{4,5}|[a-z-]+(?:\.[A-Z]{2})?/\d{7}))(?:v\d+)?(?:\.pdf)?/?$',parsed.path)
    return match.group(1) if match else None

def fetch_new_story_ids():
    r=requests.get(f'{API}/newstories.json',timeout=30);r.raise_for_status();return r.json()

def prepare_hn_doc(item):
    doc={k:item[k] for k in ('type','by','time','title','url','text','score','descendants','parent') if k in item}
    doc['hn_id']=str(item['id'])
    doc['relation']={'name':'comment','parent':str(item['parent'])} if item.get('type')=='comment' else {'name':'story'}
    linked=extract_arxiv_id(item.get('url'))
    if linked: doc['arxiv_id_linked']=linked
    return doc

def poll_once(es,limit=500):
    ids=fetch_new_story_ids()[:limit]
    existing=es.mget(index='hn-items',ids=[str(i) for i in ids])['docs'] if ids else []
    known={d['_id'] for d in existing if d.get('found')}
    actions=[]
    for ident in ids:
        if str(ident) in known: continue
        r=requests.get(f'{API}/item/{ident}.json',timeout=30);r.raise_for_status();item=r.json()
        if not item or item.get('deleted') or item.get('dead') or item.get('type')!='story':continue
        actions.append({'_index':'hn-items','_id':str(ident),'_source':prepare_hn_doc(item)})
    count,errors=helpers.bulk(es,actions,raise_on_error=False)
    es.indices.refresh(index='hn-items')
    print(f'HN : {count} nouveaux items, {len(errors)} erreurs')
    if errors: raise RuntimeError(str(errors[:3]))
    return count

def main():
    p=argparse.ArgumentParser();p.add_argument('--limit',type=int,default=500);p.add_argument('--continuous',action='store_true');p.add_argument('--interval',type=int,default=300);args=p.parse_args()
    with Elasticsearch(os.getenv('ES_HOST','http://localhost:9200')) as es:
        while True:
            poll_once(es,args.limit)
            if not args.continuous:break
            time.sleep(args.interval)
if __name__=='__main__':main()
