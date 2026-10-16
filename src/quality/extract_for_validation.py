"""Extraction bornée pour les validations ; toutes les lignes pour le nettoyage."""
import os
from itertools import islice
import pandas as pd
from elasticsearch import Elasticsearch,helpers
ARXIV_COLUMNS=['arxiv_id','title','abstract','authors_flat','categories','primary_category','date_published','date_updated','doi']
HN_COLUMNS=['hn_id','type','by','time','title','url','text','score','descendants','arxiv_id_linked']

def extract(es,index,columns,max_docs):
    if max_docs<1:raise ValueError('max_docs doit être positif')
    hits=helpers.scan(es,index=index,size=min(1000,max_docs))
    try:return pd.DataFrame([{k:h['_source'].get(k) for k in columns} for h in islice(hits,max_docs)],columns=columns)
    finally:hits.close()
def extract_arxiv(es,index='arxiv-papers',max_docs=10000):return extract(es,index,ARXIV_COLUMNS,max_docs)
def extract_hn(es,index='hn-items',max_docs=5000):return extract(es,index,HN_COLUMNS,max_docs)
if __name__=='__main__':
    with Elasticsearch(os.getenv('ES_HOST','http://localhost:9200')) as es:
        print(extract_arxiv(es).info());print(extract_hn(es).info())
