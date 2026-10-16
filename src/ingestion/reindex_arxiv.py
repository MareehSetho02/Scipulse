"""TP 3 : copie et normalisation sans suppression de la source."""
import os
import re
from datetime import datetime
from email.utils import parsedate_to_datetime
from elasticsearch import Elasticsearch, helpers

ES_HOST = os.getenv('ES_HOST', 'http://localhost:9200')
SOURCE_INDEX = 'arxiv-papers-raw-py'
DEST_INDEX = 'arxiv-papers'

def parse_authors(value):
    if not value:
        return [], ''
    result=[]
    for name in re.split(r'\s+and\s+|,\s*(?=[A-Z])', value):
        parts=name.strip().split()
        if parts:
            result.append(dict(first=parts[0] if len(parts)>1 else '', last=parts[-1], full_name=' '.join(parts)))
    return result, re.sub(r'\s+', ' ', value).strip()

def transform_for_reindex(hit):
    raw=hit['_source']
    doc={k:v for k,v in raw.items() if k not in ('authors_parsed','versions','id','update_date')}
    doc['arxiv_id']=raw.get('arxiv_id') or raw.get('id') or hit['_id']
    parsed=raw.get('authors_parsed')
    if parsed:
        authors=[dict(last=a[0],first=a[1],middle=a[2] if len(a)>2 else '',full_name=' '.join(x for x in [a[1],a[2] if len(a)>2 else '',a[0]] if x)) for a in parsed]
        flat=raw.get('authors') or ', '.join(a['full_name'] for a in authors)
    else:
        authors,flat=parse_authors(raw.get('authors',''))
    doc.update(authors=authors,authors_flat=flat)
    cats=raw.get('categories') or []
    if isinstance(cats,str): cats=cats.split()
    doc.update(categories=cats,primary_category=cats[0] if cats else None)
    doc['date_updated']=raw.get('date_updated') or raw.get('update_date')
    versions=raw.get('versions') or []
    if versions:
        try: doc['date_published']=parsedate_to_datetime(versions[0]['created']).isoformat()
        except (ValueError,KeyError,TypeError): pass
    return doc

def generate_actions(es):
    for hit in helpers.scan(es,index=SOURCE_INDEX):
        doc=transform_for_reindex(hit)
        yield {'_index':DEST_INDEX,'_id':doc['arxiv_id'],'_source':doc}

def main():
    with Elasticsearch(ES_HOST,request_timeout=60) as es:
        if not es.indices.exists(index=DEST_INDEX):
            raise RuntimeError('Créer les mappings : python -m src.utils.mappings')
        count,errors=helpers.bulk(es,generate_actions(es),chunk_size=500,raise_on_error=False)
        es.indices.refresh(index=DEST_INDEX)
        print(f'Réindexés : {count}; erreurs : {len(errors)}')
        if errors: raise RuntimeError(str(errors[:3]))

if __name__=='__main__': main()
