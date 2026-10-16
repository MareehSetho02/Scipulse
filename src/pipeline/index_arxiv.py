"""Publish the current ArXiv Silver to a separate, verifiable exploration index."""
import io
import os
import uuid
import pandas as pd
from elasticsearch import Elasticsearch, helpers
from src.pipeline.config import s3, save_report, now


def main():
    storage=s3()
    source=storage.get_object(Bucket='arxiv-clean',Key='current/data.parquet')
    articles=pd.read_parquet(io.BytesIO(source['Body'].read()))
    if articles.empty or articles.arxiv_id.isna().any() or not articles.arxiv_id.is_unique:
        raise ValueError('Silver must contain nonempty unique article identifiers')
    es=Elasticsearch(os.getenv('ES_HOST','http://elasticsearch:9200'))
    index='arxiv-papers-silver-'+uuid.uuid4().hex[:12]
    properties={k:{'type':'keyword'} for k in ['arxiv_id','doi','categories','primary_category']}
    properties.update({k:{'type':'date'} for k in ['date_published','date_updated']})
    properties.update({k:{'type':'text','analyzer':'english'} for k in ['title','abstract','authors_flat']})
    properties['abstract_short']={'type':'boolean'}
    es.indices.create(index=index,settings={'number_of_shards':1,'number_of_replicas':0},mappings={'properties':properties})
    def actions():
        import json
        # Pandas serializes missing scalars as JSON null, preserving unknown DOI/dates.
        for row in json.loads(articles.to_json(orient='records',date_format='iso')):
            yield {'_index':index,'_id':row['arxiv_id'],'_source':row}
    helpers.bulk(es.options(request_timeout=120),actions(),chunk_size=500)
    es.indices.refresh(index=index)
    count=es.count(index=index)['count']
    if count!=len(articles):
        raise RuntimeError('Indexed count differs from Silver')
    totals=articles.primary_category.value_counts().to_dict()
    buckets=es.search(index=index,size=0,aggs={'categories':{'terms':{'field':'primary_category','size':1000}}})['aggregations']['categories']['buckets']
    if {b['key']:b['doc_count'] for b in buckets}!=totals:
        raise RuntimeError('Category aggregation differs from Silver')
    search=es.search(index=index,size=3,query={'multi_match':{'query':'neural network','fields':['title^2','abstract']}})
    yearly=es.search(index=index,size=0,aggs={'years':{'date_histogram':{'field':'date_published','calendar_interval':'year'}}})
    recent=es.search(index=index,size=3,query={'term':{'primary_category':'cs.AI'}},sort=[{'date_published':{'order':'desc','missing':'_last'}}])
    report={'timestamp':now(),'index':index,'source':'s3://arxiv-clean/current/data.parquet','source_etag':source.get('ETag'),
            'silver_rows':len(articles),'indexed_rows':count,'category_counts_match':True,
            'fulltext_hits':search['hits']['total'],'annual_buckets':yearly['aggregations']['years']['buckets'],
            'recent_cs_ai_ids':[h['_id'] for h in recent['hits']['hits']],
            'legacy_index_replaced':False,'gold_modified':False}
    save_report('arxiv_index_validation',report)
    print(index,count)


if __name__=='__main__':
    main()
