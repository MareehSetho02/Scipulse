"""Validation finale Gold, comparaison de compteurs et tests analytiques réels."""
import json
import os
import pandas as pd
import great_expectations as gx
from elasticsearch import Elasticsearch
from src.pipeline.config import ROOT,save_report,now
from src.quality.common import run_suite

def main():
    df=pd.read_parquet(ROOT/'reports/gold_summary.parquet')
    e=gx.expectations
    rules=[e.ExpectColumnValuesToNotBeNull(column='arxiv_id'),e.ExpectColumnValuesToBeUnique(column='arxiv_id'),e.ExpectColumnValuesToBeBetween(column='impact_score',min_value=0,max_value=100),e.ExpectColumnValuesToBeBetween(column='citation_count',min_value=0),e.ExpectColumnValuesToBeBetween(column='hn_score',min_value=0),e.ExpectTableRowCountToBeBetween(min_value=1)]
    _,result=run_suite(df,'project_gold',rules,root=str(ROOT/'gx'))
    es=Elasticsearch(os.getenv('ES_HOST','http://localhost:9200'))
    index='arxiv-papers-enriched';count=es.count(index=index)['count']
    queries={
        'academic_without_hn':{'bool':{'filter':[{'range':{'citation_count':{'gte':50}}},{'term':{'citation_matched':True}}],'must_not':[{'term':{'hn_matched':True}}]}},
        'viral_low_citations':{'bool':{'filter':[{'range':{'hn_score':{'gte':50}}},{'range':{'citation_count':{'lt':10}}},{'term':{'citation_matched':True}}]}},
        'cross_source':{'bool':{'filter':[{'term':{'hn_matched':True}},{'term':{'citation_matched':True}}]}}
    }
    answers={name:es.count(index=index,query=q)['count'] for name,q in queries.items()}
    missing=df[~df.citation_matched]
    checks={'unique_ids':df.arxiv_id.is_unique,'same_row_count':count==len(df),'unknown_citations_preserved':bool(missing.citation_count.isna().all()),'lower_bound_flag':bool(missing.score_is_lower_bound.all()),'score_range':bool(df.impact_score.between(0,100).all()),'quality_success':bool(result.success)}
    examples=es.search(index=index,size=100,source=['impact_score','citation_component','hn_component','recency_component'])['hits']['hits']
    checks['score_formula']=all(abs(hit['_source']['impact_score']-100*(.5*hit['_source']['citation_component']+.3*hit['_source']['hn_component']+.2*hit['_source']['recency_component']))<=.0001 for hit in examples)
    report=save_report('gold_validation',{'timestamp':now(),'rows':len(df),'es_count':count,'checks':checks,'analytical_queries':answers,'gx_statistics':[r.statistics for r in result.run_results.values()]})
    print(report)
    if not all(checks.values()):raise RuntimeError('Validation Gold échouée')
if __name__=='__main__':main()
