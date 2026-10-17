"""Record Airflow completion and compare current Gold with local Spark results."""
import argparse
import json
import os
import pandas as pd
from elasticsearch import Elasticsearch
from airflow.settings import Session
from airflow.models import DagRun, TaskInstance
from src.pipeline.config import ROOT, now


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--run-id',required=True)
    parser.add_argument('--logstash-paused',action='store_true')
    args=parser.parse_args()
    def read(name):return json.loads((ROOT/'reports'/name).read_text())
    results=read('spark_results.json')
    quality=read('gold_validation.json')
    cluster=read('spark_cluster_execution.json')
    tests=read('latest_validation.json')['tests']
    session=Session()
    try:
        run=session.query(DagRun).filter_by(dag_id='scipulse_arxiv_pipeline',run_id=args.run_id).one()
        tasks=[{'task_id':t.task_id,'state':t.state,'start_date':str(t.start_date),'end_date':str(t.end_date)}
               for t in session.query(TaskInstance).filter_by(dag_id=run.dag_id,run_id=args.run_id)]
        state=run.state
    finally:
        session.close()
    frame=pd.read_parquet(ROOT/'reports/gold_summary.parquet')
    es=Elasticsearch(os.getenv('ES_HOST','http://elasticsearch:9200'))
    index='arxiv-papers-enriched'
    count=es.count(index=index)['count']
    matching=es.count(index=index,query={'term':{'run_id':results['run_id']}})['count']
    expected={
        'academic_without_hn':int(((frame.citation_count>=50)&frame.citation_matched&~frame.hn_matched).sum()),
        'viral_low_citations':int(((frame.hn_score>=50)&(frame.citation_count<10)&frame.citation_matched).sum()),
        'cross_source':int((frame.hn_matched&frame.citation_matched).sum())}
    checks={'dag_success':state=='success','all_tasks_success':bool(tasks) and all(t['state']=='success' for t in tasks),
            'row_count':count==len(frame)==results['articles'], 'all_documents_current_run':matching==count,
            'gold_quality':all(quality['checks'].values()),
            'query_counts':expected==quality['analytical_queries'],
            'two_workers_used':cluster['two_workers_used'],
            'tfidf_holdout':results['tfidf_holdout_passed'],
            'tests_success':tests['tests']>0 and not any(tests[k] for k in ['failures','errors','skipped'])}
    report={'timestamp':now(),'airflow_run_id':args.run_id,'dag_state':state,'tasks':tasks,
            'spark_run_id':results['run_id'],'spark_application_id':cluster['application_id'],
            'articles':count,'analytical_queries':expected,'tests':tests,'checks':checks,
            'full_pipeline_rerun':True,'same_host_docker':True,
            'logstash_temporarily_stopped_during_run':args.logstash_paused,'success':all(checks.values())}
    (ROOT/'reports/final_execution_validation.json').write_text(json.dumps(report,indent=2),encoding='utf-8')
    print(json.dumps(report))
    if not report['success']:raise RuntimeError('Execution validation failed')


if __name__=='__main__':main()
