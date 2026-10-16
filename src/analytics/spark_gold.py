"""Spark Gold : MinIO Silver -> jointures, fenêtres, MLlib TF-IDF -> ES-Hadoop."""
import json
import math
import os
from pathlib import Path
from urllib.parse import urlparse
from pyspark.sql import SparkSession,functions as F,Window,types as T
from pyspark.ml import Pipeline
from pyspark.ml.feature import RegexTokenizer,StopWordsRemover,HashingTF,IDF
from pyspark.ml.functions import vector_to_array

ROOT=Path(os.getenv('SCIPULSE_ROOT','/opt/airflow'))

def build_spark():
    b=SparkSession.builder.appName('SciPulse-Gold').config('spark.sql.shuffle.partitions','8').config('spark.sql.adaptive.enabled','true').config('spark.hadoop.fs.s3a.endpoint',os.getenv('MINIO_ENDPOINT','http://minio:9000')).config('spark.hadoop.fs.s3a.access.key',os.getenv('MINIO_ACCESS_KEY','minioadmin')).config('spark.hadoop.fs.s3a.secret.key',os.getenv('MINIO_SECRET_KEY','minioadmin')).config('spark.hadoop.fs.s3a.path.style.access','true').config('spark.hadoop.fs.s3a.connection.ssl.enabled','false').config('spark.hadoop.fs.s3a.impl','org.apache.hadoop.fs.s3a.S3AFileSystem').config('spark.hadoop.fs.s3a.aws.credentials.provider','org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')
    return b.getOrCreate()

def main():
    import requests
    spark=build_spark();spark.sparkContext.setLogLevel('WARN')
    report_dir=ROOT/'reports';report_dir.mkdir(exist_ok=True)
    a=spark.read.parquet('s3a://arxiv-clean/current/data.parquet').dropDuplicates(['arxiv_id']).cache()
    h=spark.read.parquet('s3a://hn-clean/current/data.parquet')
    c=spark.read.parquet('s3a://citations-clean/current/data.parquet').dropDuplicates(['doi'])
    hn=h.filter(F.col('arxiv_id_linked').isNotNull()).groupBy('arxiv_id_linked').agg(F.sum(F.coalesce('score',F.lit(0))).alias('hn_score'),F.sum(F.coalesce('comments_count',F.lit(0))).alias('hn_comments_count'),F.count('*').alias('hn_mentions')).withColumnRenamed('arxiv_id_linked','arxiv_id')
    # Mesure les deux plans ; seuls les petits côtés sont diffusés.
    broadcast_join=a.join(F.broadcast(c),'doi','left').join(F.broadcast(hn),'arxiv_id','left')
    merge_join=a.hint('merge').join(c.hint('merge'),'doi','left').join(hn.hint('merge'),'arxiv_id','left')
    plans={'broadcast':broadcast_join._jdf.queryExecution().executedPlan().toString(),'sort_merge':merge_join._jdf.queryExecution().executedPlan().toString()}
    (report_dir/'spark_plans.json').write_text(json.dumps(plans,indent=2),encoding='utf-8')
    broadcast_join.explain(mode='formatted')
    joined=broadcast_join.withColumn('citation_matched',F.col('openalex_id').isNotNull()).withColumn('hn_matched',F.col('hn_mentions').isNotNull()).fillna({'hn_score':0,'hn_comments_count':0,'hn_mentions':0}).cache()
    total=joined.count();citations=joined.filter('citation_matched').count();hn_matches=joined.filter('hn_matched').count()
    both=joined.filter('citation_matched AND hn_matched').count()
    # Croissance annuelle : lag sur catégories et années présentes, avec indicateur de continuité.
    annual=a.withColumn('year',F.year('date_published')).groupBy('primary_category','year').count()
    win=Window.partitionBy('primary_category').orderBy('year')
    annual=annual.withColumn('previous_count',F.lag('count').over(win)).withColumn('previous_year',F.lag('year').over(win)).withColumn('growth_pct',F.when((F.col('previous_count')>0)&(F.col('year')==F.col('previous_year')+1),100*(F.col('count')/F.col('previous_count')-1)))
    growth=annual.select('primary_category',F.col('year').alias('publication_year'),'growth_pct')
    pipeline=Pipeline(stages=[RegexTokenizer(inputCol='abstract',outputCol='tokens',pattern='\\W+',minTokenLength=3),StopWordsRemover(inputCol='tokens',outputCol='filtered_tokens'),HashingTF(inputCol='filtered_tokens',outputCol='tf',numFeatures=2048),IDF(inputCol='tf',outputCol='tfidf',minDocFreq=2)])
    model=pipeline.fit(joined.fillna({'abstract':''}))
    holdout=spark.createDataFrame([('Novel neural network architecture for science',),('',),('Previously unseen zzyyxx token',)],['abstract'])
    holdout_vectors=model.transform(holdout).select(vector_to_array('tfidf').alias('vector')).collect()
    holdout_ok=all(len(row.vector)==2048 and all(math.isfinite(x) for x in row.vector) for row in holdout_vectors)
    if not holdout_ok:raise RuntimeError('Échec transformation TF-IDF des exemples hors ajustement')
    enriched=model.transform(joined.fillna({'abstract':''})).withColumn('tfidf_peak',F.array_max(vector_to_array('tfidf'))).withColumn('keywords',F.slice(F.array_distinct('filtered_tokens'),1,12)).drop('tokens','filtered_tokens','tf','tfidf')
    maximum=joined.agg(F.max('citation_count'),F.max('hn_score')).first()
    max_c=int(maximum[0] or 1);max_h=int(maximum[1] or 1)
    enriched=enriched.withColumn('publication_year',F.year('date_published')).join(growth,['primary_category','publication_year'],'left')
    enriched=enriched.withColumn('age_years',F.greatest(F.datediff(F.current_date(),F.to_date('date_published'))/365.25,F.lit(0.0))).withColumn('recency_component',F.exp(-F.col('age_years')/5.0)).withColumn('citation_component',F.log1p(F.coalesce('citation_count',F.lit(0)))/F.lit(math.log1p(max_c))).withColumn('hn_component',F.log1p('hn_score')/F.lit(math.log1p(max_h))).withColumn('citation_velocity',F.col('recent_citations')/2.0).withColumn('impact_score',F.round(100*(.5*F.col('citation_component')+.3*F.col('hn_component')+.2*F.col('recency_component')),4)).withColumn('score_is_lower_bound',~F.col('citation_matched')).withColumn('signal_profile',F.when(F.col('citation_matched')&(F.col('citation_count')>=50)&~F.col('hn_matched'),'Académique sans signal HN observé').when(F.col('hn_matched')&(F.col('hn_score')>=50)&F.col('citation_matched')&(F.col('citation_count')<10),'Viral HN, peu cité').when(F.col('hn_matched')&F.col('citation_matched'),'Deux signaux observés').when(F.col('hn_matched'),'HN, citations inconnues').otherwise('Pas de signal HN observé')).withColumn('pipeline_updated_at',F.current_timestamp()).cache()
    # Vélocité annuelle par article : fenêtre sur les compteurs OpenAlex réellement observés.
    yearly_schema=T.ArrayType(T.StructType([T.StructField('year',T.IntegerType()),T.StructField('cited_by_count',T.LongType()),T.StructField('works_count',T.LongType())]))
    yearly=c.select('doi',F.explode(F.from_json('counts_by_year_json',yearly_schema)).alias('annual')).select('doi','annual.year',F.col('annual.cited_by_count').alias('citations'))
    cw=Window.partitionBy('doi').orderBy('year')
    yearly=yearly.withColumn('previous_citations',F.lag('citations').over(cw)).withColumn('citation_acceleration',F.col('citations')-F.col('previous_citations'))
    yearly.write.mode('overwrite').parquet('s3a://citations-clean/analytics/yearly_velocity/')
    annual.write.mode('overwrite').parquet('s3a://arxiv-clean/analytics/annual_growth/')
    mapping={'settings':{'number_of_shards':1,'number_of_replicas':0,'analysis':{'analyzer':{'scientific_english':{'type':'english'}}}},'mappings':{'properties':{**{k:{'type':'keyword'} for k in ['arxiv_id','doi','categories','primary_category','academic_field','signal_profile','keywords','openalex_id']},**{k:{'type':'date'} for k in ['date_published','date_updated','pipeline_updated_at','citations_observed_at']},'title':{'type':'text','analyzer':'scientific_english','fields':{'keyword':{'type':'keyword','ignore_above':512}}},'abstract':{'type':'text','analyzer':'scientific_english','term_vector':'with_positions_offsets'},**{k:{'type':'double'} for k in ['impact_score','citation_velocity','growth_pct','tfidf_peak','recency_component','citation_component','hn_component']},**{k:{'type':'long'} for k in ['citation_count','hn_score','hn_comments_count','hn_mentions','recent_citations']},**{k:{'type':'boolean'} for k in ['citation_matched','hn_matched','score_is_lower_bound']}}}}
    es=os.getenv('ES_HOST','http://elasticsearch:9200');index='arxiv-papers-enriched'
    r=requests.head(f'{es}/{index}',timeout=30)
    if r.status_code==404:requests.put(f'{es}/{index}',json=mapping,timeout=30).raise_for_status()
    else:r.raise_for_status()
    import uuid
    run_id=str(uuid.uuid4())
    requests.put(f'{es}/{index}/_mapping',json={'properties':{'run_id':{'type':'keyword'}}},timeout=30).raise_for_status()
    enriched=enriched.withColumn('run_id',F.lit(run_id))
    target=urlparse(es)
    enriched.drop('counts_by_year_json').write.format('org.elasticsearch.spark.sql').option('es.nodes',target.hostname).option('es.port',str(target.port or 9200)).option('es.nodes.wan.only','true').option('es.mapping.id','arxiv_id').option('es.write.operation','index').option('es.batch.size.entries','500').mode('append').save(index)
    requests.post(f'{es}/{index}/_refresh',timeout=60).raise_for_status()
    cleanup=requests.post(f'{es}/{index}/_delete_by_query',params={'refresh':'true'},json={'query':{'bool':{'must_not':[{'term':{'run_id':run_id}}]}}},timeout=120)
    cleanup.raise_for_status()
    sample=[r.asDict(recursive=True) for r in enriched.orderBy(F.desc('impact_score')).select('arxiv_id','title','primary_category','citation_count','hn_score','impact_score','score_is_lower_bound').limit(20).collect()]
    enriched.select('arxiv_id','title','primary_category','citation_count','hn_score','impact_score','citation_matched','hn_matched','score_is_lower_bound').toPandas().to_parquet(report_dir/'gold_summary.parquet',index=False)
    report={'articles':total,'citations_matched':citations,'hn_matched':hn_matches,'both_matched':both,'citation_coverage':citations/total,'hn_coverage':hn_matches/total,'weights':{'citations':.5,'hn':.3,'recency':.2},'max_citations':max_c,'max_hn_score':max_h,'tfidf_features':2048,'tfidf_holdout_passed':holdout_ok,'run_id':run_id,'top_articles':sample,'gold_index':index,'join_strategy':'broadcast dimensions after aggregation and DOI deduplication','limits':'Citations not queried or not found remain null. No HN mention means not observed in this collection. Supplement of HN-linked articles biases popularity estimates. Recency uses wall-clock date.'}
    (report_dir/'spark_results.json').write_text(json.dumps(report,ensure_ascii=False,indent=2,default=str),encoding='utf-8')
    print('GOLD_RESULT',json.dumps({k:v for k,v in report.items() if k!='top_articles'}),flush=True)
    model.write().overwrite().save('s3a://arxiv-clean/models/tfidf/')
    # Preuve des executors et des taches via l'API du driver avant son arret.
    ui=spark.sparkContext.uiWebUrl
    applications=requests.get(f'{ui}/api/v1/applications',timeout=30)
    applications.raise_for_status()
    application_id=spark.sparkContext.applicationId
    executors=requests.get(f'{ui}/api/v1/applications/{application_id}/executors',timeout=30)
    executors.raise_for_status()
    active=[e for e in executors.json() if e['id']!='driver']
    proof={'application_id':application_id,'master':spark.sparkContext.master,
           'executors':active,'workers_with_completed_tasks':len({e['hostPort'].split(':')[0] for e in active if e.get('completedTasks',0)>0}),
           'single_physical_host':True}
    proof['two_workers_used']=proof['workers_with_completed_tasks']>=2
    (report_dir/'spark_cluster_execution.json').write_text(json.dumps(proof,indent=2),encoding='utf-8')
    print('CLUSTER_RESULT',json.dumps(proof),flush=True)
    enriched.unpersist();joined.unpersist();a.unpersist();spark.stop()
    if not proof['two_workers_used']:
        raise RuntimeError('Deux workers distincts doivent avoir termine des taches Spark')
if __name__=='__main__':main()
