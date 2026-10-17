"""Archive the legacy index before exposing the verified Silver index by alias."""
import json
import os
import requests
from src.pipeline.config import ROOT, now


def main():
    base=os.getenv('ES_HOST','http://elasticsearch:9200')
    def call(method,path,**kwargs):
        response=requests.request(method,base+path,timeout=180,**kwargs)
        response.raise_for_status()
        return response.json()
    validation=json.loads((ROOT/'reports/arxiv_index_validation.json').read_text())
    target=validation['index']
    legacy='arxiv-papers'
    archive='arxiv-papers-tp-archive'
    if not target.startswith('arxiv-papers-silver-'):
        raise ValueError('Expected a validated Silver index')
    if call('GET',f'/{target}/_count')['count']!=validation['silver_rows']:
        raise RuntimeError('Silver index count differs from the validation')
    # Refuse to overwrite an archive or rerun against an already migrated alias.
    if requests.head(base+'/'+archive,timeout=30).status_code!=404:
        raise RuntimeError('Archive already exists; inspect it before any further migration')
    metadata=call('GET',f'/{legacy}')
    if set(metadata)!={legacy}:
        raise RuntimeError('Expected a concrete legacy index')
    call('PUT',f'/{legacy}/_block/write')
    switched=False
    try:
        call('POST',f'/{legacy}/_refresh')
        before=call('GET',f'/{legacy}/_count')['count']
        gold_before=call('GET','/arxiv-papers-enriched/_count')['count']
        call('POST',f'/{legacy}/_clone/{archive}',json={'settings':{'index.number_of_replicas':0}})
        health=call('GET',f'/_cluster/health/{archive}',params={'wait_for_status':'green','timeout':'120s'})
        if health.get('timed_out'):
            raise RuntimeError('Archive recovery did not finish')
        archived=call('GET',f'/{archive}/_count')['count']
        archive_mapping=call('GET',f'/{archive}/_mapping')[archive]['mappings']
        legacy_mapping=call('GET',f'/{legacy}/_mapping')[legacy]['mappings']
        if archived!=before or archive_mapping!=legacy_mapping:
            raise RuntimeError('Archive count or mapping differs from legacy')
        # Atomic name swap, only after the clone is recovered and verified.
        call('POST','/_aliases',json={'actions':[{'remove_index':{'index':legacy}},
             {'add':{'index':target,'alias':legacy,'is_write_index':True}}]})
        switched=True
        count=call('GET',f'/{legacy}/_count')['count']
        aliases=call('GET',f'/_alias/{legacy}')
        gold_after=call('GET','/arxiv-papers-enriched/_count')['count']
        if count!=validation['silver_rows'] or set(aliases)!={target} or gold_after!=gold_before:
            raise RuntimeError('Post-migration checks failed')
        report={'timestamp':now(),'alias':legacy,'silver_index':target,'silver_documents':count,
                'archive_index':archive,'legacy_documents':before,'archived_documents':archived,
                'archive_mapping_matches':True,'gold_count_before':gold_before,'gold_count_after':gold_after,
                'archive_write_blocked':True,'success':True}
        (ROOT/'reports/arxiv_index_migration.json').write_text(json.dumps(report,indent=2),encoding='utf-8')
        print(json.dumps(report))
    finally:
        if not switched:
            call('PUT',f'/{legacy}/_settings',json={'index.blocks.write':False})


if __name__=='__main__':
    main()
