import pandas as pd
from src.ingestion.arxiv_bulk_ingest import transform_article
from src.ingestion.reindex_arxiv import transform_for_reindex
from src.ingestion.hn_poller import extract_arxiv_id,prepare_hn_doc
from src.quality.clean_arxiv import clean

def test_arxiv_end_to_end_transform():
    raw={'id':'2401.12345','title':'  A\n title  ','abstract':'Text\n here','categories':'cs.AI cs.LG','authors':'Ada Lovelace','authors_parsed':[['Lovelace','Ada','']],'versions':[{'created':'Mon, 2 Apr 2007 19:18:42 GMT'}],'update_date':'2024-01-01'}
    doc=transform_for_reindex({'_id':raw['id'],'_source':transform_article(raw)})
    assert doc['arxiv_id']==raw['id']
    assert doc['primary_category']=='cs.AI'
    assert doc['authors'][0]['last']=='Lovelace'
    assert doc['title']=='A title'
    assert doc['date_published'].startswith('2007-04-02')

def test_links_and_join():
    assert extract_arxiv_id('https://arxiv.org/pdf/2401.12345v2.pdf')=='2401.12345'
    assert extract_arxiv_id('https://arxiv.org/abs/hep-th/9901001')=='hep-th/9901001'
    assert extract_arxiv_id('https://evil.example/arxiv.org/abs/2401.12345') is None
    assert prepare_hn_doc({'id':2,'type':'comment','parent':1})['relation']=={'name':'comment','parent':'1'}

def test_clean_preserves_source_and_is_idempotent():
    source=pd.DataFrame([{'arxiv_id':'a','abstract':None,'title':' A\n title ','authors_flat':'Ada  Lovelace','categories':['cs.AI'],'primary_category':None}]*2)
    result=clean(source)
    assert len(result)==1 and len(source)==2
    assert bool(result.iloc[0]['abstract_short'])
    assert result.iloc[0]['primary_category']=='cs.AI'
    pd.testing.assert_frame_equal(clean(result),result)

def test_minio_forbidden_is_not_treated_as_missing(monkeypatch):
    import pytest
    from botocore.exceptions import ClientError
    from src.ingestion import upload_to_minio as upload
    class Forbidden:
        def head_bucket(self,**kwargs):
            raise ClientError({'Error':{'Code':'403','Message':'Forbidden'}},'HeadBucket')
        def create_bucket(self,**kwargs):
            raise AssertionError('Ne doit pas créer après un refus d’accès')
    monkeypatch.setattr(upload,'s3',Forbidden())
    with pytest.raises(ClientError):upload.ensure_bucket_exists('arxiv-raw')
    with pytest.raises(ValueError):upload.split_and_upload('unused',chunk_size=0)
