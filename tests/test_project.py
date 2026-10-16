"""Tests des frontières Bronze/Silver et du score, indépendants des services."""
from src.pipeline.config import normalize_doi
from src.pipeline.silver import clean_arxiv,clean_hn,clean_citation
from src.ingestion.hn_poller import extract_arxiv_id

def test_identifier_normalization():
    assert normalize_doi(' HTTPS://doi.org/10.1000/ABC. ')=='10.1000/abc'
    assert normalize_doi('10.1000/one, 10.1000/two')=='10.1000/one'
    assert normalize_doi(None) is None
    assert normalize_doi('not-a-doi') is None

def test_clean_does_not_invent_content():
    raw={'id':'2401.01234v2','title':' A\n discovery ','abstract':None,'categories':'cs.AI stat.ML','authors':'Ada  Lovelace','versions':[{'created':'Mon, 2 Apr 2007 19:18:42 GMT'}]}
    out=clean_arxiv(raw)
    assert out['arxiv_id']=='2401.01234'
    assert out['abstract']=='' and out['abstract_short']
    assert out['date_published']=='2007-04-02'
    assert raw['abstract'] is None

def test_hn_sources_normalize_to_same_key():
    a=clean_hn({'id':123,'type':'story','score':20,'time':1700000000,'url':'https://arxiv.org/abs/2401.01234v2'})
    b=clean_hn({'objectID':'123','points':20,'created_at_i':1700000000,'url':'https://arxiv.org/pdf/2401.01234.pdf'})
    assert a['hn_id']==b['hn_id']=='123'
    assert a['arxiv_id_linked']==b['arxiv_id_linked']=='2401.01234'
    assert a['time']==b['time']
    assert extract_arxiv_id('https://arxiv.org.evil.test/abs/2401.01234') is None

def test_missing_citation_stays_unknown():
    out=clean_citation({'id':'https://openalex.org/W1','doi':'https://doi.org/10.1000/ABC'})
    assert out['citation_count'] is None
    assert out['doi']=='10.1000/abc'

def test_legacy_arxiv_id_and_missing_date_are_preserved():
    raw={'id':'math/0201234','title':'A legacy paper','abstract':'Some text','categories':'math.CO','versions':[]}
    out=clean_arxiv(raw)
    assert out['arxiv_id']=='math/0201234'
    assert out['date_published'] is None
    assert out['primary_category']=='math.CO'
    assert out['abstract_short']


def test_hn_poll_without_history_preserves_local_and_remote_history(monkeypatch, tmp_path):
    import json
    from src.pipeline import bronze
    history=tmp_path/'hn_algolia.jsonl'
    original=b'{"objectID":"42","url":"https://arxiv.org/abs/2401.01234"}\n'
    history.write_bytes(original)
    objects={('hn-raw','algolia/arxiv_history.jsonl'):original}
    calls=[]
    reports=[]
    class Storage:
        def upload_file(self,path,bucket,key):
            calls.append((bucket,key))
            objects[(bucket,key)]=__import__('pathlib').Path(path).read_bytes()
    def get(url,params=None):
        assert 'algolia' not in url
        return [123] if url.endswith('/newstories.json') else {'id':123,'type':'story'}
    monkeypatch.setattr(bronze,'DATA',tmp_path)
    monkeypatch.setattr(bronze,'ensure_buckets',lambda:Storage())
    monkeypatch.setattr(bronze,'get',get)
    monkeypatch.setattr(bronze,'save_report',lambda name,value:reports.append(value))
    bronze.hn(limit=1,history_pages=0)
    assert history.read_bytes()==original
    assert objects[('hn-raw','algolia/arxiv_history.jsonl')]==original
    assert len(calls)==1 and calls[0][1].startswith('firebase/')
    assert reports[0]['history_refreshed'] is False
    assert reports[0]['historical_count'] is None


def test_hn_explicit_history_refresh_still_uploads_history(monkeypatch,tmp_path):
    from pathlib import Path
    from src.pipeline import bronze
    uploads={}
    class Storage:
        def upload_file(self,path,bucket,key):uploads[key]=Path(path).read_text(encoding='utf-8')
    def get(url,params=None):
        if 'algolia' in url:return {'hits':[{'objectID':'42'}],'nbPages':1}
        return []
    monkeypatch.setattr(bronze,'DATA',tmp_path)
    monkeypatch.setattr(bronze,'ensure_buckets',lambda:Storage())
    monkeypatch.setattr(bronze,'get',get)
    monkeypatch.setattr(bronze,'save_report',lambda name,value:value)
    bronze.hn(limit=0,history_pages=1)
    assert '42' in uploads['algolia/arxiv_history.jsonl']
    assert (tmp_path/'hn_algolia.jsonl').read_text(encoding='utf-8')==uploads['algolia/arxiv_history.jsonl']


def test_hn_poll_without_history_does_not_create_empty_history(monkeypatch,tmp_path):
    from src.pipeline import bronze
    class Storage:
        def upload_file(self,path,bucket,key):assert key.startswith('firebase/')
    monkeypatch.setattr(bronze,'DATA',tmp_path)
    monkeypatch.setattr(bronze,'ensure_buckets',lambda:Storage())
    monkeypatch.setattr(bronze,'get',lambda url,params=None:[])
    monkeypatch.setattr(bronze,'save_report',lambda name,value:value)
    bronze.hn(limit=0,history_pages=0)
    assert not (tmp_path/'hn_algolia.jsonl').exists()
