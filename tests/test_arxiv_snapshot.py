"""The full Bronze object must retain the source bytes, including formatting."""
import hashlib
from pathlib import Path
import pytest
from botocore.exceptions import ClientError
from src.pipeline import bronze


@pytest.mark.parametrize('corrupt',[False,True])
def test_snapshot_integrity(monkeypatch,tmp_path,corrupt):
    source=tmp_path/'snapshot.json'
    original=b'{ "id": "a", "title": "raw" }\r\n'
    source.write_bytes(original)
    objects={}
    reports=[]
    class Body:
        def iter_chunks(self,chunk_size):yield next(iter(objects.values()))
        def close(self):pass
    class Storage:
        def head_object(self,**kwargs):
            raise ClientError({'Error':{'Code':'404'}},'HeadObject')
        def upload_file(self,path,bucket,key,ExtraArgs):
            objects[key]=Path(path).read_bytes() if not corrupt else b'X'*len(original)
        def get_object(self,**kwargs):
            return {'ContentLength':len(original),'Body':Body()}
    monkeypatch.setattr(bronze,'ensure_buckets',lambda:Storage())
    monkeypatch.setattr(bronze,'save_report',lambda name,data:reports.append(data))
    if corrupt:
        with pytest.raises(RuntimeError,match='differs'):bronze.archive_arxiv_snapshot(source)
        assert not reports
    else:
        report=bronze.archive_arxiv_snapshot(source)
        assert next(iter(objects.values()))==original
        assert report['sha256']==hashlib.sha256(original).hexdigest()
        assert report['bytes_verified'] and not report['transformed']
