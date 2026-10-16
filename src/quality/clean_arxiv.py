"""Nettoyage réversible : aperçu puis mises à jour par _id, sans suppression."""
import argparse
import os
import re
import pandas as pd
from elasticsearch import Elasticsearch,helpers
from src.quality.validate_arxiv import KNOWN_CS_CATEGORIES

def load_all_articles(es):
    return pd.DataFrame([dict(h['_source'],_id=h['_id']) for h in helpers.scan(es,index='arxiv-papers')])

def diagnose(df):
    abstract=df['abstract'].fillna('');title=df['title'].fillna('')
    return dict(null_abstracts=int(df.abstract.isna().sum()),short_abstracts=int(abstract.str.len().lt(50).sum()),null_titles=int(df.title.isna().sum()),empty_titles=int(title.str.strip().eq('').sum()),duplicates=int(df.arxiv_id.duplicated().sum()),unknown_categories=int((~df.primary_category.isin(KNOWN_CS_CATEGORIES)).sum()),multi_space_titles=int(title.str.contains(r'\s{2,}|[\r\n]',regex=True).sum()))

def clean(df):
    df=df.copy().drop_duplicates('arxiv_id',keep='first')
    for col in ('title','abstract','authors_flat'):
        if col in df:df[col]=df[col].fillna('').map(lambda x:re.sub(r'\s+',' ',str(x)).strip())
    df['abstract_short']=df.abstract.str.len().lt(50)
    for idx,row in df.iterrows():
        cats=row.get('categories')
        if isinstance(cats,str):cats=cats.split()
        if isinstance(cats,list) and cats:df.at[idx,'primary_category']=cats[0]
    return df

def print_diagnosis(report,label):print(label,report)

def reindex_cleaned(es,df):
    def actions():
        for row in df.to_dict('records'):
            ident=row.pop('_id')
            # Mise à jour partielle : conserve les champs non concernés.
            yield {'_op_type':'update','_index':'arxiv-papers','_id':ident,'doc':{k:row[k] for k in ('title','abstract','authors_flat','primary_category','abstract_short') if k in row}}
    count,errors=helpers.bulk(es,actions(),chunk_size=500,raise_on_error=False)
    es.indices.refresh(index='arxiv-papers')
    if errors:raise RuntimeError(str(errors[:3]))
    return count

def main():
    p=argparse.ArgumentParser();p.add_argument('--dry-run',action='store_true');args=p.parse_args()
    with Elasticsearch(os.getenv('ES_HOST','http://localhost:9200'),request_timeout=60) as es:
        df=load_all_articles(es)
        if df.empty:raise RuntimeError('Index vide')
        print_diagnosis(diagnose(df),'AVANT');cleaned=clean(df);print_diagnosis(diagnose(cleaned),'APRES')
        if not args.dry_run:
            if df.arxiv_id.duplicated().any():raise RuntimeError('Doublons détectés : traitement manuel des _id requis avant écriture')
            print('Mis à jour :',reindex_cleaned(es,cleaned))
if __name__=='__main__':main()
