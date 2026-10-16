"""Provisionnement reproductible de deux dashboards Kibana et export NDJSON."""
import json
import os
from pathlib import Path
import requests
from src.pipeline.config import ROOT,save_report
from src.pipeline.monitoring import collect_snapshot,markdown
BASE=os.getenv('KIBANA_URL','http://localhost:5601')
HEADERS={'kbn-xsrf':'scipulse','Content-Type':'application/json'}
VIEW='scipulse-gold-view'

def api(method,path,**kwargs):
    r=requests.request(method,BASE+path,headers=HEADERS,timeout=90,**kwargs);r.raise_for_status();return r

def save(kind,ident,attributes,references=None):
    return api('POST',f'/api/saved_objects/{kind}/{ident}?overwrite=true',json={'attributes':attributes,'references':references or []}).json()

def vis(ident,title,kind,aggs,query=''):
    state={'title':title,'type':kind,'params':{'addTooltip':True,'addLegend':True,'legendPosition':'right','type':'histogram','shareYAxis':True,'scale':'linear','mode':'stacked','times':[],'addTimeMarker':False,'dimensions':{'x':None,'y':[]}},'aggs':aggs}
    if kind=='metric':state['params']={'fontSize':38,'metric':{'colorSchema':'Green to Red','colorsRange':[{'from':0,'to':10000000}],'invertColors':False,'labels':{'show':True},'percentageMode':False,'style':{'bgFill':'#000','bgColor':False,'labelColor':False,'subText':'','fontSize':48}}}
    if kind=='table':
        for agg in state['aggs']:
            if agg['schema']=='segment':agg['schema']='bucket'
        state['params']={'perPage':10,'showPartialRows':False,'showMetricsAtAllLevels':False,'sort':{'columnIndex':None,'direction':None},'showTotal':False,'totalFunc':'sum','percentageCol':''}
    return save('visualization',ident,{'title':title,'visState':json.dumps(state),'uiStateJSON':'{}','description':'SciPulse Gold — données observées uniquement','version':1,'kibanaSavedObjectMeta':{'searchSourceJSON':json.dumps({'query':{'query':query,'language':'kuery'},'filter':[],'indexRefName':'kibanaSavedObjectMeta.searchSourceJSON.index'})}},[{'name':'kibanaSavedObjectMeta.searchSourceJSON.index','type':'index-pattern','id':VIEW}])

def metric(ident,title,field=None,kind='avg',query=''):
    vis(ident,title,'metric',[{'id':'1','enabled':True,'type':kind if field else 'count','schema':'metric','params':{'field':field} if field else {}}],query)

def dashboard(ident,title,panels,description):
    refs=[];layout=[]
    for pos,(kind,obj,x,y,w,h) in enumerate(panels):
        ref=f'panel_{pos}';refs.append({'name':ref,'type':kind,'id':obj})
        layout.append({'version':'8.13.4','type':kind,'gridData':{'x':x,'y':y,'w':w,'h':h,'i':str(pos)},'panelIndex':str(pos),'embeddableConfig':{},'panelRefName':ref})
    return save('dashboard',ident,{'title':title,'description':description,'panelsJSON':json.dumps(layout),'optionsJSON':json.dumps({'useMargins':True,'syncColors':True,'hidePanelTitles':False}),'version':1,'timeRestore':False,'kibanaSavedObjectMeta':{'searchSourceJSON':json.dumps({'query':{'query':'','language':'kuery'},'filter':[]})}},refs)

def main():
    api('POST','/api/data_views/data_view',json={'data_view':{'id':VIEW,'title':'arxiv-papers-enriched','name':'SciPulse · Gold enrichi'},'override':True})
    metric('sp-count','Articles analysés')
    metric('sp-coverage','Articles reliés à OpenAlex',query='citation_matched: true')
    metric('sp-hn','Articles avec mention HN observée',query='hn_matched: true')
    metric('sp-impact','Score d’impact moyen / 100','impact_score')
    vis('sp-categories','Principales catégories scientifiques','histogram',[{'id':'1','enabled':True,'type':'count','schema':'metric','params':{}},{'id':'2','enabled':True,'type':'terms','schema':'segment','params':{'field':'primary_category','size':12,'order':'desc','orderBy':'1'}}])
    vis('sp-timeline','Publications par année','histogram',[{'id':'1','enabled':True,'type':'count','schema':'metric','params':{}},{'id':'2','enabled':True,'type':'date_histogram','schema':'segment','params':{'field':'date_published','calendar_interval':'year','interval':'year','min_doc_count':1,'extended_bounds':{}}}])
    vis('sp-signals','Profils d’impact observés','pie',[{'id':'1','enabled':True,'type':'count','schema':'metric','params':{}},{'id':'2','enabled':True,'type':'terms','schema':'segment','params':{'field':'signal_profile','size':8,'order':'desc','orderBy':'1'}}])
    vis('sp-growth','Croissance moyenne pondérée par articles (%)','table',[{'id':'1','enabled':True,'type':'avg','schema':'metric','params':{'field':'growth_pct'}},{'id':'2','enabled':True,'type':'terms','schema':'segment','params':{'field':'primary_category','size':10,'order':'desc','orderBy':'1'}}],query='growth_pct: *')
    search_refs=[{'name':'kibanaSavedObjectMeta.searchSourceJSON.index','type':'index-pattern','id':VIEW}]
    save('search','sp-top',{'title':'Articles classés par impact — filtrables','description':'Utiliser les filtres KQL pour comparer citations et HN','columns':['title','primary_category','citation_count','hn_score','impact_score','score_is_lower_bound'],'sort':[['impact_score','desc']],'kibanaSavedObjectMeta':{'searchSourceJSON':json.dumps({'query':{'query':'','language':'kuery'},'filter':[],'indexRefName':search_refs[0]['name']})}},search_refs)
    note='''## SciPulse Insights · Impact scientifique et écho tech
**Trois sources réelles : ArXiv, Hacker News, OpenAlex.**
Score = 50 % citations normalisées (log) + 30 % score HN normalisé (log) + 20 % récence.
**Citations inconnues ≠ zéro citation.** Le score est une borne basse si `score_is_lower_bound: true`.
Filtrer en cliquant une catégorie ou via KQL : `citation_matched: true and hn_matched: false` ; `hn_score >= 50 and citation_count < 10`.
Le corpus comporte un échantillon CS/stat.ML et un complément ciblé de publications liées sur HN. Les tendances décrivent ce corpus ; elles ne mesurent pas la production mondiale. Les mots-clés affichés sont lexicaux ; le TF-IDF distribué est calculé séparément.'''
    save('visualization','sp-note',{'title':'Lire les résultats','visState':json.dumps({'title':'Lire les résultats','type':'markdown','params':{'markdown':note,'openLinksInNewTab':True},'aggs':[]}),'uiStateJSON':'{}','kibanaSavedObjectMeta':{'searchSourceJSON':'{}'}})
    dashboard('scipulse-insights','SciPulse Insights — influence scientifique et popularité tech',[('visualization','sp-note',0,0,48,9),('visualization','sp-count',0,9,12,7),('visualization','sp-coverage',12,9,12,7),('visualization','sp-hn',24,9,12,7),('visualization','sp-impact',36,9,12,7),('visualization','sp-categories',0,16,24,15),('visualization','sp-timeline',24,16,24,15),('visualization','sp-signals',0,31,24,15),('visualization','sp-growth',24,31,24,15),('search','sp-top',0,46,48,20)],'Couche Gold — trois sources, score composite, couverture et limites explicites')
    publish_monitoring()
    export_dashboards()
    print('Dashboards : /app/dashboards#/view/scipulse-insights et /app/dashboards#/view/scipulse-monitoring')


def publish_monitoring():
    # Le mode monitoring ne réécrit pas le dashboard d'exploration personnel.
    try:
        api('GET', f'/api/data_views/data_view/{VIEW}')
    except requests.HTTPError as exc:
        if exc.response.status_code != 404:
            raise
        api('POST', '/api/data_views/data_view',
            json={'data_view': {'id': VIEW, 'title': 'arxiv-papers-enriched', 'name': 'SciPulse · Gold enrichi'}})
    metric('sp-count','Articles analysés')
    metric('sp-coverage','Articles reliés à OpenAlex',query='citation_matched: true')
    metric('sp-hn','Articles avec mention HN observée',query='hn_matched: true')
    snapshot = collect_snapshot()
    save('visualization','sp-monitor-note', {
        'title':'Volumes, qualité GX et fraîcheur Bronze / Silver / Gold',
        'visState':json.dumps({'type':'markdown','params':{'markdown':markdown(snapshot),'openLinksInNewTab':True},'aggs':[]}),
        'uiStateJSON':'{}','kibanaSavedObjectMeta':{'searchSourceJSON':'{}'}})
    dashboard('scipulse-monitoring','SciPulse — Monitoring Bronze → Silver → Gold',[
        ('visualization','sp-monitor-note',0,0,48,43),
        ('visualization','sp-count',0,43,16,8),
        ('visualization','sp-coverage',16,43,16,8),
        ('visualization','sp-hn',32,43,16,8)],
        'Volumes, taux d’échec des attentes GX, dates et ancienneté ; renouvelé après chaque cycle HN')
    save_report('monitoring', snapshot)
    print('Monitoring publié :', snapshot['published_at'], flush=True)


def export_dashboards():
    objects = []
    for ident in ['scipulse-insights', 'scipulse-monitoring']:
        try:
            api('GET', f'/api/saved_objects/dashboard/{ident}')
        except requests.HTTPError as exc:
            if exc.response.status_code != 404:
                raise
        else:
            objects.append({'type':'dashboard','id':ident})
    response=api('POST','/api/saved_objects/_export',json={'objects':objects,'includeReferencesDeep':True,'excludeExportDetails':True})
    (ROOT/'artifacts').mkdir(exist_ok=True)
    (ROOT/'artifacts/kibana_dashboards.ndjson').write_text(response.text,encoding='utf-8')


if __name__=='__main__':
    import argparse
    parser=argparse.ArgumentParser()
    parser.add_argument('--monitoring-only',action='store_true')
    args=parser.parse_args()
    if args.monitoring_only:
        publish_monitoring()
        export_dashboards()
    else:
        main()
