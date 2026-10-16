"""Mesures de monitoring : manifestes MinIO, objets publiés et agrégats Gold."""
import json
import os
from datetime import datetime, timezone

import requests
from botocore.exceptions import ClientError

from src.pipeline.config import s3


def timestamp(value):
    if not value:
        return None
    result = value if isinstance(value, datetime) else datetime.fromisoformat(value.replace('Z', '+00:00'))
    return result.replace(tzinfo=timezone.utc) if result.tzinfo is None else result.astimezone(timezone.utc)


def quality_statistics(report):
    stats = report.get('statistics', report.get('gx_statistics', []))
    if not stats or any('evaluated_expectations' not in s or 'unsuccessful_expectations' not in s for s in stats):
        return {'evaluated': None, 'failed': None, 'error_pct': None}
    evaluated = sum(int(s.get('evaluated_expectations', 0)) for s in stats)
    failed = sum(int(s.get('unsuccessful_expectations', 0)) for s in stats)
    return {'evaluated': evaluated, 'failed': failed,
            'error_pct': 100 * failed / evaluated if evaluated else None}


def freshness(value, reference):
    observed = timestamp(value)
    seconds = (reference - observed).total_seconds() if observed else None
    return {'timestamp': observed.isoformat() if observed else None,
            'age_seconds': seconds}


def build_snapshot(reports, bronze_dates, silver_dates, gold, reference=None):
    reference = reference or datetime.now(timezone.utc)
    sources = {}
    for source in ['arxiv', 'hn', 'citations']:
        silver = reports.get('silver_' + source, {})
        sources[source] = {
            'bronze_rows_checked': silver.get('bronze_rows'),
            'silver_rows_checked': silver.get('silver_rows'),
            'quality_success': silver.get('quality_success'),
            'quality': quality_statistics(silver),
            'bronze': freshness(bronze_dates.get(source), reference),
            'silver': freshness(silver_dates.get(source), reference),
            'last_quality_check': freshness(silver.get('timestamp'), reference),
        }
    validation = reports.get('gold_validation', {})
    return {'published_at': reference.isoformat(), 'sources': sources,
            'gold': {'rows': gold.get('rows'),
                     'updated': freshness(gold.get('updated_at'), reference),
                     'quality': quality_statistics(validation),
                     'last_quality_check': freshness(validation.get('timestamp'), reference)},
            'rate_definition': '100 * unsuccessful_expectations / evaluated_expectations',
            'refresh': 'After each HN cycle and after the main pipeline; ages measured at publication.'}


def collect_snapshot():
    client = s3()
    reports = {}
    for name in ['silver_arxiv', 'silver_hn', 'silver_citations', 'gold_validation']:
        try:
            response = client.get_object(Bucket='pipeline-metrics', Key=name + '.json')
        except ClientError as exc:
            if exc.response['Error']['Code'] not in ('NoSuchKey', '404', 'NoSuchBucket'):
                raise
            continue
        body = response['Body']
        try:
            reports[name] = json.loads(body.read())
        finally:
            body.close()
    bronze_dates, silver_dates = {}, {}
    for source, prefix in [('arxiv', 'project/'), ('hn', ''), ('citations', 'openalex/')]:
        dates = []
        for page in client.get_paginator('list_objects_v2').paginate(Bucket=source + '-raw', Prefix=prefix):
            dates.extend(o['LastModified'] for o in page.get('Contents', []) if o['Key'].endswith('.jsonl'))
        bronze_dates[source] = max(dates) if dates else None
        try:
            silver_dates[source] = client.head_object(Bucket=source + '-clean', Key='current/data.parquet')['LastModified']
        except ClientError as exc:
            if exc.response['Error']['Code'] not in ('NoSuchKey', '404', 'NotFound'):
                raise
            silver_dates[source] = None
    response = requests.post(os.getenv('ES_HOST', 'http://localhost:9200') + '/arxiv-papers-enriched/_search',
                             json={'size': 0, 'track_total_hits': True,
                                   'aggs': {'updated': {'max': {'field': 'pipeline_updated_at'}}}}, timeout=30)
    if response.status_code == 404:
        gold = {}
    else:
        response.raise_for_status()
        result = response.json()
        gold = {'rows': result['hits']['total']['value'],
                'updated_at': result['aggregations']['updated'].get('value_as_string')}
    return build_snapshot(reports, bronze_dates, silver_dates, gold)


def display_date(info):
    value = info['timestamp']
    return timestamp(value).strftime('%Y-%m-%d %H:%M:%S UTC') if value else 'Indisponible'


def display_age(info):
    seconds = info['age_seconds']
    if seconds is None:
        return 'Indisponible'
    if seconds < 0:
        return 'Horodatage futur : vérifier les horloges'
    minutes = int(seconds // 60)
    days, hours = divmod(minutes // 60, 24)
    return f'{days} j {hours} h {minutes % 60} min'


def markdown(snapshot):
    lines = ['## Monitoring du pipeline Medallion',
             f"Instantané publié : **{snapshot['published_at']}**",
             'Les volumes Bronze/Silver ci-dessous sont ceux du dernier contrôle GX de chaque source. '
             'Les dates de dépôt sont lues directement dans MinIO. Le volume et la date Gold sont lus dans Elasticsearch.',
             '', '### Volumes et qualité Great Expectations',
             '| Source | Bronze contrôlée | Silver contrôlée | État GX | Attentes échouées / évaluées | Taux d’échec GX |',
             '|---|---:|---:|---|---:|---:|']
    for source, data in snapshot['sources'].items():
        q = data['quality']
        state = {True: 'Réussie', False: 'Échec — Silver non publiée', None: 'Indisponible'}[data['quality_success']]
        counts = f"{q['failed']} / {q['evaluated']}" if q['evaluated'] is not None else 'Indisponible'
        rate = f"{q['error_pct']:.2f} %" if q['error_pct'] is not None else 'Indisponible'
        lines.append(f"| {source} | {data['bronze_rows_checked'] if data['bronze_rows_checked'] is not None else 'Indisponible'} | "
                     f"{data['silver_rows_checked'] if data['silver_rows_checked'] is not None else 'Indisponible'} | {state} | {counts} | {rate} |")
    gold = snapshot['gold']
    q = gold['quality']
    counts = f"{q['failed']} / {q['evaluated']}" if q['evaluated'] is not None else 'Indisponible'
    rate = f"{q['error_pct']:.2f} %" if q['error_pct'] is not None else 'Indisponible'
    lines += [f"| Gold | — | {gold['rows'] if gold['rows'] is not None else 'Indisponible'} | Dernière validation | {counts} | {rate} |",
              '', '**Taux d’échec GX = 100 × attentes échouées / attentes évaluées.** '
              'Ce taux mesure les contrôles en échec, pas le pourcentage de lignes incorrectes. '
              'Un rapport absent ou sans attentes ne vaut pas 0 %.',
              '', '### Fraîcheur des données et des contrôles',
              '| Source / couche | Dernière écriture ou contrôle | Ancienneté à la publication |',
              '|---|---|---|']
    for source, data in snapshot['sources'].items():
        for label, key in [('Bronze : dernier dépôt', 'bronze'), ('Silver : dernier Parquet publié', 'silver'), ('GX : dernier contrôle', 'last_quality_check')]:
            info = data[key]
            lines.append(f'| {source} / {label} | {display_date(info)} | {display_age(info)} |')
    for label, key in [('Gold : dernier calcul Spark', 'updated'), ('Gold : dernier contrôle GX', 'last_quality_check')]:
        info = gold[key]
        lines.append(f'| {label} | {display_date(info)} | {display_age(info)} |')
    lines += ['', '**Actualisation :** après chaque cycle HN (planifié toutes les 5 minutes) et à la fin du pipeline principal. '
              'Recharger le dashboard pour lire la dernière publication. Les anciennetés sont calculées à cette publication, pas à chaque seconde. '
              'Un échec ou un retard Airflow peut retarder la publication.',
              '**Interprétation :** un dépôt récent ne signifie pas une nouvelle donnée chez le fournisseur ; OpenAlex peut être relu depuis le cache. '
              'Une Silver en échec ne remplace pas le dernier Parquet valide. Les volumes contrôlés peuvent donc différer des données publiées. '
              'Les mises à jour HN ne recalculent pas automatiquement la Gold.',
              '**Détails :** Airflow http://localhost:8080 ; résultats GX dans les Data Docs et les manifestes MinIO `pipeline-metrics/`.']
    return '\n'.join(lines)
