"""Démarrage isolé d'un export Git ; arrêt temporaire puis restauration du poste."""
import json
import subprocess
import time
import uuid
import zipfile
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parent
BASE = ['docker', 'compose', '-f', 'docker-compose.yml', '-f', 'docker-compose.project.yml']


def output(command, cwd=ROOT):
    return subprocess.check_output(command, cwd=cwd, text=True, encoding='utf-8')


def airflow(method, path, **kwargs):
    response = requests.request(method, 'http://localhost:8080/api/v1/' + path,
                                auth=('admin', 'admin'), timeout=15, **kwargs)
    response.raise_for_status()
    return response.json()


def main():
    project = 'scipulse-repro-' + uuid.uuid4().hex[:10]
    work = ROOT / '.cache' / project
    work.mkdir(parents=True)
    archive = work / 'source.zip'
    with archive.open('wb') as target:
        subprocess.run(['git', 'archive', '--format=zip', 'HEAD'], cwd=ROOT, stdout=target, check=True)
    source = work / 'source'
    source.mkdir()
    with zipfile.ZipFile(archive) as exported:
        for name in exported.namelist():
            if not (source / name).resolve().is_relative_to(source.resolve()):
                raise RuntimeError('Chemin hors export')
        exported.extractall(source)
    config = json.loads(output(BASE + ['config', '--format', 'json'], cwd=source))
    config['name'] = project
    for service in config['services'].values():
        service.pop('container_name', None)
        service.pop('ports', None)
        if 'environment' in service:
            service['environment']['AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION'] = 'true'
    for section in ['volumes', 'networks']:
        for definition in config.get(section, {}).values():
            if definition.get('external'):
                raise RuntimeError('Ressource externe interdite pour le test isolé')
            definition.pop('name', None)
    path = work / 'compose.json'
    path.write_text(json.dumps(config), encoding='utf-8')
    command = ['docker', 'compose', '-p', project, '-f', str(path)]
    if output(command + ['ps', '-aq']).strip():
        raise RuntimeError('Le projet isolé existe déjà')
    expected_count = requests.get('http://localhost:9200/arxiv-papers-enriched/_count', timeout=15)
    expected_count.raise_for_status()
    running = output(BASE + ['ps', '--services', '--filter', 'status=running']).split()
    report = {'project': project, 'source_commit': output(['git', 'rev-parse', 'HEAD']).strip(),
              'fresh_volumes': True, 'source': 'git archive HEAD', 'cached_images': True,
              'image_build_retested': False, 'full_data_pipeline_retested': False,
              'work_gold_count_before': expected_count.json()['count'], 'success': False}
    paused = {}
    stop_attempted = False
    isolated_attempted = False
    log_path = ROOT / 'reports' / 'cold_start.log'
    with log_path.open('w', encoding='utf-8') as log:
        try:
            for dag in ['scipulse_arxiv_pipeline', 'scipulse_hn_poller']:
                paused[dag] = airflow('GET', 'dags/' + dag)['is_paused']
                airflow('PATCH', 'dags/' + dag, json={'is_paused': True})
            report['work_dag_pause_states_before'] = paused.copy()
            deadline = time.monotonic() + 180
            while any(airflow('GET', 'dags/' + dag + '/dagRuns', params={'state': 'running'})['dag_runs'] for dag in paused):
                if time.monotonic() > deadline:
                    raise RuntimeError('Traitement actif : arrêt annulé')
                print('Attente de fin des tâches actives...', flush=True)
                time.sleep(5)
            print('Arrêt temporaire des conteneurs de travail ; volumes conservés.', flush=True)
            stop_attempted = True
            subprocess.run(BASE + ['stop'], cwd=ROOT, stdout=log, stderr=subprocess.STDOUT, check=True, timeout=150)
            isolated_attempted = True
            print('Démarrage de l’export Git sur volumes neufs : ' + project, flush=True)
            subprocess.run(command + ['up', '-d', '--no-build'], cwd=source, stdout=log,
                           stderr=subprocess.STDOUT, check=True, timeout=300)
            probe = '''import json,requests,psycopg2
from src.pipeline.config import s3,BUCKETS
urls={'elasticsearch':'http://elasticsearch:9200/_cluster/health','kibana':'http://kibana:5601/api/status','airflow':'http://airflow-webserver:8080/health','minio':'http://minio:9000/minio/health/live'}
result={}
for name,url in urls.items():
 r=requests.get(url,timeout=10);r.raise_for_status();result[name]=r.status_code
 if name=='airflow':
  health=r.json();assert health['metadatabase']['status']=='healthy' and health['scheduler']['status']=='healthy'
r=requests.get('http://spark:8080/json/',timeout=10);r.raise_for_status();master=r.json()
result['alive_workers']=master['aliveworkers'];assert master['aliveworkers']==2
client=s3();names={b['Name'] for b in client.list_buckets()['Buckets']};assert set(BUCKETS)<=names
result['buckets']=sorted(names)
assert all(client.list_objects_v2(Bucket=b,MaxKeys=1).get('KeyCount',0)==0 for b in BUCKETS)
result['fresh_buckets_empty']=True
assert requests.head('http://elasticsearch:9200/arxiv-papers-enriched',timeout=10).status_code==404
result['fresh_gold_absent']=True
with psycopg2.connect(host='postgres',dbname='scipulse',user='scipulse',password='scipulse') as conn:
 with conn.cursor() as cursor:
  cursor.execute("select schema_name from information_schema.schemata where schema_name in ('staging','marts')")
  schemas=sorted(row[0] for row in cursor.fetchall());assert schemas==['marts','staging'];result['dbt_schemas']=schemas
r=requests.get('http://airflow-webserver:8080/api/v1/dags',auth=('admin','admin'),timeout=10);r.raise_for_status()
dags={d['dag_id']:d['is_paused'] for d in r.json()['dags']}
assert dags.get('scipulse_arxiv_pipeline') is True and dags.get('scipulse_hn_poller') is True
result['dags_paused']=dags
print(json.dumps(result))'''
            deadline = time.monotonic() + 240
            while True:
                checked = subprocess.run(command + ['exec', '-T', 'airflow-scheduler', 'python', '-c', probe],
                                         cwd=source, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                         text=True, encoding='utf-8', timeout=60)
                if checked.returncode == 0:
                    report['probes'] = json.loads(checked.stdout.strip().splitlines()[-1])
                    break
                log.write(checked.stdout + '\n'); log.flush()
                if time.monotonic() > deadline:
                    raise RuntimeError('Services isolés non prêts : ' + checked.stdout[-1500:])
                print('Attente des services et des deux workers...', flush=True)
                time.sleep(5)
            tested = subprocess.run(command + ['exec', '-T', 'airflow-scheduler', 'python', '-m', 'pytest', 'tests', '-q'],
                                    cwd=source, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                    text=True, encoding='utf-8', timeout=90)
            log.write(tested.stdout);log.flush()
            report['export_tests_output'] = tested.stdout
            report['export_tests_passed'] = tested.returncode == 0 and '20 passed' in tested.stdout
            if not report['export_tests_passed']:
                raise RuntimeError('Tests de l’export en échec')
            report['isolated_start_success'] = True
            print('Export démarré, 2 workers, 7 buckets neufs, DAGs en pause et 20 tests réussis.', flush=True)
        except Exception as exc:
            report['error'] = str(exc)
        finally:
            if isolated_attempted:
                # Projet unique créé ci-dessus ; aucune ressource de travail n'est visée.
                try:
                    cleaned = subprocess.run(command + ['down', '-v', '--remove-orphans'], cwd=source,
                                             stdout=log, stderr=subprocess.STDOUT, timeout=150)
                    report['isolated_cleanup_success'] = cleaned.returncode == 0
                except Exception as exc:
                    report['cleanup_error'] = str(exc)
            if stop_attempted:
                print('Restauration de la stack de travail avec ses volumes existants.', flush=True)
                try:
                    restored = subprocess.run(BASE + ['up', '-d', '--no-build', *running], cwd=ROOT,
                                              stdout=log, stderr=subprocess.STDOUT, timeout=240)
                    report['restore_command_success'] = restored.returncode == 0
                except Exception as exc:
                    report['restore_error'] = str(exc)
            deadline = time.monotonic() + 180
            while True:
                try:
                    for dag, state in paused.items():
                        airflow('PATCH', 'dags/' + dag, json={'is_paused': state})
                    count = requests.get('http://localhost:9200/arxiv-papers-enriched/_count', timeout=10)
                    count.raise_for_status()
                    report['work_gold_count_after'] = count.json()['count']
                    report['work_gold_preserved'] = count.json()['count'] == report['work_gold_count_before']
                    report['work_dag_pause_states_restored'] = True
                    break
                except requests.RequestException:
                    if time.monotonic() > deadline:
                        report['restore_error'] = 'Services de travail non prêts après restauration'
                        break
                    time.sleep(5)
            report['success'] = bool(report.get('isolated_start_success') and report.get('isolated_cleanup_success')
                                     and report.get('work_gold_preserved') and report.get('work_dag_pause_states_restored'))
            (ROOT/'reports/cold_start.json').write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')
    print(json.dumps(report, ensure_ascii=False, indent=2), flush=True)
    if not report['success']:
        raise RuntimeError('Vérification incomplète ; consulter reports/cold_start.json et cold_start.log')


if __name__ == '__main__':
    main()
