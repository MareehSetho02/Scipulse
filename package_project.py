"""Export reproductible du code, des documents, des preuves et du corpus préparé."""
import argparse
import hashlib
import json
import subprocess
import zipfile
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent
REQUIRED = ['README.md', 'docker-compose.yml', 'docker-compose.project.yml',
            'docker/runtime/Dockerfile', 'docker/runtime/requirements.txt',
            'airflow/dags/dag_arxiv_pipeline.py', 'airflow/dags/dag_hn_poller.py',
            'src/pipeline/bronze.py', 'src/pipeline/silver.py', 'src/pipeline/monitoring.py',
            'src/analytics/spark_gold.py', 'project.ps1', 'gx/great_expectations.yml',
            'dbt/dbt_project.yml', 'dbt/profiles.yml', 'artifacts/kibana_dashboards.ndjson',
            'docs/METHODOLOGIE_PROJET.md', 'docs/VALIDATION.md', 'docs/MONITORING.md',
            'docs/REPRODUCTIBILITE_ET_RENDU.md', 'reports/final_validation.json',
            'reports/cold_start.json', 'reports/tests_validation.xml', 'reports/tests_latest.xml',
            'reports/latest_validation.json', 'reports/bronze_arxiv_snapshot.json',
            'reports/category_velocity.json', 'reports/arxiv_index_validation.json',
            'src/pipeline/index_arxiv.py', 'docs/EXPLORATION_ARXIV.md',
            'src/pipeline/verify_execution.py', 'reports/final_execution_validation.json',
            'reports/arxiv_index_migration.json',
            'notebooks/01_qualite_et_exploration.ipynb', 'notebooks/02_spark_score_et_validation.ipynb',
            *[f'artifacts/screenshots/scipulse-kibana{i}.png' for i in range(6)],
            'artifacts/screenshots/monitoring-volumes-qualite.png',
            'artifacts/screenshots/monitoring-fraicheur.png']


def allowed(path):
    rel = path.relative_to(ROOT)
    return (path.is_file() and not any(p.startswith('.') or p == '__pycache__' for p in rel.parts)
            and path.suffix.lower() not in ('.pyc', '.zip', '.log')
            and path.name not in ('desktop.ini', 'delivery_manifest.json'))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--name', default='scipulse_projet4_AIA01')
    args = parser.parse_args()
    if not args.name.replace('_', '').replace('-', '').isalnum():
        parser.error('Nom : lettres, chiffres, _ et - uniquement')
    missing = [p for p in REQUIRED if not (ROOT / p).is_file()]
    if missing:
        raise RuntimeError('Fichiers manquants : ' + ', '.join(missing))
    files = set()
    folders = ['src', 'docker', 'airflow/dags', 'dbt/models', 'tests', 'docs', 'notebooks',
               'artifacts/screenshots', 'artifacts/figures', 'gx/expectations', 'gx/checkpoints',
               'gx/validation_definitions', 'gx/uncommitted/data_docs']
    for folder in folders:
        files.update(p for p in (ROOT / folder).rglob('*') if allowed(p))
    for pattern in ['reports/*.json', 'reports/tests_validation.xml', 'reports/tests_latest.xml', 'reports/gold_summary.parquet',
                    'data/project/*.jsonl', 'data/project/*.parquet', 'data/project/openalex_cache/*.json']:
        files.update(p for p in ROOT.glob(pattern) if allowed(p))
    for name in ['package_project.py', 'verify_cold_start.py', 'prepare_tp.py', 'run_tp4.py',
                 'project.ps1', 'run_tp.ps1', 'docker-compose.yml', 'docker-compose.project.yml',
                 'requirements.txt', 'README.md', 'LICENSE', 'Makefile', '.gitignore', '.dockerignore',
                 'dbt/dbt_project.yml', 'dbt/profiles.yml', 'gx/great_expectations.yml',
                 'artifacts/kibana_dashboards.ndjson']:
        if (ROOT / name).is_file():
            files.add(ROOT / name)
    commit = subprocess.run(['git', 'rev-parse', 'HEAD'], cwd=ROOT, capture_output=True, text=True)
    validation = json.loads((ROOT / 'reports/final_validation.json').read_text(encoding='utf-8'))
    cold = json.loads((ROOT / 'reports/cold_start.json').read_text(encoding='utf-8'))
    latest = json.loads((ROOT / 'reports/latest_validation.json').read_text(encoding='utf-8'))
    execution = json.loads((ROOT / 'reports/final_execution_validation.json').read_text(encoding='utf-8'))
    if not latest['success']:
        raise RuntimeError('Latest validation failed')
    if not execution['success']:
        raise RuntimeError('Full pipeline execution validation failed')
    manifest = {
        'created_at': datetime.now(timezone.utc).isoformat(),
        'git_head': commit.stdout.strip() if commit.returncode == 0 else None,
        'export': 'Working files; Git metadata and local secrets excluded',
        'archive_name_provisional': args.name == 'scipulse_projet4_AIA01',
        'validation_success': validation['success'], 'tests': validation['tests'],
        'latest_validation_success': latest['success'], 'latest_tests': latest['tests'],
        'working_stack_pipeline_rerun': execution['full_pipeline_rerun'],
        'working_stack_pipeline_run_id': execution['airflow_run_id'],
        'working_stack_pipeline_success': execution['success'],
        'cold_start_success': cold.get('success', False),
        'cold_start_source_commit': cold.get('source_commit'),
        'image_build_retested': cold.get('image_build_retested', False),
        'full_data_pipeline_retested': cold.get('full_data_pipeline_retested', False),
        'limits': ['Prepared sample, not the complete ArXiv dump',
                   'Category citation analytics are stored in MinIO, not displayed in Kibana',
                   'Cold start uses existing images; full source collection not rerun on fresh volumes',
                   'arxiv-papers aliases the verified Silver index; legacy articles retained in arxiv-papers-tp-archive; one-time migration is separate from fresh startup'],
        'sha256': {},
    }
    output = ROOT / 'artifacts' / (args.name + '.zip')
    output.parent.mkdir(exist_ok=True)
    with zipfile.ZipFile(output, 'w', compression=zipfile.ZIP_DEFLATED, compresslevel=6) as archive:
        for file in sorted(files):
            rel = file.relative_to(ROOT).as_posix()
            digest = hashlib.sha256()
            with file.open('rb') as source, archive.open('scipulse/' + rel, 'w', force_zip64=True) as target:
                for chunk in iter(lambda: source.read(1024 * 1024), b''):
                    target.write(chunk)
                    digest.update(chunk)
            manifest['sha256'][rel] = digest.hexdigest()
        content = json.dumps(manifest, ensure_ascii=False, indent=2)
        archive.writestr('scipulse/reports/delivery_manifest.json', content)
    with zipfile.ZipFile(output) as archive:
        assert archive.testzip() is None
        names = set(archive.namelist())
        assert all('scipulse/' + p in names for p in REQUIRED)
        assert not any(any(part in ('.env', '.env.example', '.venv', '.git', '.user.yml') for part in Path(n).parts) for n in names)
        assert 'scipulse/build_deliverables.py' not in names
        assert 'scipulse/capture_dashboards.py' not in names
    (ROOT / 'reports/delivery_manifest.json').write_text(content, encoding='utf-8')
    print(output, output.stat().st_size, 'octets ; CRC et fichiers requis vérifiés')


if __name__ == '__main__':
    main()
