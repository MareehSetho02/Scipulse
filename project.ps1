param([ValidateSet('up','run','test','dashboards','down')][string]$Action='up')
$ErrorActionPreference='Stop'
Push-Location $PSScriptRoot
try {
    $composeArgs=@('compose','-f','docker-compose.yml','-f','docker-compose.project.yml')
    switch($Action){
        'up' { & docker @composeArgs up -d --build }
        'run' {
            & docker @composeArgs exec -T airflow-scheduler airflow dags unpause scipulse_arxiv_pipeline
            if($LASTEXITCODE -ne 0){throw 'Activation du DAG principal échouée'}
            & docker @composeArgs exec -T airflow-scheduler airflow dags unpause scipulse_hn_poller
            if($LASTEXITCODE -ne 0){throw 'Activation du polling HN échouée'}
            & docker @composeArgs exec -T airflow-scheduler airflow dags trigger scipulse_arxiv_pipeline
        }
        'test' { & docker @composeArgs exec -T airflow-scheduler python -m pytest tests/test_project.py tests/test_monitoring.py -q }
        'dashboards' { & docker @composeArgs exec -T airflow-scheduler python -m src.pipeline.dashboards }
        'down' { & docker @composeArgs down }
    }
    if($LASTEXITCODE -ne 0){throw "Commande $Action échouée"}
} finally {Pop-Location}
