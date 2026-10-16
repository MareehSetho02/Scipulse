# Monitoring du pipeline Medallion

## Indicateurs et sources

Le dashboard est disponible à http://localhost:5601/app/dashboards#/view/scipulse-monitoring.

| Information | Source de la mesure |
|---|---|
| Volumes Bronze et Silver contrôlés | Dernier manifeste `silver_*.json` dans MinIO, bucket `pipeline-metrics` |
| Volume Gold | Nombre de documents dans `arxiv-papers-enriched` ; les compteurs Kibana interrogent également cet index |
| Taux d'échec GX | Nombre d'attentes échouées divisé par le nombre d'attentes évaluées, par source et pour la Gold |
| Dernier dépôt Bronze | Date de dernière modification du plus récent objet JSONL de la source dans MinIO |
| Dernière Silver publiée | Date de modification de l'objet Parquet `current/data.parquet` dans MinIO |
| Dernier contrôle qualité | Horodatage du manifeste de validation |
| Dernier calcul Gold | Maximum de `pipeline_updated_at` dans les documents Elasticsearch |
| Ancienneté | Différence entre la date de publication du monitoring et chacun de ces horodatages |

Le taux GX est un taux de **contrôles en échec**, pas un pourcentage de lignes incorrectes. Une attente peut concerner plusieurs lignes ou l'ensemble de la table. Les rapports absents ou sans attentes sont affichés comme indisponibles, jamais comme 0 %.

Les volumes Bronze et Silver sont ceux du dernier jeu contrôlé : ils ne prétendent pas mesurer le nombre de lignes de fichiers arrivés après ce contrôle. Si la qualité échoue, les lignes Silver contrôlées ne sont pas publiées ; le dernier Parquet valide et sa date restent visibles.

## Actualisation

Le DAG HN enchaîne `bronze_hn` → `silver_hn` → `refresh_monitoring` toutes les cinq minutes. La dernière tâche utilise `all_done` : elle peut publier le dernier état disponible même si la qualité Silver échoue. Un échec de connexion à MinIO, Elasticsearch ou Kibana reste une erreur de tâche et bénéficie des retries du DAG.

Le DAG principal republie aussi le monitoring lors de sa tâche `publish_dashboards`. Le mode de publication ciblé ne modifie pas le dashboard d'exploration `scipulse-insights`.

Le tableau est un instantané renouvelé par Airflow. Ses anciennetés sont calculées à la publication, pas en continu dans le navigateur. Recharger le dashboard pour voir la dernière version. Un retard du scheduler, un DAG en pause ou un échec de publication peut retarder le renouvellement ; la date de publication permet de le constater.

La fraîcheur des objets mesure leur dépôt dans la plateforme, pas une actualisation chez le fournisseur. OpenAlex peut être réutilisé depuis le cache. Le polling HN ne recalcule pas automatiquement la Gold.

Pour republier uniquement le monitoring, sans relancer Spark ni recréer le dashboard augmenté :

```powershell
docker compose -f docker-compose.yml -f docker-compose.project.yml exec -T airflow-scheduler python -m src.pipeline.dashboards --monitoring-only
```

## Fichiers concernés

- `src/pipeline/monitoring.py` : lecture des manifestes et métadonnées, calcul des taux et anciennetés, génération du tableau.
- `src/pipeline/dashboards.py` : publication Kibana et export NDJSON ; option `--monitoring-only`.
- `airflow/dags/dag_hn_poller.py` : actualisation après chaque cycle HN.
- `tests/test_monitoring.py` : taux pondéré par le nombre d'attentes, états absents, échecs, horloges futures et préservation du dashboard augmenté.
- `project.ps1` : la commande `test` exécute les tests du pipeline et du monitoring.
- `reports/monitoring.json` : dernier instantané, également publié dans MinIO `pipeline-metrics/monitoring.json`.
- `reports/monitoring_validation.json` : résultats des contrôles de publication, navigateur et Airflow.
- `artifacts/kibana_dashboards.ndjson` : export des dashboards et références après publication.

Les tests du pipeline et du monitoring ont passé ensemble dans Docker : 16 tests. Le contrôle navigateur a vérifié l'affichage des taux, des dates UTC, des anciennetés et l'absence d'erreurs visibles. Une comparaison des attributs du dashboard augmenté avant et après publication a confirmé qu'il était conservé.

## Capture du dashboard

La capture [monitoring-fraicheur.png](../artifacts/screenshots/monitoring-fraicheur.png) présente les dernières écritures et les anciennetés par source et par couche.

La capture [monitoring-volumes-qualite.png](../artifacts/screenshots/monitoring-volumes-qualite.png) présente les volumes contrôlés et les taux d'échec GX.

## Ajout Git manuel

```powershell
git add -- src/pipeline/monitoring.py src/pipeline/dashboards.py
git add -- airflow/dags/dag_hn_poller.py tests/test_monitoring.py
git add -- project.ps1
git add -- docs/MONITORING.md
git add -- reports/monitoring.json reports/monitoring_validation.json
git add -- artifacts/kibana_dashboards.ndjson
```

Ajouter aussi les documents mis à jour après relecture.
