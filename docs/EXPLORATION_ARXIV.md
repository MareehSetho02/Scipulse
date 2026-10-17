# Exploration ArXiv depuis Silver

`src/pipeline/index_arxiv.py` lit le Parquet `arxiv-clean/current/data.parquet` directement dans MinIO et publie ses articles dans un nouvel index `arxiv-papers-silver-<identifiant>`. Chaque lancement crée un index distinct ; il ne remplace aucun index existant. Ce module se lance manuellement, indépendamment du DAG Gold.

Les identifiants et catégories sont des champs `keyword`, les dates des champs `date`, et les titres, résumés et auteurs des champs `text`. Les titres et résumés utilisent l'analyseur anglais. L'identifiant Elasticsearch est l'identifiant ArXiv.

```powershell
docker exec scipulse-airflow-sched python -m src.pipeline.index_arxiv
```

Le traitement vérifie le nombre de documents et compare tous les effectifs par catégorie à ceux de la Silver. Il exécute également une recherche plein texte `neural network`, un histogramme annuel et une recherche des publications récentes de `cs.AI`. Les résultats et l'ETag de l'objet source figurent dans `reports/arxiv_index_validation.json`.

`arxiv-papers` est un alias vers l'index Silver validé `arxiv-papers-silver-7b2c13bd8aef`. Les recherches peuvent utiliser directement ce nom stable. L'ancien index est conservé dans `arxiv-papers-tp-archive`, en lecture seule, avec ses 100 000 articles et son mapping d'origine. La copie a été réalisée par clonage après blocage des écritures, puis le nom a été basculé atomiquement. Le rapport est `reports/arxiv_index_migration.json` ; le script ponctuel est `src/pipeline/migrate_arxiv_index.py`. Ce script refuse d'écraser une archive existante.

`src/pipeline/index_arxiv.py` continue de créer un nouvel index distinct à chaque lancement ; il ne déplace pas automatiquement cet alias. L'index enrichi `arxiv-papers-enriched` reste alimenté par le traitement Spark et conserve ses 100 075 articles.

Exécution vérifiée : `arxiv-papers-silver-7b2c13bd8aef`, 100 075 documents. Le nombre de documents et tous les effectifs par catégorie correspondent à la Silver.
