# Exploration ArXiv depuis Silver

`src/pipeline/index_arxiv.py` lit le Parquet `arxiv-clean/current/data.parquet` directement dans MinIO et publie ses articles dans un nouvel index `arxiv-papers-silver-<identifiant>`. Chaque lancement crée un index distinct ; il ne remplace aucun index existant. Ce module se lance manuellement, indépendamment du DAG Gold.

Les identifiants et catégories sont des champs `keyword`, les dates des champs `date`, et les titres, résumés et auteurs des champs `text`. Les titres et résumés utilisent l'analyseur anglais. L'identifiant Elasticsearch est l'identifiant ArXiv.

```powershell
docker exec scipulse-airflow-sched python -m src.pipeline.index_arxiv
```

Le traitement vérifie le nombre de documents et compare tous les effectifs par catégorie à ceux de la Silver. Il exécute également une recherche plein texte `neural network`, un histogramme annuel et une recherche des publications récentes de `cs.AI`. Les résultats et l'ETag de l'objet source figurent dans `reports/arxiv_index_validation.json`.

L'index historique `arxiv-papers` reste distinct de cette sortie. Son remplacement n'est pas réalisé par ce script. L'index enrichi `arxiv-papers-enriched` reste alimenté par le traitement Spark.

Exécution vérifiée : `arxiv-papers-silver-7b2c13bd8aef`, 100 075 documents. Le nombre de documents et tous les effectifs par catégorie correspondent à la Silver.
