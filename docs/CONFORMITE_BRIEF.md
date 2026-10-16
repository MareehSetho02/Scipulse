# Attentes couvertes — du démarrage au dashboard Kibana augmenté


| Attente couverte | Fichiers ou résultats |
|---|---|
| Infrastructure Docker | `docker-compose.yml`, `docker-compose.project.yml`, `docker/runtime/` |
| Trois sources | ArXiv, HN Firebase/Algolia, OpenAlex ; `src/pipeline/bronze.py` et `reports/bronze_*.json` |
| Bronze JSONL | Buckets `arxiv-raw`, `hn-raw`, `citations-raw` |
| Silver séparées en Parquet | `src/pipeline/silver.py` ; buckets `arxiv-clean`, `hn-clean`, `citations-clean` |
| Qualité Great Expectations | Sept attentes ArXiv, six HN, cinq citations ; manifestes Silver et Data Docs |
| Polling HN toutes les cinq minutes | `airflow/dags/dag_hn_poller.py` |
| Transformations dbt | `src/pipeline/dbt_mirror.py`, trois modèles SQL et six tests dans `dbt/` |
| Jointure Spark des trois sources | `src/analytics/spark_gold.py` ; DOI pour OpenAlex et identifiant ArXiv pour HN |
| Justification de la stratégie | Jointures gauches, déduplication, agrégation HN, broadcast ; `docs/METHODOLOGIE_PROJET.md` |
| Comparaison broadcast / sort-merge | Plans dans `reports/spark_plans.json`, sans benchmark chronométré |
| Couverture | 100 075 articles, 4 958 correspondances OpenAlex, 81 HN, 13 communes ; `reports/spark_results.json` |
| Fenêtres temporelles | Croissance annuelle, évolution des citations et vélocité récente ; sorties MinIO |
| TF-IDF MLlib | Modèle dans MinIO ; test de transformation de trois textes hors ajustement réussi |
| Score d'impact | Citations, popularité HN et récence ; pondérations justifiées dans la méthodologie |
| Exécution entre workers | Deux workers Docker sur un même hôte : 108 et 55 tâches terminées, aucune en échec ; `reports/spark_cluster_execution.json` |
| Écriture des résultats Spark | Connecteur Elasticsearch-Hadoop vers `arxiv-papers-enriched` ; résumé dans `reports/gold_summary.parquet` |
| Tests du code | Huit tests du pipeline réussis dans Docker ; douze tests locaux en incluant les TP |

## Gold, orchestration et dashboard

| Attente | Réalisation et vérification |
|---|---|
| Écriture Gold via Elasticsearch-Hadoop | `src/analytics/spark_gold.py` ; 100 075 documents observés dans `arxiv-papers-enriched` pendant la revue |
| Spark intégré à Airflow | `airflow/dags/dag_arxiv_pipeline.py` ; `BashOperator` et preuve de réussite dans `reports/airflow_cluster_execution.json` |
| Contrôle des résultats Gold | `src/pipeline/validate_gold.py` ; sept contrôles de cohérence et six attentes GX réussis dans le rapport enregistré |
| Dashboard augmenté sur Gold | `src/pipeline/dashboards.py` ; compteurs, score d'impact, citations, profils et tableau filtrable |
| Articles très cités sans HN observé | Filtre `citation_count >= 50 and hn_matched: false`, vérifié dans Kibana par l'utilisatrice |
| Articles populaires HN et peu cités | Filtre `hn_score >= 50 and citation_count < 10`, vérifié dans Kibana par l'utilisatrice |
| Croissance des sous-domaines selon les signaux combinés | **Partiellement couvert :** le panneau existant mesure la croissance des publications, sans classement selon la croissance combinée citations/HN |
| Preuves de consultation | Rapport navigateur antérieur dans `reports/browser_checks.json` ; filtres confirmés par l'utilisatrice ; six captures personnelles présentes : `scipulse-kibana0.png` à `scipulse-kibana5.png`, décrites dans `artifacts/screenshots/README.md` |

Les rapports et captures décrivent des vérifications enregistrées ; ils ne garantissent pas l'état permanent des services.

## Exigences à distinguer des choix d'implémentation

| Exigence du brief | État ou limite à cette étape |
|---|---|
| Analyse des données avec statistiques et visualisations | Documents et notebooks avec graphiques dans `artifacts/figures/` ; noms et générateur non imposés |
| Document ou notebook sur Spark | Méthodologie et notebook Spark : source, jointures, indicateurs et score |
| Procédure de validation et captures Kibana | Contrôles et filtres documentés ; six captures personnelles examinées, dont les filtres affichant 881 et 3 résultats |
| Volumes, taux d'erreur GX et fraîcheur dans le monitoring | Tableau de volumes et état qualité présents ; indicateurs détaillés de taux d'erreur et de fraîcheur à revoir |
| Exploration ArXiv du module 1 | Anciens index et TP présents ; leur provenance ne démontre pas automatiquement une indexation de la Silver Medallion actuelle |
| Indicateurs temporels par catégorie | Croissance des publications par catégorie ; vélocité récente par article, sans agrégation par catégorie explicitement exposée |
| Ingestion complète des données sources | Corpus ArXiv échantillonné ; appels OpenAlex groupés par DOI, sans pagination exhaustive |

Le brief présente les trois questions analytiques du dashboard augmenté comme des exemples de cas d'usage. Le panneau de croissance combinée manquant n'est donc pas présenté comme un panneau explicitement obligatoire ; sa couverture reste décrite sans la surestimer.

## Limites des résultats

- Corpus CS/stat.ML échantillonné et complété par des références HN, non représentatif de l'intégralité d'ArXiv.
- Couverture OpenAlex partielle ; une citation manquante reste inconnue.
- Collecte HN non exhaustive ; absence de correspondance signifie absence observée dans cette collecte.
- Score heuristique dépendant de la date, du corpus et des maxima de normalisation.
- Comparaison des plans de jointure, sans mesure indépendante de leurs temps d'exécution.

La preuve Spark est issue du calcul exécuté après reprise de Docker. Elle ne constitue pas une validation sur plusieurs machines physiques.
