# SciPulse — Du pipeline de données au dashboard Kibana augmenté

SciPulse rapproche les publications ArXiv, les mentions Hacker News et les citations OpenAlex pour calculer des indicateurs de veille scientifique.

**Périmètre documenté : de l'infrastructure au dashboard Kibana augmenté, inclus.** Ce document ne revendique pas la finalisation de l'ensemble du brief.

## Parcours des données

ArXiv, Hacker News et OpenAlex → Bronze JSONL dans MinIO → nettoyage et validation Great Expectations → Silver Parquet dans MinIO → jointures et indicateurs Spark → Gold Elasticsearch → dashboard Kibana augmenté.

Un miroir PostgreSQL permet les transformations dbt. Spark lit directement les trois Parquet Silver, sans utiliser les tables dbt.

## Infrastructure

La stack Docker regroupe Elasticsearch, Kibana, Logstash, Airflow, PostgreSQL, MinIO et Spark. Spark utilise un master et deux workers Docker sur le même ordinateur. Airflow héberge le driver.

Depuis le dossier `scipulse`, dans PowerShell :

```powershell
docker compose -f docker-compose.yml -f docker-compose.project.yml up -d --build
docker compose -f docker-compose.yml -f docker-compose.project.yml ps
```

Docker Desktop doit utiliser des conteneurs Linux. Environ 16 Go de RAM sur le poste et idéalement 12 Go alloués à Docker sont recommandés pour toute la stack pendant le calcul. Lors de la vérification sur ce poste avec environ 8 Go pour Docker, Kibana et Logstash ont été suspendus temporairement pendant Spark, puis redémarrés.

| Interface | Adresse |
|---|---|
| Airflow | http://localhost:8080 |
| MinIO | http://localhost:9001 |
| Master Spark | http://localhost:8090 |

Les valeurs par défaut du Compose permettent le démarrage local. `OPENALEX_API_KEY` est facultative dans la configuration ; si une clé est nécessaire pour l'accès ou les quotas de l'API, la définir localement. Les connexions entre conteneurs utilisent leurs noms Docker. Sous Linux, définir `AIRFLOW_UID` selon l'utilisateur du poste. Le fichier `.env` personnel ne doit pas être ajouté à Git ; tu peux également laisser tes modifications de `.env.example` hors de la sélection.

### Déclencher et vérifier le parcours

Après le démarrage et lorsque les services sont prêts, depuis `scipulse` :

```powershell
.\project.ps1 run
```

Cette commande active le polling HN et déclenche le DAG principal. Elle n'attend pas sa fin : suivre `scipulse_arxiv_pipeline` dans Airflow. Le parcours va jusqu'au contrôle Gold et à la publication Kibana. Une relance peut actualiser les collectes, les indicateurs et les résultats.

Pour vérifier le code sans relancer les collectes :

```powershell
.\project.ps1 test
```

La construction Docker et l'exécution utilisent `docker/runtime/requirements.txt` ; le dossier `.venv` du poste n'est pas nécessaire aux conteneurs et ne doit pas être partagé.

## 1. Collecter les trois sources dans Bronze

`src/pipeline/bronze.py` écrit les données brutes en JSONL dans les buckets `arxiv-raw`, `hn-raw` et `citations-raw`.

- **ArXiv** : sélection par échantillonnage réservoir de 100 000 articles CS/stat.ML, avec graine 42, complétée par des articles réellement référencés sur HN. Le corpus obtenu contient 100 075 articles. Un corpus local déjà préparé peut être réutilisé.
- **Hacker News** : collecte des stories via Firebase et d'un historique de liens ArXiv via Algolia. Les collectes Firebase sont horodatées dans MinIO. Le polling sans collecte historique conserve l'objet Algolia précédent.
- **OpenAlex** : recherche sur un lot initial de 5 000 DOI du corpus ArXiv et un complément lié à HN. Les résultats sont mis en cache pour limiter les appels réseau. L'absence de résultat signifie « citations inconnues ».

L'ordre de préparation est HN, ArXiv, puis OpenAlex : la sélection ArXiv tient compte de l'historique HN et OpenAlex utilise les DOI du corpus ArXiv.

## 2. Nettoyer et valider les données Silver

`src/pipeline/silver.py` harmonise les identifiants, les DOI, les textes, les catégories et les dates, puis retire les doublons. Les résumés courts sont signalés et les citations manquantes restent nulles.

Great Expectations applique sept attentes à ArXiv, six à HN et cinq aux citations. Une validation en échec bloque la publication du Parquet de la source concernée.

| Source | Objet Silver dans MinIO |
|---|---|
| ArXiv | `arxiv-clean/current/data.parquet` |
| HN | `hn-clean/current/data.parquet` |
| OpenAlex | `citations-clean/current/data.parquet` |

Les copies locales sont dans `data/project/`. Les résultats de qualité sont dans `reports/silver_*.json` et les Data Docs dans `gx/uncommitted/data_docs/local_site/index.html`.

## 3. Transformer avec dbt

`src/pipeline/dbt_mirror.py` copie les colonnes utiles d'ArXiv Silver dans PostgreSQL. Les trois modèles dbt préparent les articles, construisent une dimension des catégories et agrègent les publications par mois et catégorie. Leurs six tests ont été exécutés avec succès.

## 4. Rapprocher les sources et calculer les indicateurs Spark

`src/analytics/spark_gold.py` lit les trois sources Silver dans MinIO. ArXiv constitue la table d'ancrage.

### Justification des jointures

Les jointures sont gauches pour conserver les articles sans correspondance. Les citations sont raccordées par DOI et HN par identifiant ArXiv extrait des URL. Les citations sont dédupliquées par DOI et HN est agrégé par article avant jointure pour éviter de multiplier les lignes.

Les tables d'enrichissement sont petites par rapport au corpus ArXiv : le broadcast permet de les diffuser aux executors. Les plans broadcast et sort-merge sont conservés dans `reports/spark_plans.json`. Cette comparaison porte sur les plans ; ce n'est pas un benchmark chronométré.

### Indicateurs temporels et TF-IDF

Les fenêtres `lag` calculent la croissance annuelle des publications par catégorie et l'évolution des citations annuelles. La croissance est définie uniquement pour des années consécutives. La vélocité récente correspond aux citations observées sur l'année courante et la précédente, divisées par deux ; elle n'est pas annualisée pour l'année courante.

Le pipeline MLlib applique `RegexTokenizer`, `StopWordsRemover`, `HashingTF` sur 2 048 dimensions et `IDF`. Le modèle est conservé dans MinIO. Trois textes hors ajustement, dont un vide, vérifient que la transformation produit des vecteurs finis de la bonne dimension.

`tfidf_peak` représente le poids maximal du vecteur. Les mots-clés exposés sont les premiers mots distincts après filtrage, et non un classement par poids TF-IDF. Le TF-IDF n'entre pas dans le score d'impact.

### Score d'impact

```text
C = log(1 + citations) / log(1 + maximum des citations du corpus)
H = log(1 + score HN cumulé) / log(1 + maximum du score HN du corpus)
R = exp(-âge en années / 5)

impact_score = 100 × (0,50 × C + 0,30 × H + 0,20 × R)
```

Les logarithmes réduisent la domination des très gros compteurs. Les poids privilégient l'influence académique (50 %), puis l'attention tech (30 %) et la récence (20 %). Ils constituent une heuristique explicite, pas des paramètres appris ni une mesure objective de qualité scientifique.

Quand les citations sont inconnues, leur contribution vaut zéro, mais `citation_count` reste nul et `score_is_lower_bound` vaut `true`. Les scores dépendent du corpus, de sa couverture et de la date du calcul.

Le job écrit les articles enrichis dans `arxiv-papers-enriched` avec le connecteur Elasticsearch-Hadoop. Les agrégats temporels et le modèle TF-IDF sont conservés dans MinIO.

### Résultats du calcul exécuté

| Indicateur | Résultat |
|---|---:|
| Articles traités | 100 075 |
| Articles reliés à OpenAlex | 4 958, soit environ 4,95 % |
| Articles avec une mention HN observée | 81 |
| Articles avec les deux signaux | 13 |
| Tâches terminées par les deux executors | 108 et 55 |
| Tâches Spark en échec | 0 |

Ces résultats décrivent un instantané. Le complément ciblé HN introduit un biais de sélection ; ils ne sont pas représentatifs de toute la production scientifique. Une absence de HN signifie « non observé dans cette collecte ».

## 5. Écrire la Gold et orchestrer le parcours dans Airflow

Le job `src/analytics/spark_gold.py` crée le mapping si l'index n'existe pas puis écrit le DataFrame avec le connecteur `elasticsearch-hadoop` (`org.elasticsearch.spark.sql`). L'identifiant `arxiv_id` sert de clé Elasticsearch pour remplacer les articles existants sans les dupliquer. Après une écriture réussie, les documents qui n'appartiennent plus au nouveau calcul sont retirés.

Dans Kibana Dev Tools, vérifier l'index et son volume :

```http
GET _cat/indices/arxiv-papers-enriched?v
GET arxiv-papers-enriched/_count
```

Le compteur observé pendant la revue était de **100 075 documents**.

Le DAG `scipulse_arxiv_pipeline` utilise un `BashOperator` pour soumettre `spark_gold` au master `spark://spark:7077`. Les Silver et dbt doivent réussir avant Spark. La validation Gold puis la publication Kibana suivent le calcul. Le DAG prévoit une nouvelle tentative en cas d'échec.

Dans Airflow, ouvrir http://localhost:8080, sélectionner le DAG puis une exécution réussie. Les logs de `spark_gold` montrent `spark-submit`, `GOLD_RESULT`, `CLUSTER_RESULT` et le code de sortie zéro. La preuve enregistrée dans `reports/airflow_cluster_execution.json` indique la réussite des dix tâches après reprise de Docker. Il ne s'agit pas d'un nouveau lancement à chaque consultation.

`src/pipeline/validate_gold.py` contrôle les résultats avant publication. Le rapport enregistré `reports/gold_validation.json` contient sept contrôles de cohérence réussis et six attentes Great Expectations réussies. La formule du score y est contrôlée sur un échantillon de 100 documents.

## 6. Explorer le dashboard Kibana augmenté

Ouvrir http://localhost:5601/app/dashboards#/view/scipulse-insights.

`src/pipeline/dashboards.py` crée la Data View sur `arxiv-papers-enriched`, les compteurs, les catégories, l'évolution des publications, les profils d'impact, le tableau de croissance et les articles triés par score. La tâche Airflow `publish_dashboards` publie ces objets.

Pour retrouver les articles très cités sans mention HN observée :

```text
citation_count >= 50 and hn_matched: false
```

Pour retrouver les articles populaires sur HN et peu cités :

```text
hn_score >= 50 and citation_count < 10
```

Ces deux filtres ont été vérifiés dans Kibana par l'utilisatrice pendant la revue. Les seuils sont des choix de démonstration. Une absence de HN n'établit pas qu'un article n'a jamais été discuté ; une citation inconnue ne signifie pas zéro citation.

La vérification navigateur enregistrée dans `reports/browser_checks.json` ne relevait aucune erreur visible et confirmait 4 063 articles sous le filtre `primary_category: "cs.AI"`. Les six captures personnelles présentes sont répertoriées dans [le dossier des captures](artifacts/screenshots/README.md).

**Limite du panneau de croissance :** il mesure la croissance des publications par catégorie. Il ne démontre pas la croissance des sous-domaines selon les deux signaux citations/HN combinés.

## Fichiers du parcours jusqu'au dashboard augmenté

| Fichiers | Rôle |
|---|---|
| `docker-compose.yml`, `docker-compose.project.yml` | Services Docker, volumes, réseau et cluster Spark. |
| `docker/runtime/Dockerfile`, `docker/runtime/requirements.txt`, `.dockerignore` | Construction de l'environnement et dépendances. |
| `docker/postgres/init.sql` | Initialisation PostgreSQL. |
| `docker/logstash/config/logstash.yml`, `docker/logstash/project/metrics.conf` | Configuration Logstash et événements de supervision. |
| `.env.example`, `.gitignore` | Paramètres d'exemple et exclusions Git. |
| `src/pipeline/config.py` | Connexions, chemins, buckets et rapports. |
| `src/pipeline/bronze.py` | Collecte des trois sources. |
| `src/ingestion/hn_poller.py` | Extraction des identifiants ArXiv, réutilisée par le pipeline. |
| `src/pipeline/silver.py`, `src/quality/common.py` | Nettoyage et validations Great Expectations. |
| `gx/great_expectations.yml` | Configuration qualité ; les suites et checkpoints sont créés par le code. |
| `src/pipeline/dbt_mirror.py`, `dbt/` | Miroir PostgreSQL, modèles et tests SQL. |
| `src/analytics/spark_gold.py` | Jointures, fenêtres, TF-IDF, score et écriture des résultats enrichis. |
| `airflow/dags/dag_arxiv_pipeline.py` | Orchestration Bronze, Silver, dbt, Spark, contrôle Gold et publication Kibana. |
| `src/pipeline/validate_gold.py` | Contrôles de cohérence Gold avant publication. |
| `src/pipeline/dashboards.py` | Création et export des objets Kibana. |
| `artifacts/kibana_dashboards.ndjson` | Export des objets Kibana et de leurs références. |
| `reports/gold_validation.json`, `reports/airflow_cluster_execution.json`, `reports/browser_checks.json` | Preuves enregistrées de qualité, d'orchestration et de vérification navigateur. |
| `airflow/dags/dag_hn_poller.py` | Polling HN toutes les cinq minutes. |
| `tests/test_project.py` | Tests de nettoyage, d'identifiants et de conservation de l'historique HN. |
| `docs/METHODOLOGIE_PROJET.md` | Justification des méthodes jusqu'à l'exploration Kibana. |
| `docs/RESULTATS_PROJET.md` | Résultats Silver, Spark, Gold et vérification du dashboard. |
| `docs/CONFORMITE_BRIEF.md` | Correspondance avec les attentes couvertes à cette étape. |
| `notebooks/01_qualite_et_exploration.ipynb` | Exploration des données et qualité Silver. |
| `notebooks/02_spark_score_et_validation.ipynb` | Plans, couverture, score, contrôles Gold et lecture du dashboard. |

Les notebooks contiennent des cellules de reproduction sans sorties Jupyter enregistrées. Les mesures proviennent des scripts exécutés et de leurs rapports.

`reports/spark_results.json`, `spark_plans.json`, `spark_cluster_execution.json` et `gold_summary.parquet` décrivent les sorties du calcul. Les données brutes, caches, Parquet et logs sont générés et ne doivent pas être ajoutés avec un `git add .` global.

Les supports de cours restent distincts de cette documentation. Le parcours des TP 1 à 4 est décrit dans `docs/TP_01_04_corrige.md`.

## Documents et illustrations à versionner

Le brief demande une analyse de données avec statistiques descriptives et visualisations, une explication du pipeline Spark et une procédure de validation avec captures Kibana. Les noms de fichiers sont un choix d'organisation.

Les deux notebooks et les documents de `docs/` couvrent ces explications. Les quatre graphiques de `artifacts/figures/` illustrent les catégories, les publications, la couverture et les scores ; ils sont référencés dans les documents et doivent les accompagner tant qu'ils ne sont pas remplacés par d'autres visualisations.

`build_deliverables.py` est un outil facultatif de génération des documents et graphiques. Il n'est appelé ni par le DAG ni par les commandes `project.ps1` : il peut rester hors de Git. Les documents générés peuvent être conservés sans lui.

Les six captures personnelles de l'utilisatrice, `scipulse-kibana0.png` à `scipulse-kibana5.png`, sont décrites dans [artifacts/screenshots/README.md](artifacts/screenshots/README.md). Les captures 4 et 5 montrent les filtres et leurs résultats : 881 et 3 articles. Ne pas confondre les captures avec les graphiques de `figures/`.

Les commandes d'ajout explicites, par étape, se trouvent dans [docs/PLAN_COMMITS.md](docs/PLAN_COMMITS.md). Les données, le dump, les Parquet, les caches, les logs, `.env` et `.venv/` ne font pas partie de cette sélection.

## État de la revue et limites

Le fonctionnement de la Gold, du déclenchement Spark par Airflow et des deux filtres du dashboard augmenté a été vérifié. Cela ne signifie pas que chaque exigence du brief est intégralement démontrée.

- Le corpus traité est un échantillon et un complément HN, pas le dump complet.
- L'ingestion OpenAlex utilise des appels groupés par DOI ; elle n'est pas une collecte exhaustive par pagination de tout OpenAlex.
- La vélocité récente est calculée par article. Une agrégation de vélocité par catégorie n'est pas exposée explicitement dans le dashboard actuel.
- Le tableau de croissance porte sur les publications ; les trois cas d'usage du brief sont proposés comme exemples, sans imposer trois panneaux précis.
- Le monitoring existe, mais le taux d'erreur GX et la fraîcheur ne sont pas présentés comme des séries de mesures détaillées. Le rapport Bronze/Silver affiché est un instantané.
- Les anciens index et scripts des TP restent distincts du pipeline Medallion ; leur existence ne prouve pas à elle seule une indexation intermédiaire issue de la Silver actuelle.
- Les six captures personnelles du dashboard augmenté sont présentes et ont été examinées. Elles peuvent être ajoutées à Git ; elles ne constituent pas une capture du dashboard de monitoring.

Les extensions facultatives et le conditionnement du rendu ne sont pas décrits à cette étape.
