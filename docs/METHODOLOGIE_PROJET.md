# Méthodologie — SciPulse Insights

Périmètre : collecte des trois sources, qualité Silver, transformations dbt, calcul Spark, écriture Gold, intégration Airflow et dashboards Kibana augmenté et de monitoring.

## Architecture et choix des outils

L'architecture comporte trois couches Medallion : les données brutes JSONL sont conservées dans Bronze sur MinIO ; les données nettoyées et validées sont écrites en Parquet dans Silver sur MinIO ; Spark rapproche les trois sources et publie les articles enrichis dans Elasticsearch pour constituer la Gold.

Airflow orchestre la collecte ArXiv en batch, le polling Hacker News toutes les cinq minutes, la collecte des citations, le nettoyage et le traitement Spark. La stack Docker regroupe Elasticsearch, Kibana, Logstash, Airflow, MinIO, Spark et PostgreSQL, utilisé comme métastore Airflow.

OpenAlex fournit le signal de citations académiques. Son API permet de rechercher les DOI des articles ArXiv et de récupérer les compteurs de citations ainsi que leur historique annuel. Le DOI constitue ainsi la clé de rapprochement avec les publications. La couverture obtenue est mesurée et les citations indisponibles restent inconnues.

## Qualité des données

Les enregistrements Bronze sont conservés sans normalisation. Le nettoyage Silver harmonise les identifiants, catégories, espaces, DOI et dates, puis déduplique par clé. Les résumés courts sont signalés.

Pour ArXiv, le dump source complet est conservé octet pour octet dans `arxiv-raw/snapshots/<sha256>/<nom_source>`. Le SHA-256 calculé sur le fichier local est comparé à celui obtenu par relecture de l'objet MinIO ; un ETag multipart n'est pas utilisé comme empreinte du contenu. Le rapport `reports/bronze_arxiv_snapshot.json` conserve la taille, les empreintes et le chemin de l'objet. L'échantillon de travail reste séparé dans `arxiv-raw/project/arxiv.jsonl` et alimente la Silver actuelle. L'archivage complet ne signifie donc pas que Spark traite l'intégralité du dump.

Les suites contrôlent identifiants présents/uniques, types HN, scores non négatifs, dates présentes, titres non vides et volume non nul. La suite citations contrôle DOI, identifiant OpenAlex et citations non négatives. Un score absent sur un commentaire est autorisé ; une absence de notice académique est conservée comme inconnue. Les Data Docs contiennent les résultats mesurés.

## Jointures et couverture

ArXiv constitue la table d'ancrage. Une jointure gauche sur DOI enrichit les citations ; une jointure gauche sur l'identifiant ArXiv ajoute les mentions HN agrégées. Les références HN PDF et ABS, avec ou sans version, sont normalisées vers le même identifiant. Les notices OpenAlex ne portent pas toutes un identifiant ArXiv fiable ; le raccordement académique est donc réalisé sur DOI uniquement.

Avant jointure, les citations sont dédupliquées par DOI et HN est agrégé par article pour éviter une multiplication des lignes. Les dimensions sont petites par rapport aux 100 000 articles, ce qui justifie un broadcast join. Les deux plans broadcast et sort-merge sont sauvegardés dans `reports/spark_plans.json`. Il s'agit d'une comparaison des plans, pas d'un benchmark de temps indépendant.

La couverture citations, HN et commune aux trois sources est mesurée explicitement. L'échantillon comporte un complément volontaire de références HN : il permet de démontrer les croisements, mais biaise les estimations globales de popularité.

## TF-IDF et indicateurs temporels

MLlib exécute `RegexTokenizer` → `StopWordsRemover` → `HashingTF` (2 048 dimensions) → `IDF` (minDocFreq 2). Le modèle est enregistré dans MinIO. `tfidf_peak` résume le poids maximal de l'article. Les mots-clés lexicaux exposés dans Gold ne sont pas un classement des mots par TF-IDF, car HashingTF ne conserve pas une correspondance inversible entre dimensions et mots.

Un test hors ajustement transforme trois textes nouveaux, dont un texte vide et un terme inconnu, avec le modèle ajusté, puis vérifie la dimension et la finitude des vecteurs. Il vérifie le contrat de transformation ; il ne mesure aucune précision prédictive, car aucun objectif supervisé n'est défini.

Les fenêtres Spark `lag` mesurent la croissance annuelle du nombre de publications par catégorie et l'évolution annuelle des citations. Une croissance n'est calculée que pour des années consécutives. La dernière année peut être incomplète : la comparer directement à une année complète serait trompeur. Les tendances décrivent l'échantillon et non l'intégralité d'ArXiv.

La vélocité est la moyenne annuelle des citations observées sur l'année courante et la précédente (`recent_citations / 2`). Pour l'année courante, cette valeur n'est pas annualisée. La date d'observation des citations est conservée.

Par catégorie principale et année de citation, Spark additionne les compteurs annuels OpenAlex des articles rapprochés par DOI. La sortie `s3a://citations-clean/analytics/category_yearly_velocity/` contient `category_citations` (citations observées par an), `observed_articles`, `citations_per_observed_article`, puis `citation_acceleration` et `citation_growth_pct` calculés avec `lag`. L'accélération est la différence avec l'année précédente ; le taux d'évolution exige un total précédent strictement positif. Les deux restent nuls en l'absence d'année consécutive. Une année absente n'est pas transformée en zéro. Ces indicateurs décrivent les articles couverts par OpenAlex, sans extrapolation aux articles non rapprochés. Une même notice DOI associée à plusieurs articles ArXiv est comptée pour chacun ; les volumes sont donc des sommes par article ArXiv, pas des citations uniques. L'année courante est partielle et les changements de couverture entre années peuvent influencer les comparaisons.

Le calcul est intégré au job Gold. L'option `--category-velocity-only` recalcule uniquement cette sortie à partir de Silver et de l'historique annuel déjà exporté, sans réindexer Elasticsearch. Le dashboard actuel n'expose pas encore cette table.

L'exécution ciblée a produit 1 466 couples catégorie/année pour 128 catégories. Le rapport `reports/category_velocity.json` contient le chemin MinIO et un extrait des résultats. Cette exécution utilise Spark local avec un cœur ; le traitement complet reste soumis au cluster par Airflow.

## Score d'impact composite

Pour chaque article :

- `C = log(1 + citations) / log(1 + max_citations_du_corpus)` ;
- `H = log(1 + score_HN_cumulé) / log(1 + max_score_HN_du_corpus)` ;
- `R = exp(-âge_en_années / 5)` ;
- **`impact_score = 100 × (0,50 C + 0,30 H + 0,20 R)`**.

Les logarithmes limitent la domination des très gros compteurs. Les 50 % académiques reflètent l'objectif de veille scientifique ; les 30 % HN captent l'attention de la communauté tech ; les 20 % de récence permettent aux travaux nouveaux de rester visibles. Ces poids sont une heuristique explicite et modifiable, pas des paramètres appris ou une vérité scientifique.

Quand les citations sont inconnues, le terme académique apporte zéro au score, mais `citation_count` reste null et `score_is_lower_bound` est vrai. Il faut comparer en priorité les articles de couverture semblable, ou filtrer `citation_matched: true`. Une absence de HN dans la fenêtre de collecte ne constitue pas une preuve d'absence historique.

La normalisation par maximum dépend du corpus ; les scores de deux runs différents ne sont donc pas directement comparables. Les citations favorisent les travaux anciens, HN représente une audience particulière et le score mesure un mélange d'influence et d'attention, pas la qualité intrinsèque d'un article. Aucune causalité n'est inférée.

## Contrôles du traitement Spark

Le notebook de traitement lit le résumé produit par Spark pour contrôler l'unicité des identifiants, les bornes du score et la conservation des citations inconnues. Le job vérifie aussi la transformation TF-IDF de trois textes hors ajustement et conserve les compteurs de tâches par executor dans `reports/spark_cluster_execution.json`.

La validation sur jeu de test couvre aussi des formats hors du jeu réel : DOI multiples, DOI invalides, version ArXiv, lien trompeur, résumé absent et convergence des identifiants HN Firebase/Algolia.

## Écriture Gold et orchestration Airflow

Le DataFrame enrichi est écrit dans `arxiv-papers-enriched` avec le connecteur Elasticsearch-Hadoop. Le mapping distingue les textes, les dates, les compteurs, les scores et les indicateurs booléens. `arxiv_id` est utilisé comme identifiant de document, ce qui évite de créer un doublon lors d'une réécriture du même article. Le retrait des documents d'un ancien calcul intervient après une écriture réussie.

Le DAG principal soumet Spark au cluster via un `BashOperator`. Il impose les dépendances Bronze → Silver → dbt et Spark → contrôle Gold → publication Kibana. Une nouvelle tentative est prévue en cas d'échec. La preuve enregistrée dans `reports/airflow_cluster_execution.json` montre une exécution complète réussie après reprise de Docker.

Le contrôle Gold vérifie les identifiants, le nombre d'articles, les citations inconnues, les bornes du score et la formule sur 100 documents. Le rapport `reports/gold_validation.json` conserve les résultats de ces contrôles.

## Dashboard Kibana augmenté

Le dashboard **SciPulse Insights** lit uniquement l'index Gold `arxiv-papers-enriched`. Il présente le nombre d'articles, la couverture OpenAlex et HN, le score d'impact moyen, les catégories, les publications par année, les profils d'impact, la croissance des publications et un tableau d'articles trié par score.

Adresse : http://localhost:5601/app/dashboards#/view/scipulse-insights

Dans la barre KQL, appliquer successivement :

```text
citation_count >= 50 and hn_matched: false
```

Ce filtre sélectionne les articles très cités sans mention HN observée dans la collecte.

```text
hn_score >= 50 and citation_count < 10
```

Ce filtre sélectionne les articles populaires sur HN et peu cités parmi ceux dont les citations sont connues. Les seuils 50 et 10 sont des choix de démonstration.

Les deux filtres sélectionnent respectivement 881 et 3 articles dans les captures. Le rapport navigateur enregistré dans `reports/browser_checks.json` ne relevait aucune erreur visible sur le dashboard et confirmait 4 063 articles pour le filtre `primary_category: "cs.AI"`. Ce rapport décrit une vérification antérieure, pas une surveillance continue.

**Limite :** le tableau de croissance utilise `growth_pct`, calculé sur les publications. Il ne classe pas les sous-domaines selon une croissance combinée des citations et du signal HN. Le panneau ne mesure donc pas cette croissance combinée.

## Périmètre des preuves et captures

Les statistiques descriptives s'appuient sur quatre graphiques : catégories, publications, couverture et distribution du score. Ils restent liés aux documents qui les affichent ; le script qui les a générés n'est pas nécessaire pour lire ces documents.

Les six captures `scipulse-kibana0.png` à `scipulse-kibana5.png` illustrent le dashboard augmenté. Elles couvrent les compteurs, les graphiques, le classement et les deux filtres analytiques. La capture 4 affiche 881 articles très cités sans HN observé ; la capture 5 affiche 3 articles populaires HN et peu cités. Les filtres KQL sont visibles. Voir [l'inventaire des captures](../artifacts/screenshots/README.md).

La vélocité récente est calculée par article ; la croissance des publications et les compteurs annuels de citations sont calculés par catégorie. Ces calculs ne constituent pas un classement des catégories par évolution combinée citations/HN. Le monitoring présente désormais les volumes contrôlés, le taux d'échec des attentes GX et les dates de dépôt, de publication Silver et de calcul Gold, avec leur ancienneté à la publication. Le taux vaut 100 × attentes échouées / attentes évaluées ; un rapport absent n'est pas assimilé à 0 %. La tâche HN `refresh_monitoring`, exécutée avec `all_done`, renouvelle cet instantané après chaque cycle planifié. Les méthodes et limites sont détaillées dans [MONITORING.md](MONITORING.md).

## Références techniques

La procédure, les résultats et les limites des tests sont décrits dans [VALIDATION.md](VALIDATION.md). Elle distingue les contrôles sur cas synthétiques, la cohérence du corpus Gold et le test technique de transformation TF-IDF.

- Spark MLlib TF-IDF : https://spark.apache.org/docs/3.5.7/ml-features.html#tf-idf
- Connecteur officiel Elastic : https://www.elastic.co/docs/reference/elasticsearch-hadoop/apache-spark-support
- Configuration ES-Hadoop : https://www.elastic.co/docs/reference/elasticsearch-hadoop/configuration
- OpenAlex, requêtes DOI groupées : https://help.openalex.org/how-to/api-recipes/
- API HN : https://github.com/HackerNews/API
- Historique HN : https://hn.algolia.com/api

