# Méthodologie — SciPulse Insights

Périmètre : collecte des trois sources, qualité Silver, transformations dbt, calcul Spark, écriture Gold, intégration Airflow et dashboard Kibana augmenté.

## Architecture et choix des outils

L'architecture suit les trois couches Medallion demandées : les données brutes JSONL sont conservées dans Bronze sur MinIO ; les données nettoyées et validées sont écrites en Parquet dans Silver sur MinIO ; Spark rapproche les trois sources et publie les articles enrichis dans Elasticsearch pour constituer la Gold.

Airflow orchestre la collecte ArXiv en batch, le polling Hacker News toutes les cinq minutes, la collecte des citations, le nettoyage et le traitement Spark. La stack Docker regroupe Elasticsearch, Kibana, Logstash, Airflow, MinIO, Spark et PostgreSQL, utilisé comme métastore Airflow.

OpenAlex fournit le signal de citations académiques. Son API permet de rechercher les DOI des articles ArXiv et de récupérer les compteurs de citations ainsi que leur historique annuel. Le DOI constitue ainsi la clé de rapprochement avec les publications. La couverture obtenue est mesurée et les citations indisponibles restent inconnues.

## Qualité des données

Les enregistrements Bronze sont conservés sans normalisation. Le nettoyage Silver harmonise les identifiants, catégories, espaces, DOI et dates, puis déduplique par clé. Les résumés courts sont signalés.

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

Les deux filtres ont été vérifiés dans Kibana par l'utilisatrice au cours de la revue. Le rapport navigateur enregistré dans `reports/browser_checks.json` ne relevait aucune erreur visible sur le dashboard et confirmait 4 063 articles pour le filtre `primary_category: "cs.AI"`. Ce rapport décrit une vérification antérieure, pas une surveillance continue.

**Limite :** le tableau de croissance utilise `growth_pct`, calculé sur les publications. Il ne classe pas les sous-domaines selon une croissance combinée des citations et du signal HN. Ce cas d'usage du brief n'est donc pas démontré par ce panneau.

## Périmètre des preuves et captures

Les statistiques descriptives et les graphiques de catégories, de publications, de couverture et de score répondent au contenu d'analyse demandé. Leurs quatre noms de fichiers ne sont pas imposés. Ils restent liés aux documents qui les affichent ; le script qui les a générés n'est pas nécessaire pour lire ces documents.

Les six captures personnelles `scipulse-kibana0.png` à `scipulse-kibana5.png` sont présentes et ont été examinées. Elles couvrent les compteurs, les graphiques, le classement et les deux filtres analytiques. La capture 4 affiche 881 articles très cités sans HN observé ; la capture 5 affiche 3 articles populaires HN et peu cités. Les filtres KQL sont visibles. Voir [l'inventaire des captures](../artifacts/screenshots/README.md).

La vélocité récente décrite ici est calculée par article ; la croissance des publications est calculée par catégorie. Ces calculs ne constituent pas un classement des catégories par évolution combinée citations/HN. Le monitoring existant présente un instantané Bronze/Silver ; son taux d'erreur GX et sa fraîcheur détaillée restent à examiner séparément.

## Références techniques

- Spark MLlib TF-IDF : https://spark.apache.org/docs/3.5.7/ml-features.html#tf-idf
- Connecteur officiel Elastic : https://www.elastic.co/docs/reference/elasticsearch-hadoop/apache-spark-support
- Configuration ES-Hadoop : https://www.elastic.co/docs/reference/elasticsearch-hadoop/configuration
- OpenAlex, requêtes DOI groupées : https://help.openalex.org/how-to/api-recipes/
- API HN : https://github.com/HackerNews/API
- Historique HN : https://hn.algolia.com/api

