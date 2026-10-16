# Validation du pipeline et des résultats analytiques

## Périmètre

La validation couvre les transformations, les identifiants, la qualité Gold, la cohérence entre les sorties Spark et Elasticsearch, les requêtes analytiques et le monitoring. Les commandes de contrôle ci-dessous ne relancent ni la collecte ni le calcul Spark. Le contrôle Gold publie ses résultats dans MinIO et les Data Docs, sans réécrire les articles Elasticsearch.

## 1. Tests sur des cas contrôlés

Depuis le dossier `scipulse`, avec la stack démarrée :

```powershell
docker compose -f docker-compose.yml -f docker-compose.project.yml exec -T airflow-scheduler python -m pytest tests -q --junitxml=/opt/airflow/reports/tests_validation.xml
```

| Fichier | Cas couverts | Tests réussis |
|---|---|---:|
| `tests/test_tp.py` | Transformation d'article, liens ArXiv, nettoyage idempotent, refus d'accès MinIO | 4 |
| `tests/test_project.py` | DOI, identifiants ArXiv, convergence Firebase/Algolia, résumés absents, citations inconnues, conservation de l'historique HN | 8 |
| `tests/test_monitoring.py` | Taux GX pondéré, rapports absents ou incomplets, échec qualité, ancienneté du dernier Parquet valide, horloge future, conservation du dashboard augmenté | 8 |
| **Total** | **Aucun échec, erreur ou test ignoré** | **20** |

Le rapport machine est `reports/tests_validation.xml`. Les entrées synthétiques permettent de vérifier des formats et situations indépendants des articles du corpus. Elles n'évaluent pas la représentativité statistique du corpus.

La commande `project.ps1 test` cible les 16 tests du pipeline et du monitoring ; la commande complète ci-dessus inclut également les quatre tests des TP.

## 2. Contrôles Gold

```powershell
docker compose -f docker-compose.yml -f docker-compose.project.yml exec -T airflow-scheduler python -m src.pipeline.validate_gold
```

Les sept contrôles enregistrés dans `reports/gold_validation.json` vérifient :

1. L'unicité des identifiants du résumé Spark.
2. L'égalité du nombre de lignes Spark et du nombre de documents Elasticsearch.
3. La conservation des citations inconnues dans le résumé.
4. Le marqueur de borne basse associé aux citations inconnues.
5. L'intervalle du score, entre 0 et 100.
6. La réussite de la suite GX Gold : six attentes.
7. La formule du score sur un échantillon de 100 documents Elasticsearch.

La comparaison des identifiants de calcul confirme aussi que les 100 075 documents Elasticsearch portent le `run_id` du résultat Spark enregistré. Ce contrôle identifie la version du calcul ; il ne remplace pas une comparaison exhaustive de chaque champ de chaque document.

## 3. Requêtes analytiques

Dans http://localhost:5601/app/dashboards#/view/scipulse-insights, appliquer les filtres KQL ci-dessous.

| Cas | Filtre | Résumé Spark | Elasticsearch |
|---|---|---:|---:|
| Articles très cités sans HN observé | `citation_count >= 50 and hn_matched: false` | 881 | 881 |
| Articles populaires HN et peu cités | `hn_score >= 50 and citation_count < 10` | 3 | 3 |
| Articles reliés aux citations et à HN | `citation_matched: true and hn_matched: true` | 13 | 13 |

Les deux premiers cas sont illustrés par les captures `scipulse-kibana4.png` et `scipulse-kibana5.png`. Les seuils sont des paramètres d'exploration ; une absence de mention HN signifie « non observée dans la collecte ».

Pour retrouver les nombres attendus indépendamment d'Elasticsearch, exécuter dans un notebook Python disposant de pandas et pyarrow :

```python
from pathlib import Path
import pandas as pd

root = Path.cwd()
if not (root / 'reports').exists():
    root = root.parent
gold = pd.read_parquet(root / 'reports/gold_summary.parquet')
expected = {
    'academic_without_hn': int(((gold.citation_count >= 50) & ~gold.hn_matched & gold.citation_matched).sum()),
    'viral_low_citations': int(((gold.hn_score >= 50) & (gold.citation_count < 10) & gold.citation_matched).sum()),
    'cross_source': int((gold.hn_matched & gold.citation_matched).sum()),
}
print(expected)
```

La comparaison des résultats attendus et des compteurs Elasticsearch est enregistrée dans `reports/final_validation.json`. Les valeurs décrivent la version du calcul indiquée dans ce rapport ; elles peuvent évoluer après un nouveau traitement Spark.

## 4. TF-IDF sur des textes hors ajustement

Le job Spark transforme trois textes qui n'ont pas servi à ajuster le modèle, dont un vide et un contenant un terme inconnu. Il vérifie des vecteurs de 2 048 dimensions sans valeur non finie. Le résultat est enregistré sous `tfidf_holdout_passed` dans `reports/spark_results.json`.

Ce contrôle a réussi lors du calcul Spark enregistré. Il n'a pas été réexécuté pendant cette validation, qui n'a pas relancé Spark. Il contrôle le fonctionnement de la transformation sur quelques cas nouveaux, sans mesurer une précision prédictive, une qualité de recommandation ou une généralisation à tout ArXiv.

## 5. Monitoring

Les résultats de qualité, les horodatages et les anciennetés sont décrits dans [MONITORING.md](MONITORING.md). Les deux captures de monitoring sont disponibles dans [l'inventaire](../artifacts/screenshots/README.md). Les tests simulent notamment un contrôle récent en échec alors que le dernier Parquet valide est plus ancien.

## Résultats et limites

Le dernier contrôle enregistre 20 tests réussis, sept contrôles Gold réussis, six attentes GX Gold réussies et trois réponses analytiques identiques entre Spark et Elasticsearch. Les fichiers de preuve sont `tests_validation.xml`, `gold_validation.json` et `final_validation.json` dans `reports/`.

Cette validation ne constitue pas un nouveau démarrage depuis des volumes vierges ni une réexécution intégrale de Bronze à Kibana. La reproductibilité complète se vérifie séparément. Elle ne valide pas davantage une vélocité agrégée par catégorie ou un classement par croissance combinée citations/HN : ces limites restent décrites dans la méthodologie.

## Ajout Git manuel

```powershell
git add -- docs/VALIDATION.md
git add -- reports/tests_validation.xml reports/final_validation.json reports/gold_validation.json
```
