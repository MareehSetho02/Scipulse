# Reproductibilité et export du projet

## Démarrage

Le dossier de travail est `scipulse/`. Docker Desktop doit utiliser des conteneurs Linux et Docker Compose v2.

```powershell
.\project.ps1 up
```

La commande construit le runtime et démarre la stack. Le runtime contient Python, Java, Spark et les connecteurs S3A et Elasticsearch. PostgreSQL initialise les bases Airflow et dbt ; MinIO reçoit les sept buckets du pipeline.

Après démarrage, les nouveaux DAGs sont en pause. Pour déclencher le parcours et activer HN :

```powershell
.\project.ps1 run
```

Suivre les tâches dans Airflow : la commande de déclenchement n'attend pas la fin du traitement.

Les paramètres de démonstration sont définis dans le Compose. Les clés et paramètres personnels peuvent être définis localement dans `.env`, qui reste hors du dépôt et de l'export. Les ports locaux utilisés sont notamment 8080 pour Airflow, 9001 pour MinIO, 5601 pour Kibana et 8090 pour le master Spark.

Au démarrage du serveur web Airflow, la commande supprime le fichier temporaire `/opt/airflow/airflow-webserver.pid` avant de lancer le service. Cela évite qu'un PID conservé après un arrêt empêche le redémarrage du conteneur. Les volumes de données ne sont pas concernés.

Environ 16 Go de RAM sur le poste et idéalement 12 Go pour Docker sont recommandés lorsque la stack et les executors Spark travaillent simultanément. Un démarrage de services sans calcul Spark ne démontre pas que cette mémoire est suffisante pour chaque traitement.

## Vérification sur volumes neufs

`verify_cold_start.py` exporte le commit Git courant dans un répertoire de `.cache/`. Il résout le Compose depuis cet export et utilise un nom de projet Docker unique, sans ports hôte ni volumes externes.

Le script met temporairement les DAGs de travail en pause, attend la fin des traitements actifs, puis arrête les conteneurs de travail sans supprimer leurs volumes. La stack isolée utilise des volumes neufs et ses DAGs restent en pause. À la fin, ses seules ressources temporaires sont supprimées et la stack de travail est redémarrée avec ses états de pause initiaux.

Les vérifications portent sur :

- la disponibilité d'Elasticsearch, de Kibana, de MinIO et d'Airflow ;
- la santé du scheduler et du métastore Airflow ;
- l'enregistrement des deux workers Spark ;
- la présence des sept buckets, initialement vides ;
- l'absence initiale de l'index Gold ;
- les schémas PostgreSQL `staging` et `marts` ;
- le chargement des deux DAGs en pause ;
- les 20 tests exécutés depuis les fichiers de l'export ;
- la conservation du nombre de documents Gold dans la stack de travail après restauration.

Le résultat détaillé est enregistré dans `reports/cold_start.json`. Le journal technique est `reports/cold_start.log`.

Le test enregistré a réussi sur l'export Git. La première remise en route de la stack de travail a rencontré un PID Airflow résiduel ; la commande du serveur web a ensuite été corrigée et son redémarrage ciblé a été vérifié. Le rapport conserve cet incident initial, la reprise des DAGs et la conservation des 100 075 documents Gold. La correction locale du Compose est postérieure au commit utilisé pour l'export testé.

Cette vérification réutilise les images Docker disponibles. Elle ne teste pas leur reconstruction sans cache, une installation sans accès réseau ou une nouvelle collecte complète sur les volumes neufs. Les résultats des données proviennent de l'exécution décrite dans [VALIDATION.md](VALIDATION.md).

Le script se lance depuis le poste avec Python et requests disponibles, et suppose que la stack de travail répond déjà :

```powershell
.\.venv\Scripts\python.exe -X utf8 verify_cold_start.py
```

Il arrête temporairement les services : ne pas le lancer pendant une utilisation interactive du dashboard ou un traitement long.

## Export ZIP

Le script `package_project.py` produit un export de fichiers, sans métadonnées Git. Il conserve le code, les configurations, les documents, les notebooks, les résultats de validation, les huit captures, les graphiques et le corpus préparé utilisé pour les analyses.

```powershell
.\.venv\Scripts\python.exe package_project.py
```

Le nom par défaut est `artifacts/scipulse_projet4_AIA01.zip`. Pour utiliser le nom du rendu :

```powershell
.\.venv\Scripts\python.exe package_project.py --name prenom_nom_projet4_AIA01
```

Le nom définitif est à renseigner avant le dépôt. Le script refuse les chemins et les caractères autres que lettres, chiffres, tirets et underscores.

Les exclusions comprennent `.env`, `.env.example`, `.venv/`, `.git/`, les fichiers cachés, les logs, les ZIP imbriqués et le dump ArXiv complet. Les scripts facultatifs de génération de documents et de capture automatique ne sont pas inclus. Les JSONL et Parquet de `data/project/` correspondent uniquement au corpus préparé ; ils sont distincts du dump complet.

Les contrôles de l'archive vérifient les fichiers requis, les huit images, les exclusions et l'intégrité CRC. Le manifeste `reports/delivery_manifest.json`, également inclus dans le ZIP, contient les empreintes SHA-256 des fichiers exportés, le commit de référence et les limites du test de reproductibilité. Les fichiers de travail exportés peuvent comprendre des modifications plus récentes que ce commit.

## Points de portée

- Le corpus est échantillonné et complété de références HN ; les chiffres ne décrivent pas tout ArXiv.
- Les citations non collectées restent inconnues et le score reste une heuristique.
- Les citations annuelles par catégorie sont enregistrées dans MinIO ; cette table n'est pas exposée dans Kibana.
- L'alias `arxiv-papers` expose l'index vérifié issu de Silver ; les anciens articles sont conservés dans `arxiv-papers-tp-archive`. La migration est ponctuelle, distincte du démarrage sur volumes neufs. Voir [EXPLORATION_ARXIV.md](EXPLORATION_ARXIV.md).
- Le dump complet est conservé dans Bronze avec contrôle SHA-256 ; l'export ZIP et les traitements Silver/Gold utilisent le corpus préparé.
- La comparaison de jointures porte sur les plans, sans benchmark de temps indépendant.
- Les tests TF-IDF contrôlent le contrat de transformation, sans mesure de précision prédictive.

## Ajout Git manuel

```powershell
git add -- verify_cold_start.py package_project.py
git add -- docker-compose.project.yml
git add -- docs/REPRODUCTIBILITE_ET_RENDU.md
git add -- reports/cold_start.json reports/delivery_manifest.json
```

Le ZIP et le corpus de données restent hors de Git. La création des commits et le push sont indépendants de l'export.
