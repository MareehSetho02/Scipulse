import json
import os
import re
import sys
import time

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

from elasticsearch import Elasticsearch, helpers

# ── Configuration ────────────────────────────────────────────────────────

ES_HOST = os.getenv("ES_HOST", "http://localhost:9200")
INDEX_NAME = "arxiv-papers-raw-py"   # Index séparé pour comparer avec Logstash
BATCH_SIZE = 1000                    # Nombre de documents par requête bulk


### 5.2. Fonction de transformation d'un article

def transform_article(raw: dict) -> dict:
    """
    Transforme un article ArXiv brut en document prêt à indexer.
    Équivalent Python des filtres Logstash (mutate, date, split, ruby).
    """

    # 1. Éclatement des catégories (équivalent du mutate split)
    categories_raw = raw.get("categories", "")
    if isinstance(categories_raw, str):
        categories = [cat.strip() for cat in categories_raw.split() if cat.strip()]
    elif isinstance(categories_raw, list):
        categories = [str(cat).strip() for cat in categories_raw if str(cat).strip()]
    else:
        categories = []

    # 2. Nettoyage de l'abstract (équivalent du mutate gsub + strip)
    abstract = raw.get("abstract", "")
    if abstract is None:
        abstract = ""
    abstract = re.sub(r"\s+", " ", str(abstract)).strip()

    # 3. Nettoyage du titre
    title = raw.get("title", "")
    if title is None:
        title = ""
    title = re.sub(r"\s+", " ", str(title)).strip()

    # 4. Construction du document transformé
    doc = {
        "arxiv_id": raw.get("arxiv_id") or raw.get("id"),
        "primary_category": categories[0] if categories else None,
        "date_updated": raw.get("date_updated") or raw.get("update_date"),
        "id": raw.get("id"),
        "submitter": raw.get("submitter"),
        "authors": raw.get("authors"),
        "title": title,
        "comments": raw.get("comments"),
        "journal_ref": raw.get("journal-ref") or raw.get("journal_ref"),
        "doi": raw.get("doi"),
        "report_no": raw.get("report-no") or raw.get("report_no"),
        "categories": categories,
        "abstract": abstract,
        "versions": raw.get("versions", []),
        "update_date": raw.get("update_date"),
        "authors_parsed": raw.get("authors_parsed", []),
    }

    return doc


def generate_actions(filepath: str):
    """
    Générateur qui lit un fichier JSON ligne par ligne et produit des actions bulk ES.
    Utilise un générateur (yield) pour ne pas charger tout le fichier en mémoire.
    """
    with open(filepath, "r", encoding="utf-8") as f:
        for line_number, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue

            # 1. Lire la ligne JSON en traitant les erreurs
            try:
                raw = json.loads(line)
            except json.JSONDecodeError as e:
                print(
                    f"[WARN] Ligne {line_number} ignorée : JSON invalide ({e})",
                    file=sys.stderr,
                )
                continue

            # 2. Normaliser les données de l'article
            # avec la fonction transform_article
            doc = transform_article(raw)

            # 3. Vérification minimale : l'arxiv_id doit exister
            # Sinon il est simplement ignoré
            arxiv_id = doc.get("id")
            if not arxiv_id:
                print(
                    f"[WARN] Ligne {line_number} ignorée : id ArXiv manquant",
                    file=sys.stderr,
                )
                continue

            # Action bulk au format ES
            # Attention ! `generate_actions` est un générateur
            yield {
                "_index": INDEX_NAME,
                "_id": arxiv_id,
                "_source": doc,
            }

def ingest(filepath: str):
    """Ingestion bulk du fichier ArXiv dans Elasticsearch."""

    # 1. Connexion à Elasticsearch
    es = Elasticsearch(ES_HOST)

    # 2. Vérification de la connexion
    try:
        if not es.ping():
            print(f"[ERREUR] Impossible de se connecter à Elasticsearch sur {ES_HOST}")
            return
        print(f"[OK] Connecté à Elasticsearch : {ES_HOST}")
    except Exception as e:
        print(f"[ERREUR] Connexion Elasticsearch échouée : {e}")
        return

    # Les identifiants stables permettent de relancer sans supprimer l'index.

    # 4. Ingestion via l'API bulk
    print(f"\n📥 Ingestion de {filepath} → index '{INDEX_NAME}'")
    print(f"   Taille des lots : {BATCH_SIZE} documents\n")

    # 4.1 Variables de traçage
    start_time = time.time()
    success_count = 0
    error_count = 0
    batch_count = 0

    # 4.2 Ingestion
    # helpers.streaming_bulk gère automatiquement le découpage en lots
    # Pour chaque lot, exécuter la fonction generate_actions
    for ok, result in helpers.streaming_bulk(
        es,
        generate_actions(filepath),
        chunk_size=BATCH_SIZE,
        raise_on_error=False,
        raise_on_exception=False,
    ):
        action, info = next(iter(result.items()))

        if ok:
            success_count += 1
            if success_count % BATCH_SIZE == 0:
                batch_count += 1
                print(f"[INFO] Lot {batch_count:>4} terminé — {success_count:>7,} documents indexés")
        else:
            error_count += 1
            doc_id = info.get("_id", "inconnu")
            error_reason = info.get("error", "erreur inconnue")
            print(f"[WARN] Échec indexation doc {doc_id}: {error_reason}", file=sys.stderr)

    # 5. Bilan
    elapsed = time.time() - start_time
    rate = success_count / elapsed if elapsed > 0 else 0

    print(f"\n{'═' * 50}")
    print(f"  ✅ Succès  : {success_count:>7,}")
    print(f"  ❌ Erreurs : {error_count:>7,}")
    print(f"  ⏱  Durée   : {elapsed:.1f} secondes")
    print(f"  🚀 Débit   : {rate:,.0f} documents/seconde")
    print(f"{'═' * 50}")

    # 6. Forcer le rafraîchissement de l'index pour que les documents soient cherchables
    es.indices.refresh(index=INDEX_NAME)

    # 7. Vérification finale
    count = es.count(index=INDEX_NAME)["count"]
    print(f"\n🔍 Vérification : {count:,} documents dans l'index '{INDEX_NAME}'")
    if error_count:
        raise RuntimeError(f"{error_count} documents non indexés")


if __name__ == "__main__":
    filepath = sys.argv[1] if len(sys.argv) > 1 else "data/arxiv-raw/arxiv-cs-subset-100k.json"

    if not os.path.exists(filepath):
        print(f"❌ Fichier non trouvé : {filepath}")
        print(f"   Usage : python -m src.ingestion.arxiv_bulk_ingest <chemin_fichier>")
        sys.exit(1)

    ingest(filepath)
