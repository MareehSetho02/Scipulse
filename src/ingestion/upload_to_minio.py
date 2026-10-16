import json
import os
from io import BytesIO

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError

# ── Configuration MinIO ─────────────────────────────────────────────

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET = os.getenv("MINIO_SECRET_KEY", "minioadmin")
BUCKET = "arxiv-raw"

# ── Connexion ────────────────────────────────────────────────────────

# Création du client S3
s3 = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS,
    aws_secret_access_key=MINIO_SECRET,
    config=Config(signature_version="s3v4"),
    region_name="us-east-1",
)


def ensure_bucket_exists(bucket_name: str) -> None:
    """Crée le bucket s'il n'existe pas déjà."""
    try:
        s3.head_bucket(Bucket=bucket_name)
        print(f"[OK] Bucket '{bucket_name}' déjà existant.")
    except ClientError as exc:
        if str(exc.response["Error"].get("Code")) not in ("404", "NoSuchBucket", "NotFound"):
            raise
        print(f"[INFO] Bucket '{bucket_name}' introuvable, création...")
        s3.create_bucket(Bucket=bucket_name)
        print(f"[OK] Bucket '{bucket_name}' créé.")


def upload_file(local_path: str, object_name: str | None = None):
    """Upload un fichier vers MinIO."""
    if object_name is None:
        object_name = os.path.basename(local_path)

    try:
        s3.upload_file(local_path, BUCKET, object_name)
        print(f"[OK] Fichier uploadé : {local_path} -> s3://{BUCKET}/{object_name}")
    except FileNotFoundError:
        print(f"[ERREUR] Fichier introuvable : {local_path}")
        raise
    except ClientError as e:
        print(f"[ERREUR] Échec de l'upload de {local_path} : {e}")
        raise


def split_and_upload(
    source_file: str,
    chunk_size: int = 10_000,
    prefix: str = "chunks/",
):
    """
    Découpe un fichier JSON lines en chunks et les upload vers MinIO.
    Chaque chunk contient `chunk_size` articles.
    """
    if chunk_size < 1:
        raise ValueError("chunk_size doit être positif")
    lines_buffer: list[str] = []
    chunk_index = 1
    total_lines = 0

    with open(source_file, "r", encoding="utf-8") as f:
        for line_number, line in enumerate(f, start=1):
            stripped = line.strip()
            if not stripped:
                continue

            # Vérification minimale : chaque ligne doit être un JSON valide
            try:
                json.loads(stripped)
            except json.JSONDecodeError as e:
                print(
                    f"[ERREUR] Ligne {line_number} invalide dans {source_file}: {e}"
                )
                raise

            lines_buffer.append(stripped + "\n")
            total_lines += 1

            if len(lines_buffer) >= chunk_size:
                object_name = f"{prefix}arxiv_chunk_{chunk_index:03d}.jsonl"
                _upload_chunk(lines_buffer, object_name)
                print(
                    f"[OK] Chunk {chunk_index} uploadé "
                    f"({len(lines_buffer)} articles) -> s3://{BUCKET}/{object_name}"
                )
                lines_buffer = []
                chunk_index += 1

    # Upload du dernier chunk s'il reste des lignes
    if lines_buffer:
        object_name = f"{prefix}arxiv_chunk_{chunk_index:03d}.jsonl"
        _upload_chunk(lines_buffer, object_name)
        print(
            f"[OK] Chunk {chunk_index} uploadé "
            f"({len(lines_buffer)} articles) -> s3://{BUCKET}/{object_name}"
        )

    print(f"[INFO] Découpage terminé : {total_lines} articles traités.")


def _upload_chunk(lines: list[str], object_name: str):
    """Upload un chunk en mémoire vers MinIO."""
    content = "".join(lines).encode("utf-8")
    buffer = BytesIO(content)

    try:
        s3.upload_fileobj(buffer, BUCKET, object_name)
    except ClientError as e:
        print(f"[ERREUR] Échec de l'upload du chunk {object_name} : {e}")
        raise


if __name__ == "__main__":
    import sys

    # Amélioration pour une application réelle :
    # Utiliser le module `argparse`
    source = (
        sys.argv[1]
        if len(sys.argv) > 1
        else "data/arxiv-raw/arxiv-metadata-oai-snapshot.json"
    )

    # 1) Traiter l'erreur si la source n'existe pas
    if not os.path.exists(source):
        print(f"[ERREUR] Le fichier source n'existe pas : {source}")
        raise FileNotFoundError(source)

    # S'assurer que le bucket existe
    ensure_bucket_exists(BUCKET)

    # 2) Afficher les information de chargement dans le bucket S3
    # -> Amélioration de bonne pratique : enregistrer l'information dans un fichier de log
    print("[INFO] Début du chargement vers MinIO")
    print(f"[INFO] Endpoint    : {MINIO_ENDPOINT}")
    print(f"[INFO] Bucket      : {BUCKET}")
    print(f"[INFO] Source file  : {source}")

    full_object_name = os.path.basename(source)

    # 3) Upload du fichier complet
    print("[INFO] Upload du fichier complet...")
    upload_file(source, full_object_name)

    # 4) Découpage en chunks de 10 000 articles
    print("[INFO] Découpage en chunks de 10 000 articles...")
    split_and_upload(source, chunk_size=10_000, prefix="chunks/")

    print("[OK] Traitement terminé avec succès.")