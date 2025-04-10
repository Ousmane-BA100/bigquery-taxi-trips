import requests
import time
import logging
import io
import yaml
import pyarrow.parquet as pq
from datetime import datetime, UTC
from google.cloud import storage
from typing import Dict, Any, List, Callable, Optional, Tuple


def load_config(config_path: str) -> Dict[str, Any]:
    """Charge la configuration depuis un fichier YAML."""
    with open(config_path, 'r') as file:
        return yaml.safe_load(file)


def init_logging() -> Tuple[io.StringIO, logging.Logger]:
    """Initialise et configure le logging."""
    log_stream = io.StringIO()
    logging_format = "%(asctime)s - %(levelname)s - %(message)s"
    logging.basicConfig(stream=log_stream, level=logging.INFO, format=logging_format)
    logger = logging.getLogger(__name__)
    return log_stream, logger


def create_gcs_client() -> storage.Client:
    """Crée et retourne un client Google Cloud Storage."""
    return storage.Client()


def check_file_exists(client: storage.Client, bucket_name: str, path: str) -> bool:
    """Vérifie si un fichier existe dans GCS."""
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(path)
    return blob.exists()


def upload_to_gcs(client: storage.Client, bucket_name: str, path: str, content: bytes) -> None:
    """Télécharge le contenu dans GCS."""
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(path)
    blob.upload_from_string(content)


def upload_log(client: storage.Client, config: Dict[str, Any], log_stream: io.StringIO, logger: logging.Logger) -> None:
    """Télécharge les logs dans GCS."""
    bucket_name = config["bucket_name"]
    log_folder = config["log_folder"]
    timestamp = datetime.now(UTC).strftime('%Y%m%d_%H%M%S')
    log_filename = f"{log_folder}extract_log_{timestamp}.log"
    
    upload_to_gcs(client, bucket_name, log_filename, log_stream.getvalue())
    logger.info(f"Log file uploaded to {log_filename}")


def extract_field_names(config: Dict[str, Any]) -> List[str]:
    """Extrait les noms de champs de la configuration."""
    if "fields" not in config:
        return []
    
    return [field["name"] for field in config["fields"] if "name" in field]


def fetch_and_filter_file(url: str, selected_columns: List[str], logger: logging.Logger) -> Optional[bytes]:
    """Télécharge un fichier depuis une URL, filtre les colonnes et retourne son contenu."""
    try:
        logger.info(f"Téléchargement depuis {url}...")
        response = requests.get(url, stream=True)
        
        if response.status_code == 200:
            # Sauvegarder temporairement le contenu
            with io.BytesIO(response.content) as temp_buffer:
                # Si aucune colonne n'est spécifiée ou si le fichier n'est pas un Parquet
                if not selected_columns or not url.endswith('.parquet'):
                    logger.info("Aucun filtrage de colonnes appliqué")
                    return response.content
                
                try:
                    # Lire le schéma sans charger les données
                    parquet_schema = pq.read_schema(temp_buffer)
                    available_columns = parquet_schema.names
                    
                    # Vérifier quelles colonnes demandées sont disponibles
                    columns_to_read = [col for col in selected_columns if col in available_columns]
                    
                    if not columns_to_read:
                        logger.warning(f"Aucune des colonnes sélectionnées {selected_columns} n'a été trouvée dans le fichier. Utilisation de toutes les colonnes.")
                        return response.content
                    
                    logger.info(f"Filtrage des colonnes : {columns_to_read}")
                    logger.info(f"Économie de stockage : {len(available_columns) - len(columns_to_read)}/{len(available_columns)} colonnes supprimées")
                    
                    # Revenir au début du buffer pour la lecture
                    temp_buffer.seek(0)
                    
                    # Lire seulement les colonnes sélectionnées
                    table = pq.read_table(temp_buffer, columns=columns_to_read)
                    
                    # Convertir en buffer pour l'upload
                    output_buffer = io.BytesIO()
                    pq.write_table(table, output_buffer)
                    
                    return output_buffer.getvalue()
                    
                except Exception as e:
                    logger.error(f"Erreur lors du filtrage des colonnes : {str(e)}. Utilisation du fichier complet.")
                    return response.content
                
        elif response.status_code == 404:
            logger.warning(f"Fichier non trouvé à {url}, passage au suivant...")
            return None
        else:
            logger.error(f"Échec du téléchargement depuis {url}. Code de statut HTTP : {response.status_code}")
            return None
    except Exception as e:
        logger.error(f"Erreur lors du téléchargement depuis {url} : {str(e)}")
        return None


def generate_file_list(config: Dict[str, Any], url_pattern_fn: Callable) -> List[Dict[str, str]]:
    """Génère une liste de fichiers à traiter avec leurs URLs et chemins GCS."""
    current_year = datetime.now().year
    start_year = config.get("start_year", 2020)
    file_prefix = config.get("file_prefix", "")
    file_extension = config.get("file_extension", ".parquet")
    
    files = []
    for year in range(start_year, current_year + 1):
        for month in range(1, 13):
            file_name = f"{file_prefix}_{year}-{month:02d}{file_extension}"
            gcs_path = f"{config['gcs_folder']}{file_name}"
            download_url = url_pattern_fn(file_name)
            
            files.append({
                "file_name": file_name,
                "gcs_path": gcs_path,
                "url": download_url
            })
    
    return files


def process_files(client: storage.Client, config: Dict[str, Any], files: List[Dict[str, str]], 
                  logger: logging.Logger) -> None:
    """Traite une liste de fichiers: téléchargement, filtrage et stockage dans GCS."""
    bucket_name = config["bucket_name"]
    selected_columns = extract_field_names(config)
    delay = config.get("delay_between_downloads", 1)
    
    if selected_columns:
        logger.info(f"Filtrage des données pour inclure uniquement ces colonnes : {selected_columns}")
    else:
        logger.info("Aucun filtrage de colonnes appliqué - toutes les colonnes seront conservées")
    
    for file_info in files:
        if check_file_exists(client, bucket_name, file_info["gcs_path"]):
            logger.info(f"{file_info['file_name']} existe déjà dans GCS, passage au suivant...")
            continue
        
        content = fetch_and_filter_file(file_info["url"], selected_columns, logger)
        if content:
            upload_to_gcs(client, bucket_name, file_info["gcs_path"], content)
            logger.info(f"{file_info['file_name']} téléchargé avec succès vers GCS")
            
        time.sleep(delay)


def main(config_path: str) -> None:
    """Fonction principale qui orchestre le processus d'extraction et téléchargement."""
    # Charger la configuration
    config = load_config(config_path)
    
    # Initialiser le logging
    log_stream, logger = init_logging()
    logger.info(f"Démarrage de l'extraction de données le : {datetime.now(UTC)}")
    
    # Initialiser le client GCS
    client = create_gcs_client()
    
    try:
        # Définir la fonction de pattern URL pour NYC Yellow Taxi
        def nyc_taxi_url_pattern(file_name: str) -> str:
            return f"https://d37ci6vzurychx.cloudfront.net/trip-data/{file_name}"
        
        # Générer la liste de fichiers
        files = generate_file_list(config, nyc_taxi_url_pattern)
        
        # Traiter les fichiers
        process_files(client, config, files, logger)
        
        logger.info("Téléchargement et upload vers GCS terminés!")
    except Exception as e:
        logger.error(f"Erreur inattendue : {str(e)}")
    finally:
        upload_log(client, config, log_stream, logger)


if __name__ == '__main__':
    config_path = "config/config.yaml" # Chemin vers le fichier de configuration
    main(config_path)
