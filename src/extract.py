import requests
import time
import logging
import io
import yaml
import pyarrow.parquet as pq
import argparse
from datetime import datetime, timezone
from google.cloud import storage
from typing import Dict, Any, List, Callable, Optional, Tuple


def load_config(config_path: str, section: Optional[str] = None) -> Dict[str, Any]:
    """Charge la configuration depuis un fichier YAML, éventuellement une section spécifique.
    
    Si une section est spécifiée, retourne un dictionnaire fusionné contenant :
    1. Tous les paramètres de niveau racine qui ne sont pas des dictionnaires
    2. La section demandée
    3. La section file_formats si elle existe
    """
    with open(config_path, 'r') as file:
        config = yaml.safe_load(file) or {}
    
    # Résoudre les références dans la configuration
    resolved_config = resolve_config_references(config)
    
    # Si une section spécifique est demandée, retourner un dictionnaire fusionné
    if section and section in resolved_config:
        # 1. D'abord les paramètres de niveau racine qui ne sont pas des dictionnaires
        section_config = {k: v for k, v in resolved_config.items() 
                         if not isinstance(v, dict) or k == 'file_formats'}
        
        # 2. Ensuite la section demandée
        section_config.update(resolved_config[section])
        
        # 3. S'assurer que file_formats est bien inclus même s'il n'est pas dans la section
        if 'file_formats' not in section_config and 'file_formats' in resolved_config:
            section_config['file_formats'] = resolved_config['file_formats']
        
        # 4. Inclure également les paramètres de format de fichier spécifiés dans la section
        if 'file_format' in section_config and 'file_formats' in resolved_config:
            file_format = section_config['file_format']
            if file_format in resolved_config['file_formats']:
                section_config.update(resolved_config['file_formats'][file_format])
            
        return section_config
    
    return resolved_config


def resolve_config_references(config: Dict[str, Any]) -> Dict[str, Any]:
    """Résout les références dans la configuration, comme ${project_id}."""
    config_str = yaml.dump(config)
    
    # Chercher et remplacer les références ${key} par leur valeur
    for key, value in config.items():
        if isinstance(value, str):
            placeholder = f"${{{key}}}"
            config_str = config_str.replace(placeholder, value)
    
    return yaml.safe_load(config_str)


def init_logging() -> Tuple[io.StringIO, logging.Logger]:
    """Initialise et configure le système de journalisation."""
    log_stream = io.StringIO()
    logging_format = "%(asctime)s - %(levelname)s - %(message)s"

    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    # Handler pour l'écran
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter(logging_format))
    
    # Handler pour le buffer (log_stream)
    stream_handler = logging.StreamHandler(log_stream)
    stream_handler.setFormatter(logging.Formatter(logging_format))
    
    # Nettoyer les handlers existants pour éviter les doublons
    logger.handlers.clear()
    logger.addHandler(console_handler)
    logger.addHandler(stream_handler)

    return log_stream, logger


def create_gcs_client(project_id: str) -> storage.Client:
    """Crée et retourne un client Google Cloud Storage."""
    return storage.Client(project=project_id)


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
    timestamp = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')
    log_filename = f"{log_folder}extract_log_{timestamp}.log"
    
    upload_to_gcs(client, bucket_name, log_filename, log_stream.getvalue())
    logger.info(f"Log file uploaded to {log_filename}")


def fetch_file(url: str, logger: logging.Logger) -> Optional[bytes]:
    """Télécharge un fichier depuis une URL et retourne son contenu."""
    try:
        logger.info(f"Téléchargement depuis {url}...")
        response = requests.get(url, stream=True)
        
        if response.status_code == 200:
            logger.info("Téléchargement réussi")
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
    start_year = config.get("start_year", current_year)
    gcs_folder = config["gcs_folder"].rstrip('/') + '/'
    file_format = config["file_format"]
    file_extension = config["file_formats"][file_format]["extension"]
    
    files = []
    
    # Pour chaque année de start_year à l'année actuelle
    for year in range(start_year, current_year + 1):
        # Pour chaque mois de l'année
        for month in range(1, 13):
            # Format du nom de fichier (ex: yellow_tripdata_2023-01.parquet)
            file_name = f"{config['file_prefix']}_{year}-{month:02d}{file_extension}"
            
            # URL complète du fichier
            url = url_pattern_fn(file_name)
            
            # Chemin de destination dans GCS
            gcs_path = f"{gcs_folder}{file_name}"
            
            files.append({
                "file_name": file_name,
                "url": url,
                "gcs_path": gcs_path
            })
    
    return files


def process_files(client: storage.Client, config: Dict[str, Any], files: List[Dict[str, str]], 
                 logger: logging.Logger) -> int:
    """Traite une liste de fichiers: téléchargement et stockage dans GCS.
    Retourne le nombre de fichiers traités avec succès."""
    bucket_name = config["bucket_name"]
    delay = config.get("delay_between_downloads", 1)
    
    logger.info("Aucun filtrage de colonnes - toutes les colonnes seront conservées")
    
    files_processed = 0
    for file_info in files:
        if check_file_exists(client, bucket_name, file_info["gcs_path"]):
            logger.info(f"{file_info['file_name']} existe déjà dans GCS, passage au suivant...")
            continue
        
        content = fetch_file(file_info["url"], logger)
        if content:
            upload_to_gcs(client, bucket_name, file_info["gcs_path"], content)
            logger.info(f"{file_info['file_name']} téléchargé avec succès vers GCS")
            files_processed += 1
            
        time.sleep(delay)
    
    return files_processed


def extract_pipeline(config_path: str = None):
    """La fonction qui effectue l'extraction avec un chemin de configuration donné."""
    # Si pas de chemin spécifié, utiliser un chemin par défaut
    if not config_path:
        config_path = '/home/airflow/gcs/dags/config.yaml'
    
    # Charger la configuration avec la section 'extract'
    config = load_config(config_path, 'extract')
    
    # Vérifier les paramètres requis
    required_params = ['project_id', 'bucket_name', 'gcs_folder', 'log_folder']
    missing_params = [param for param in required_params if param not in config]
    
    if missing_params:
        print(f"Paramètres manquants dans la configuration: {', '.join(missing_params)}")
        return
    
    # Initialiser le logging
    log_stream, logger = init_logging()
    logger.info(f"Démarrage de l'extraction de données le : {datetime.now(timezone.utc)}")
    
    # Initialiser le client GCS
    client = create_gcs_client(config["project_id"])
    
    try:
        # Définir la fonction de pattern URL pour NYC Yellow Taxi
        def nyc_taxi_url_pattern(file_name: str) -> str:
            return f"https://d37ci6vzurychx.cloudfront.net/trip-data/{file_name}"
        
        # Générer la liste de fichiers
        files = generate_file_list(config, nyc_taxi_url_pattern)
        logger.info(f"Nombre de fichiers à traiter: {len(files)}")
        
        # Traiter les fichiers
        files_processed = process_files(client, config, files, logger)
        
        logger.info(f"Téléchargement et upload vers GCS terminés! {files_processed} fichiers traités.")
        return files_processed
    except Exception as e:
        logger.error(f"Erreur inattendue : {str(e)}")
        raise
    finally:
        # Télécharger les logs dans GCS
        try:
            upload_log(client, config, log_stream, logger)
        except Exception as e:
            print(f"Erreur lors de l'upload des logs: {str(e)}")


def main(*args, **kwargs):
    """Fonction principale qui peut être appelée directement ou via ligne de commande.
    
    Args:
        *args: Arguments positionnels (ignorés)
        **kwargs: Arguments nommés. Peut contenir 'config_path' avec le chemin vers le fichier de configuration.
    """
    # Chemin de configuration par défaut
    config_path = None
    
    # Si appelé avec des arguments nommés (depuis Airflow)
    if kwargs and 'config_path' in kwargs:
        config_path = kwargs['config_path']
    # Sinon, essayer de parser les arguments en ligne de commande
    elif len(sys.argv) > 1:
        try:
            # Utiliser parse_known_args pour ignorer les arguments inconnus
            parser = argparse.ArgumentParser(description='Extract NYC Yellow Taxi data', add_help=False)
            parser.add_argument('--config', type=str, dest='config_path', 
                             help='Chemin vers le fichier de configuration')
            args, _ = parser.parse_known_args()
            if args.config_path:
                config_path = args.config_path
        except:
            # En cas d'erreur de parsing, continuer avec les valeurs par défaut
            pass
    
    # Si aucun chemin de configuration n'est fourni, utiliser la valeur par défaut
    if not config_path:
        config_path = '/opt/airflow/dags/config.yaml'
    
    print(f"Using config file: {config_path}")
    
    # Appeler la fonction d'extraction avec le chemin de configuration
    return extract_pipeline(config_path)


if __name__ == '__main__':
    import sys
    main()