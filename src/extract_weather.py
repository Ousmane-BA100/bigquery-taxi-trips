import requests
import time
import logging
import io
import yaml
import argparse
from datetime import datetime, timedelta, UTC
from google.cloud import storage
from typing import Dict, Any, List, Callable, Optional, Tuple


def load_config(config_path: str, section: Optional[str] = None) -> Dict[str, Any]:
    """Charge la configuration depuis un fichier YAML, éventuellement une section spécifique."""
    with open(config_path, 'r') as file:
        config = yaml.safe_load(file)
    
    # Résoudre les références dans la configuration
    resolved_config = resolve_config_references(config)
    
    # Si une section spécifique est demandée, retourner uniquement cette section
    if section and section in resolved_config:
        # Fusionner les paramètres globaux avec les paramètres spécifiques à la section
        # en excluant les autres sections de configuration spécifiques
        section_config = {k: v for k, v in resolved_config.items() 
                        if k != 'extract' and k != 'extract_weather' and k != 'load'}
        section_config.update(resolved_config[section])
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


def upload_to_gcs(client: storage.Client, bucket_name: str, path: str, content: str) -> None:
    """Télécharge le contenu dans GCS."""
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(path)
    blob.upload_from_string(content)


def upload_log(client: storage.Client, config: Dict[str, Any], log_stream: io.StringIO, logger: logging.Logger) -> None:
    """Télécharge les logs dans GCS."""
    bucket_name = config["bucket_name"]
    log_folder = config["log_folder"]
    timestamp = datetime.now(UTC).strftime('%Y%m%d_%H%M%S')
    log_filename = f"{log_folder}extract_weather_log_{timestamp}.log"
    
    upload_to_gcs(client, bucket_name, log_filename, log_stream.getvalue())
    logger.info(f"Log file uploaded to {log_filename}")


def extract_asos_data(station_id: str, start_date: str, end_date: str, logger: logging.Logger) -> Optional[str]:
    """Extrait les données ASOS pour une station météo spécifique"""
    logger.info(f"Extraction des données pour la station {station_id} du {start_date} au {end_date}")
    
    base_url = "https://mesonet.agron.iastate.edu/cgi-bin/request/asos.py?"
    params = {
        'station': station_id,
        'data': 'all',
        'year1': start_date.split('-')[0],
        'month1': start_date.split('-')[1],
        'day1': start_date.split('-')[2],
        'year2': end_date.split('-')[0],
        'month2': end_date.split('-')[1],
        'day2': end_date.split('-')[2],
        'tz': 'America/New_York',
        'format': 'comma',
        'latlon': 'yes',
        'report_type': '1',
        'report_type': '2'  # Inclut les METAR horaires
    }
    
    url_params = "&".join([f"{k}={v}" for k, v in params.items()])
    url = base_url + url_params
    
    try:
        response = requests.get(url)
        response.raise_for_status()  # Lève une exception pour les erreurs HTTP
        
        # Vérifier si les données sont valides (au moins les en-têtes)
        if len(response.text.split('\n')) <= 6:  # Seulement des en-têtes
            logger.warning(f"Aucune donnée retournée pour {station_id}")
            return None
            
        return response.text
    except requests.exceptions.RequestException as e:
        logger.error(f"Erreur lors de l'extraction des données pour {station_id}: {str(e)}")
        return None


def get_date_ranges(config: Dict[str, Any]) -> List[Dict[str, str]]:
    """Génère les plages de dates pour l'extraction des données météo."""
    start_date_str = config.get("start_date", "2022-01-01")
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_date = datetime.now()
    
    # Traiter mois par mois pour éviter timeout et faciliter l'organisation
    date_ranges = []
    current_date = start_date
    
    while current_date < end_date:
        # Calculer le premier jour du mois suivant
        if current_date.month == 12:
            next_month = datetime(current_date.year + 1, 1, 1)
        else:
            next_month = datetime(current_date.year, current_date.month + 1, 1)
        
        # S'assurer que la date de fin ne dépasse pas aujourd'hui
        period_end = min(next_month - timedelta(days=1), end_date)
        
        date_ranges.append({
            "start_date": current_date.strftime("%Y-%m-%d"),
            "end_date": period_end.strftime("%Y-%m-%d"),
            "year_month": current_date.strftime("%Y/%m")  # Pour organiser le stockage
        })
        
        # Passer au mois suivant
        current_date = next_month
    
    return date_ranges


def process_weather_data(client: storage.Client, config: Dict[str, Any], logger: logging.Logger) -> int:
    """Traite l'extraction et le stockage des données météo pour toutes les stations et périodes.
    Retourne le nombre de fichiers traités avec succès."""
    bucket_name = config["bucket_name"]
    gcs_folder = config["gcs_folder"]
    stations = config.get("stations", [])
    delay = config.get("delay_between_downloads", 2)
    
    if not stations:
        logger.error("Aucune station météo définie dans la configuration")
        return 0
        
    logger.info(f"Traitement de {len(stations)} stations météo")
    
    # Générer les plages de dates
    date_ranges = get_date_ranges(config)
    logger.info(f"{len(date_ranges)} périodes mensuelles à traiter")
    
    files_processed = 0
    
    # Pour chaque station et chaque plage de dates
    for station in stations:
        station_id = station["id"]
        logger.info(f"Traitement de la station {station_id} ({station.get('name', '')})")
        
        for date_range in date_ranges:
            start_date = date_range["start_date"]
            end_date = date_range["end_date"]
            year_month = date_range["year_month"]
            
            # Définir le chemin GCS pour ce fichier
            file_name = f"{station_id}_{start_date.replace('-', '')}_{end_date.replace('-', '')}.csv"
            gcs_path = f"{gcs_folder}{year_month}/{file_name}"
            
            # Vérifier si le fichier existe déjà
            if check_file_exists(client, bucket_name, gcs_path):
                logger.info(f"{file_name} existe déjà dans GCS, passage au suivant...")
                continue
                
            # Extraire les données
            data = extract_asos_data(station_id, start_date, end_date, logger)
            
            if data:
                upload_to_gcs(client, bucket_name, gcs_path, data)
                logger.info(f"{file_name} téléchargé avec succès vers GCS: {gcs_path}")
                files_processed += 1
                
            # Attendre un peu pour ne pas surcharger l'API
            time.sleep(delay)
    
    return files_processed


def main():
    """Fonction principale qui orchestre le processus d'extraction et téléchargement des données météo."""
    parser = argparse.ArgumentParser(description='Extraire et télécharger des données météo ASOS vers GCS')
    parser.add_argument('--config', required=True, help='Chemin vers le fichier de configuration YAML')
    args = parser.parse_args()
    
    # Charger la configuration avec la section 'extract_weather'
    config = load_config(args.config, 'extract_weather')
    
    # Vérifier les paramètres requis
    required_params = ['project_id', 'bucket_name', 'gcs_folder', 'log_folder']
    missing_params = [param for param in required_params if param not in config]
    
    if missing_params:
        print(f"Paramètres manquants dans la configuration: {', '.join(missing_params)}")
        return
    
    # Initialiser le logging
    log_stream, logger = init_logging()
    logger.info(f"Démarrage de l'extraction des données météo le : {datetime.now(UTC)}")
    
    # Initialiser le client GCS
    client = create_gcs_client(config["project_id"])
    
    try:
        # Traiter les données météo
        files_processed = process_weather_data(client, config, logger)
        
        logger.info(f"Extraction des données météo terminée! {files_processed} fichiers traités.")
    except Exception as e:
        logger.error(f"Erreur inattendue : {str(e)}")
    finally:
        upload_log(client, config, log_stream, logger)


if __name__ == '__main__':
    main()