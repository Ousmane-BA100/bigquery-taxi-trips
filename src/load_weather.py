import argparse
import io
import logging
import os
import tempfile
import yaml
from datetime import datetime, UTC, timedelta
from typing import Dict, Any, List, Optional, Tuple

import pandas as pd
from google.cloud import storage, bigquery

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
                        if k != 'extract' and k != 'extract_weather' and k != 'load' and k != 'load_weather'}
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


def create_gcp_clients(project_id: str) -> Tuple[storage.Client, bigquery.Client]:
    """Crée et retourne les clients GCP nécessaires."""
    storage_client = storage.Client(project=project_id)
    bq_client = bigquery.Client(project=project_id)
    return storage_client, bq_client


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
    log_filename = f"{log_folder}load_weather_log_{timestamp}.log"
    
    upload_to_gcs(client, bucket_name, log_filename, log_stream.getvalue())
    logger.info(f"Log file uploaded to {log_filename}")


def ensure_dataset_exists(client: bigquery.Client, dataset_id: str, location: str = "US") -> str:
    """Vérifie si le dataset existe, le crée sinon."""
    dataset_ref = f"{client.project}.{dataset_id}"
    try:
        client.get_dataset(dataset_ref)  # Vérifie si le dataset existe
    except Exception:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = location
        client.create_dataset(dataset)
    
    return dataset_ref


def create_table_if_not_exists(client: bigquery.Client, dataset_id: str, table_id: str, 
                               schema: List[bigquery.SchemaField], partition_field: str = None, 
                               clustering_fields: List[str] = None) -> str:
    """Crée la table si elle n'existe pas avec la configuration spécifiée."""
    table_ref = f"{client.project}.{dataset_id}.{table_id}"
    
    try:
        client.get_table(table_ref)  # Vérifie si la table existe
    except Exception:
        table = bigquery.Table(table_ref, schema=schema)
        
        # Configuration du partitionnement
        if partition_field:
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field=partition_field
            )
        
        # Configuration du clustering
        if clustering_fields:
            table.clustering_fields = clustering_fields
        
        client.create_table(table)
    
    return table_ref


def get_weather_table_schema() -> List[bigquery.SchemaField]:
    """Définit et retourne le schéma optimisé de la table météo pour les analyses de taxis."""
    return [
        bigquery.SchemaField("station", "STRING", description="Code de la station météo"),
        bigquery.SchemaField("valid", "TIMESTAMP", description="Date et heure de l'observation"),
        bigquery.SchemaField("lon", "FLOAT", description="Longitude"),
        bigquery.SchemaField("lat", "FLOAT", description="Latitude"),
        bigquery.SchemaField("tmpf", "FLOAT", description="Température en Fahrenheit"),
        bigquery.SchemaField("p01i", "FLOAT", description="Précipitations sur 1 heure (pouces)"),
        bigquery.SchemaField("vsby", "FLOAT", description="Visibilité (miles)"),
        bigquery.SchemaField("sknt", "FLOAT", description="Vitesse du vent (nœuds)"),
        bigquery.SchemaField("gust", "FLOAT", description="Rafales de vent (nœuds)"),
        bigquery.SchemaField("skyc1", "STRING", description="Conditions du ciel"),
        bigquery.SchemaField("wxcodes", "STRING", description="Codes météo"),
        bigquery.SchemaField("file_date", "DATE", description="Date du fichier source"),
        bigquery.SchemaField("processed_at", "TIMESTAMP", description="Date de traitement"),
        bigquery.SchemaField("station_name", "STRING", description="Nom de la station"),
        bigquery.SchemaField("station_zone", "STRING", description="Zone géographique")
    ]


def list_gcs_files(client: storage.Client, bucket_name: str, prefix: str, 
                  start_date: str = None, end_date: str = None) -> List[storage.Blob]:
    """Liste les fichiers dans GCS correspondant aux critères."""
    bucket = client.bucket(bucket_name)
    blobs = list(bucket.list_blobs(prefix=prefix))
    
    if start_date or end_date:
        filtered_blobs = []
        for blob in blobs:
            try:
                # Extraire la date du nom du fichier (STATION_YYYYMMDD_YYYYMMDD.csv)
                filename = blob.name.split('/')[-1]
                parts = filename.split('_')
                file_date = parts[1]  # YYYYMMDD format
                
                if start_date and file_date < start_date.replace('-', ''):
                    continue
                if end_date and file_date > end_date.replace('-', ''):
                    continue
                
                filtered_blobs.append(blob)
            except:
                # Si format incorrect, inclure par défaut
                filtered_blobs.append(blob)
        
        return filtered_blobs
    
    return blobs


def clean_numeric_data(df: pd.DataFrame) -> pd.DataFrame:
    """Nettoie les données numériques pour BigQuery en convertissant les valeurs problématiques."""
    # Liste des colonnes numériques à nettoyer
    numeric_columns = ['tmpf', 'p01i', 'vsby', 'sknt', 'gust', 'lon', 'lat']
    
    for col in numeric_columns:
        if col in df.columns:
            # Convertir en chaîne pour assurer la consistance du traitement
            df[col] = df[col].astype(str)
            
            # Remplacer les valeurs spéciales par None (qui deviendra NULL dans BigQuery)
            df[col] = df[col].replace(['M', 'NA', 'None', 'null', '', 'nan'], None)
            
            # Convertir en float, les valeurs non-convertibles deviennent NaN
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    return df


def process_weather_file(blob: storage.Blob, temp_dir: str, stations_info: List[Dict[str, Any]], 
                         logger: logging.Logger) -> Optional[pd.DataFrame]:
    """Traite un fichier CSV météo et retourne un DataFrame avec uniquement les colonnes pertinentes."""
    try:
        filename = blob.name.split('/')[-1]
        local_path = os.path.join(temp_dir, filename)
        
        # Extraire l'ID de la station du nom de fichier
        station_id = filename.split('_')[0]
        
        # Trouver les informations de la station
        station_info = next((s for s in stations_info if s['id'] == station_id), 
                           {'name': 'Unknown', 'zone': 'Unknown'})
        
        # Télécharger le fichier localement
        blob.download_to_filename(local_path)
        
        # Charger le CSV (sauter les 5 premières lignes de métadonnées)
        df = pd.read_csv(local_path, skiprows=5)
        
        if df.empty:
            logger.warning(f"Fichier vide: {filename}")
            return None
        
        # Sélectionner uniquement les colonnes pertinentes pour l'analyse taxi
        relevant_columns = [
            'station', 'valid', 'lat', 'lon',
            'tmpf',    # température 
            'p01i',    # précipitations
            'vsby',    # visibilité
            'sknt',    # vitesse du vent
            'gust',    # rafales
            'skyc1',   # condition du ciel
            'wxcodes'  # codes météo
        ]
        
        # Garder seulement les colonnes qui existent dans le dataframe
        existing_columns = [col for col in relevant_columns if col in df.columns]
        df = df[existing_columns]
        
        # Nettoyer les données numériques pour éviter les erreurs de conversion
        df = clean_numeric_data(df)
        
        # Ajouter les métadonnées avec types explicites
        now = datetime.now(UTC)
        # Convertir file_date directement en objet date pandas
        df['file_date'] = pd.to_datetime(now.strftime('%Y-%m-%d')).date()
        df['processed_at'] = now  # datetime object
        df['station_name'] = station_info.get('name', 'Unknown')
        df['station_zone'] = station_info.get('zone', 'Unknown')
        
        # Convertir la colonne 'valid' en timestamp
        if 'valid' in df.columns:
            df['valid'] = pd.to_datetime(df['valid'], errors='coerce')
        
        logger.info(f"Fichier {filename} traité avec succès: {len(df)} lignes, {len(df.columns)} colonnes sélectionnées")
        return df
    
    except Exception as e:
        logger.error(f"Erreur lors du traitement du fichier {blob.name}: {str(e)}")
        return None


def load_to_bigquery(client: bigquery.Client, dataframe: pd.DataFrame, table_ref: str, 
                    logger: logging.Logger) -> bool:
    """Charge un DataFrame dans BigQuery."""
    try:
        # Configuration du job avec explicit schema et détection automatique
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION]
        )
        
        job = client.load_table_from_dataframe(dataframe, table_ref, job_config=job_config)
        job.result()  # Attendre la fin du job
        
        logger.info(f"Chargement réussi: {len(dataframe)} lignes dans {table_ref}")
        return True
    
    except Exception as e:
        logger.error(f"Erreur lors du chargement dans BigQuery: {str(e)}")
        return False


def process_and_load_weather_data(storage_client: storage.Client, bq_client: bigquery.Client, 
                                 config: Dict[str, Any], files: List[storage.Blob], 
                                 logger: logging.Logger) -> int:
    """Traite et charge les fichiers météo dans BigQuery"""
    dataset_id = config.get('dataset_id', 'weather_data')
    table_id = config.get('table_id', 'taxi_weather_data')  # Nom modifié pour refléter les données optimisées
    stations = config.get('stations', [])
    
    # Création de la structure BigQuery
    ensure_dataset_exists(bq_client, dataset_id, config.get('location', 'US'))
    table_schema = get_weather_table_schema()
    table_ref = create_table_if_not_exists(
        bq_client, 
        dataset_id, 
        table_id, 
        table_schema, 
        partition_field='valid', 
        clustering_fields=['station']
    )
    
    if not files:
        logger.warning("Aucun fichier à traiter")
        return 0
    
    logger.info(f"Traitement de {len(files)} fichiers avec filtrage des colonnes pour l'analyse des taxis")
    
    files_processed = 0
    
    with tempfile.TemporaryDirectory() as temp_dir:
        # Traiter et charger les fichiers un par un pour éviter les problèmes de mémoire
        for blob in files:
            logger.info(f"Traitement du fichier: {blob.name}")
            
            # Traiter le fichier avec uniquement les colonnes pertinentes
            df = process_weather_file(blob, temp_dir, stations, logger)
            
            if df is not None and not df.empty:
                # Charger directement dans BigQuery
                if load_to_bigquery(bq_client, df, table_ref, logger):
                    files_processed += 1
                    logger.info(f"Fichier {blob.name} chargé avec succès: {len(df)} lignes")
                else:
                    logger.error(f"Échec du chargement du fichier {blob.name}")
    
    logger.info(f"Chargement terminé: {files_processed}/{len(files)} fichiers traités avec données optimisées")
    return files_processed


def main():
    """Fonction principale qui orchestre le chargement des données météo vers BigQuery."""
    parser = argparse.ArgumentParser(description='Charger les données météo pertinentes depuis GCS vers BigQuery')
    parser.add_argument('--config', required=True, help='Chemin vers le fichier de configuration YAML')
    parser.add_argument('--mode', choices=['recent', 'all', 'specific'], default='recent',
                      help='Mode de chargement: récent, tout, ou spécifique')
    parser.add_argument('--days', type=int, default=60, 
                      help='Nombre de jours à charger en mode récent')
    parser.add_argument('--start-date', help='Date de début (YYYY-MM-DD) pour le mode spécifique')
    parser.add_argument('--end-date', help='Date de fin (YYYY-MM-DD) pour le mode spécifique')
    args = parser.parse_args()
    
    # Charger la configuration avec la section 'load_weather'
    config = load_config(args.config, 'load_weather')
    
    # Vérifier les paramètres requis
    required_params = ['project_id', 'bucket_name', 'gcs_folder', 'log_folder']
    missing_params = [param for param in required_params if param not in config]
    
    if missing_params:
        print(f"Paramètres manquants dans la configuration: {', '.join(missing_params)}")
        return
    
    # Initialiser le logging
    log_stream, logger = init_logging()
    logger.info(f"Démarrage du chargement des données météo optimisées pour l'analyse des taxis: {datetime.now(UTC)}")
    
    # Initialiser les clients GCP
    storage_client, bq_client = create_gcp_clients(config["project_id"])
    
    try:
        # Déterminer la plage de dates selon le mode
        start_date = None
        end_date = None
        
        if args.start_date and args.end_date:
            # Si les dates sont explicitement fournies, utiliser le mode spécifique
            start_date = args.start_date
            end_date = args.end_date
            logger.info(f"Mode spécifique: chargement des données du {start_date} au {end_date}")
        elif args.mode == 'recent':
            start_date = (datetime.now(UTC) - timedelta(days=args.days)).strftime('%Y-%m-%d')
            logger.info(f"Mode récent: chargement des données depuis {start_date}")
        elif args.mode == 'specific' and args.start_date:
            start_date = args.start_date
            end_date = args.end_date
            logger.info(f"Mode spécifique: chargement des données du {start_date} au {end_date}")
        else:
            logger.info("Mode complet: chargement de toutes les données disponibles")
        
        # Lister les fichiers à traiter
        bucket_name = config["bucket_name"]
        gcs_folder = config["gcs_folder"]
        
        files = list_gcs_files(
            storage_client, 
            bucket_name, 
            gcs_folder, 
            start_date, 
            end_date
        )
        logger.info(f"Nombre de fichiers à traiter: {len(files)}")
        
        # Traiter et charger les fichiers avec données optimisées
        files_processed = process_and_load_weather_data(
            storage_client,
            bq_client,
            config,
            files,
            logger
        )
        
        logger.info(f"Chargement des données météo optimisées terminé! {files_processed} fichiers traités.")
    except Exception as e:
        logger.error(f"Erreur inattendue : {str(e)}")
    finally:
        upload_log(storage_client, config, log_stream, logger)


if __name__ == '__main__':
    main()