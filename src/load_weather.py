import argparse
import io
import logging
import os
import tempfile
import yaml
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional, Set, Tuple

import pandas as pd
from google.cloud import storage, bigquery

# ------------------------------------------------------------------------------
# Fonctions de configuration
# ------------------------------------------------------------------------------

def load_config(config_path: str, section: Optional[str] = None) -> Dict[str, Any]:
    """Charge la configuration depuis un fichier YAML, éventuellement une section spécifique."""
    with open(config_path, 'r') as file:
        config = yaml.safe_load(file)
    
    # Résoudre les références dans la configuration
    resolved_config = resolve_config_references(config)
    
    if section and section in resolved_config:
        # Fusionner les paramètres globaux avec les paramètres de la section,
        # en excluant les autres sections spécifiques
        section_config = {k: v for k, v in resolved_config.items() 
                          if k not in ['extract', 'extract_weather', 'load', 'load_weather']}
        section_config.update(resolved_config[section])
        return section_config
    
    return resolved_config

def resolve_config_references(config: Dict[str, Any]) -> Dict[str, Any]:
    """Résout les références dans la configuration, comme ${project_id}."""
    config_str = yaml.dump(config)
    for key, value in config.items():
        if isinstance(value, str):
            placeholder = f"${{{key}}}"
            config_str = config_str.replace(placeholder, value)
    return yaml.safe_load(config_str)

# ------------------------------------------------------------------------------
# Fonctions de logging
# ------------------------------------------------------------------------------

def init_logging(debug: bool = False) -> Tuple[io.StringIO, logging.Logger]:
    """Initialise et configure le système de journalisation (affichage console et stockage en mémoire)."""
    log_stream = io.StringIO()
    logging_format = "%(asctime)s - %(levelname)s - %(message)s"
    
    # Logger identifié par le module courant
    logger = logging.getLogger(__name__)
    
    # Définir le niveau de log
    if debug:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)
    
    # Handler console
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter(logging_format))
    
    # Handler pour le buffer en mémoire
    stream_handler = logging.StreamHandler(log_stream)
    stream_handler.setFormatter(logging.Formatter(logging_format))
    
    # Nettoyage des anciens handlers pour éviter les doublons
    logger.handlers.clear()
    logger.addHandler(console_handler)
    logger.addHandler(stream_handler)
    
    return log_stream, logger

# ------------------------------------------------------------------------------
# Fonctions pour la connexion à GCP
# ------------------------------------------------------------------------------

def create_gcp_clients(project_id: str) -> Tuple[storage.Client, bigquery.Client]:
    """Crée et retourne les clients GCP pour Storage et BigQuery."""
    storage_client = storage.Client(project=project_id)
    bq_client = bigquery.Client(project=project_id)
    return storage_client, bq_client

# ------------------------------------------------------------------------------
# Fonctions d'upload de logs
# ------------------------------------------------------------------------------

def upload_to_gcs(client: storage.Client, bucket_name: str, path: str, content: str) -> None:
    """Télécharge le contenu dans GCS."""
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(path)
    blob.upload_from_string(content)

def upload_log(client: storage.Client, config: Dict[str, Any], log_stream: io.StringIO, logger: logging.Logger) -> None:
    """Télécharge les logs dans GCS."""
    bucket_name = config["bucket_name"]
    log_folder = config["log_folder"]
    timestamp = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')
    log_filename = f"{log_folder}load_weather_log_{timestamp}.log"
    upload_to_gcs(client, bucket_name, log_filename, log_stream.getvalue())
    logger.info(f"Log file uploaded to {log_filename}")

# ------------------------------------------------------------------------------
# Fonctions de gestion de BigQuery
# ------------------------------------------------------------------------------

def ensure_dataset_exists(client: bigquery.Client, dataset_id: str, location: str = "US") -> str:
    """Vérifie si le dataset existe ; le crée sinon."""
    dataset_ref = f"{client.project}.{dataset_id}"
    try:
        client.get_dataset(dataset_ref)
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
        client.get_table(table_ref)
    except Exception:
        table = bigquery.Table(table_ref, schema=schema)
        if partition_field:
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field=partition_field
            )
        if clustering_fields:
            table.clustering_fields = clustering_fields
        client.create_table(table)
    return table_ref

def get_weather_table_schema() -> List[bigquery.SchemaField]:
    """Renvoie le schéma optimisé de la table météo pour l'analyse des taxis."""
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
        bigquery.SchemaField("station_zone", "STRING", description="Zone géographique"),
        bigquery.SchemaField("source_file", "STRING", description="Nom du fichier source")
    ]

# ------------------------------------------------------------------------------
# Fonctions de gestion des fichiers sur GCS
# ------------------------------------------------------------------------------

def list_gcs_files(client: storage.Client, bucket_name: str, prefix: str, 
                   start_date: str = None, end_date: str = None) -> List[storage.Blob]:
    """Liste les fichiers dans GCS répondant aux critères (filtrage par dates si fourni)."""
    bucket = client.bucket(bucket_name)
    blobs = list(bucket.list_blobs(prefix=prefix))
    
    if start_date or end_date:
        filtered_blobs = []
        for blob in blobs:
            try:
                # On suppose que le nom du fichier comporte une date sous format YYYYMMDD en 2e position
                filename = blob.name.split('/')[-1]
                parts = filename.split('_')
                file_date = parts[1]  # format attendu: YYYYMMDD
                
                if start_date and file_date < start_date.replace('-', ''):
                    continue
                if end_date and file_date > end_date.replace('-', ''):
                    continue
                
                filtered_blobs.append(blob)
            except Exception:
                filtered_blobs.append(blob)
        return filtered_blobs
    return blobs

# ------------------------------------------------------------------------------
# Fonctions pour vérifier les fichiers déjà traités
# ------------------------------------------------------------------------------

def get_existing_weather_files(bq_client: bigquery.Client, config: Dict[str, Any], logger: logging.Logger) -> Set[str]:
    """Récupère la liste des noms de fichiers déjà traités dans la table à partir de la colonne 'source_file'."""
    try:
        table_id = config['table_id']
        location = config.get('location', 'US')
        
        query = f"""
            SELECT DISTINCT source_file
            FROM `{table_id}`
            WHERE source_file IS NOT NULL
        """
        
        logger.info(f"Récupération des fichiers existants depuis {table_id}")
        query_job = bq_client.query(query, location=location)
        existing_files = {row[0] for row in query_job.result()}
        
        logger.info(f"Nombre de fichiers existants : {len(existing_files)}")
        logger.debug(f"Liste des fichiers existants : {existing_files}")
        
        return existing_files
    except Exception as e:
        logger.warning(f"Erreur lors de la récupération des fichiers existants: {str(e)}")
        logger.warning("Cela peut être normal si la table n'existe pas encore.")
        return set()

# ------------------------------------------------------------------------------
# Traitement des données météo
# ------------------------------------------------------------------------------

def clean_numeric_data(df: pd.DataFrame) -> pd.DataFrame:
    """Nettoie les colonnes numériques pour BigQuery."""
    numeric_columns = ['tmpf', 'p01i', 'vsby', 'sknt', 'gust', 'lon', 'lat']
    for col in numeric_columns:
        if col in df.columns:
            df[col] = df[col].astype(str)
            df[col] = df[col].replace(['M', 'NA', 'None', 'null', '', 'nan'], None)
            df[col] = pd.to_numeric(df[col], errors='coerce')
    return df

def process_weather_file(blob: storage.Blob, temp_dir: str, stations_info: List[Dict[str, Any]], 
                         logger: logging.Logger) -> Optional[pd.DataFrame]:
    """Traite un fichier CSV météo et retourne un DataFrame contenant uniquement les colonnes pertinentes."""
    try:
        filename = blob.name.split('/')[-1]
        local_path = os.path.join(temp_dir, filename)
        
        # Extraire l'ID de la station à partir du nom de fichier
        station_id = filename.split('_')[0]
        station_info = next((s for s in stations_info if s['id'] == station_id), 
                            {'name': 'Unknown', 'zone': 'Unknown'})
        
        # Télécharger le fichier en local
        blob.download_to_filename(local_path)
        df = pd.read_csv(local_path, skiprows=5)
        
        if df.empty:
            logger.warning(f"Fichier vide: {filename}")
            return None
        
        relevant_columns = [
            'station', 'valid', 'lat', 'lon', 'tmpf', 'p01i', 'vsby', 
            'sknt', 'gust', 'skyc1', 'wxcodes'
        ]
        
        existing_columns = [col for col in relevant_columns if col in df.columns]
        df = df[existing_columns]
        df = clean_numeric_data(df)
        
        now = datetime.now(timezone.utc)
        df['file_date'] = pd.to_datetime(now.strftime('%Y-%m-%d')).date()
        df['processed_at'] = now
        df['station_name'] = station_info.get('name', 'Unknown')
        df['station_zone'] = station_info.get('zone', 'Unknown')
        df['source_file'] = filename  # Ajout du nom de fichier pour le suivi
        
        if 'valid' in df.columns:
            df['valid'] = pd.to_datetime(df['valid'], errors='coerce')
        
        logger.info(f"Fichier {filename} traité avec succès: {len(df)} lignes, {len(df.columns)} colonnes")
        return df
    
    except Exception as e:
        logger.error(f"Erreur lors du traitement du fichier {blob.name}: {str(e)}")
        return None

def load_to_bigquery(client: bigquery.Client, dataframe: pd.DataFrame, table_ref: str, 
                     logger: logging.Logger) -> bool:
    """Charge un DataFrame dans BigQuery en mode APPEND."""
    try:
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION]
        )
        job = client.load_table_from_dataframe(dataframe, table_ref, job_config=job_config)
        job.result()  # Attendre la fin du chargement
        
        logger.info(f"Chargement réussi: {len(dataframe)} lignes dans {table_ref}")
        return True
    except Exception as e:
        logger.error(f"Erreur lors du chargement dans BigQuery: {str(e)}")
        return False

def process_and_load_weather_data(storage_client: storage.Client, bq_client: bigquery.Client, 
                                 config: Dict[str, Any], files: List[storage.Blob], 
                                 existing_files: Set[str], logger: logging.Logger, 
                                 stations: List[Dict[str, Any]]) -> int:
    """Traite et charge les fichiers météo dans BigQuery."""
    # Vérifier si table_id de la configuration est déjà un chemin complet
    config_table_id = config.get('table_id', '')
    
    if '.' in config_table_id and config_table_id.count('.') == 2:
        # Si c'est déjà un chemin complet (project.dataset.table), l'utiliser directement
        table_ref = config_table_id
        # Extraire le dataset_id du table_id pour ensure_dataset_exists
        parts = table_ref.split('.')
        dataset_id = parts[1]
        table_name = parts[2]  # Le nom de la table sans le projet/dataset
    else:
        # Sinon, construire le chemin comme avant
        dataset_id = config.get('dataset_id', 'weather_data')
        table_name = config.get('table_name', 'raw_weather_data')
        table_ref = f"{bq_client.project}.{dataset_id}.{table_name}"
    
    # Création ou vérification de la structure BigQuery
    ensure_dataset_exists(bq_client, dataset_id, config.get('location', 'US'))
    table_schema = get_weather_table_schema()
    
    # Création de la table si nécessaire - passer uniquement le nom de la table
    table_ref = create_table_if_not_exists(
        bq_client, 
        dataset_id, 
        table_name,  # Utiliser seulement le nom de la table
        table_schema, 
        partition_field='valid', 
        clustering_fields=['station']
    )
    
    if not files:
        logger.warning("Aucun fichier à traiter")
        return 0
    
    logger.info(f"Traitement de {len(files)} fichiers pour l'analyse météo")
    files_processed = 0
    
    with tempfile.TemporaryDirectory() as temp_dir:
        for blob in files:
            filename = blob.name.split('/')[-1]
            
            # Vérification supplémentaire que le fichier n'a pas déjà été traité
            if filename in existing_files:
                logger.info(f"Fichier {filename} déjà traité, ignoré.")
                continue
                
            logger.info(f"Traitement du fichier: {blob.name}")
            df = process_weather_file(blob, temp_dir, stations, logger)
            
            if df is not None and not df.empty:
                if load_to_bigquery(bq_client, df, table_ref, logger):
                    files_processed += 1
                    logger.info(f"Fichier {blob.name} chargé avec succès")
                else:
                    logger.error(f"Échec du chargement du fichier {blob.name}")
    
    logger.info(f"Chargement terminé: {files_processed}/{len(files)} fichiers traités")
    return files_processed

# ------------------------------------------------------------------------------
# Pipeline principal
# ------------------------------------------------------------------------------

def load_weather_pipeline(config_path: str = None, debug: bool = False):
    """Pipeline complet de chargement des données météo depuis GCS vers BigQuery."""
    if not config_path:
        config_path = '/home/airflow/gcs/dags/config.yaml'
    
    # Charger la configuration avec la section 'load_weather'
    config = load_config(config_path, 'load_weather')
    stations = config.get("stations", [])
    
    # Vérifier que les paramètres requis sont présents
    required_params = ['project_id', 'bucket_name', 'gcs_folder', 'log_folder', 'table_id']
    missing_params = [param for param in required_params if param not in config]
    if missing_params:
        print(f"Paramètres manquants dans la configuration: {', '.join(missing_params)}")
        return
    
    # Initialiser les logs
    log_stream, logger = init_logging(debug)
    logger.info(f"Démarrage du pipeline de chargement des données météo: {datetime.now(timezone.utc)}")
    
    # Initialiser les clients GCP
    storage_client, bq_client = create_gcp_clients(config["project_id"])
    
    try:
        bucket_name = config["bucket_name"]
        gcs_folder = config["gcs_folder"]
        
        # Récupérer l'ensemble des fichiers sur GCS
        all_blobs = list_gcs_files(storage_client, bucket_name, gcs_folder)
        all_filenames = [blob.name.split('/')[-1] for blob in all_blobs]
        logger.info(f"Nombre total de fichiers sur GCS : {len(all_blobs)}")
        logger.debug(f"Liste des fichiers GCS : {all_filenames[:10]}...")
        
        # Récupérer la liste des fichiers déjà chargés
        existing_files = get_existing_weather_files(bq_client, config, logger)
        
        # Filtrer pour ne garder que les nouveaux fichiers
        new_files = [blob for blob in all_blobs if blob.name.split('/')[-1] not in existing_files]
        logger.info(f"Nombre de nouveaux fichiers à traiter : {len(new_files)}")
        
        if new_files:
            new_filenames = [blob.name.split('/')[-1] for blob in new_files[:10]]
            logger.debug(f"Exemple de nouveaux fichiers : {new_filenames}...")
        
        # Processer et charger les fichiers
        process_and_load_weather_data(storage_client, bq_client, config, new_files, existing_files, logger,stations)
        logger.info("Chargement des données météo terminé!")
    
    except Exception as e:
        logger.error(f"Erreur durant le pipeline : {str(e)}")
        raise
    finally:
        try:
            upload_log(storage_client, config, log_stream, logger)
        except Exception as e:
            print(f"Erreur lors du téléchargement des logs: {str(e)}")
            print(log_stream.getvalue())

# ------------------------------------------------------------------------------
# Fonction principale
# ------------------------------------------------------------------------------

def main():
    try:
        if 'AIRFLOW_CTX_DAG_ID' in os.environ:
            # Exécution via Airflow : utiliser valeurs par défaut
            return load_weather_pipeline()
        else:
            # Exécution locale avec argparse
            parser = argparse.ArgumentParser(
                description='Charger les données météo depuis GCS vers BigQuery'
            )
            parser.add_argument('--config', required=True, help='Chemin vers le fichier de configuration YAML')
            parser.add_argument('--debug', action='store_true', help='Activer le mode debug')
            args = parser.parse_args()
            return load_weather_pipeline(args.config, args.debug)
    except Exception as e:
        logging.error(f"Erreur dans la fonction main: {str(e)}")
        raise


if __name__ == '__main__':
    main()