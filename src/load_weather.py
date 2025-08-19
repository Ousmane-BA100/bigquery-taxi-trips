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
import re

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

def ensure_dataset_exists(client: bigquery.Client, dataset_id: str, location: str = 'europe-west9') -> str:
    """Vérifie si le dataset existe ; le crée sinon."""
    logger = logging.getLogger(__name__)
    dataset_ref = f"{client.project}.{dataset_id}"
    try:
        client.get_dataset(dataset_ref)
        logger.info(f"Le dataset {dataset_id} existe déjà dans la région {location}")
    except Exception:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = location
        client.create_dataset(dataset)
        logger.info(f"Création du dataset {dataset_id} dans la région {location}")
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
    """Retourne le schéma de la table météo"""
    return [
        bigquery.SchemaField("station", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("valid", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("lon", "FLOAT", mode="REQUIRED"),
        bigquery.SchemaField("lat", "FLOAT", mode="REQUIRED"),
        bigquery.SchemaField("temp_f", "FLOAT", description="Température en Fahrenheit"),
        bigquery.SchemaField("precip_in", "FLOAT", description="Précipitations en pouces"),
        bigquery.SchemaField("vis_miles", "FLOAT", description="Visibilité en miles"),
        bigquery.SchemaField("wind_speed_kt", "FLOAT", description="Vitesse du vent en nœuds"),
        bigquery.SchemaField("wind_dir_deg", "FLOAT", description="Direction du vent en degrés"),
        bigquery.SchemaField("wind_gust_kt", "FLOAT", description="Rafales de vent en nœuds"),
        bigquery.SchemaField("sky_cover1", "STRING", description="Couverture nuageuse (couche 1)"),
        bigquery.SchemaField("weather_codes", "STRING", description="Codes météorologiques"),
        bigquery.SchemaField("file_date", "DATE", description="Date du fichier source"),
        bigquery.SchemaField("processed_at", "TIMESTAMP", description="Date de traitement"),
        bigquery.SchemaField("station_name", "STRING", description="Nom de la station"),
        bigquery.SchemaField("station_zone", "STRING", description="Zone géographique"),
        bigquery.SchemaField("source_file", "STRING", description="Nom du fichier source")
    ]

# ------------------------------------------------------------------------------
# Fonctions de gestion des fichiers sur GCS
# ------------------------------------------------------------------------------

def list_gcs_files(storage_client, bucket_name, prefix, start_date=None, end_date=None, logger=None):
    """Liste les fichiers dans un bucket GCS avec un préfixe donné."""
    try:
        blobs = storage_client.list_blobs(bucket_name, prefix=prefix)
        
        # Convertir les objets Blob en noms de fichiers
        files = [blob.name for blob in blobs if blob.name.endswith('.csv')]
        
        if logger:
            logger.info(f"Fichiers trouvés : {files}")
        return files
        
    except Exception as e:
        if logger:
            logger.error(f"Erreur lors de la liste des fichiers GCS: {str(e)}")
        raise

# ------------------------------------------------------------------------------
# Fonctions pour vérifier les fichiers déjà traités
# ------------------------------------------------------------------------------

def get_existing_weather_files(bq_client: bigquery.Client, config: Dict[str, Any], logger: logging.Logger) -> Set[str]:
    """Récupère la liste des noms de fichiers déjà traités dans la table à partir de la colonne 'source_file'."""
    try:
        table_id = config['table_id']
        location = config.get('location', 'europe-west9')
        
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
                         config: Dict[str, Any], logger: logging.Logger) -> Optional[pd.DataFrame]:
    """Traite un fichier météo et retourne un DataFrame nettoyé et formaté."""
    temp_path = None
    try:
        # Extraire le nom du fichier
        filename = blob.name.split('/')[-1]
        logger.info(f"Traitement du fichier : {filename}")
        
        # Créer un fichier temporaire
        temp_path = os.path.join(temp_dir, filename)
        blob.download_to_filename(temp_path)
        
        # Lire le fichier CSV
        df = pd.read_csv(temp_path, comment='#', low_memory=False)
        
        # Vérifier les colonnes obligatoires
        required_columns = ['station', 'valid', 'lon', 'lat', 'tmpf', 'p01i', 'vsby', 'sknt']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            logger.error(f"Colonnes manquantes dans {filename}: {', '.join(missing_columns)}")
            return None
        
        # Convertir les colonnes numériques
        numeric_columns = {
            'lon': float, 'lat': float, 'tmpf': float, 'p01i': float, 
            'vsby': float, 'sknt': float, 'drct': float, 'gust': float
        }
        
        for col, dtype in numeric_columns.items():
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Renommer les colonnes
        df = df.rename(columns={
            'tmpf': 'temp_f',
            'p01i': 'precip_in',
            'vsby': 'vis_miles',
            'sknt': 'wind_speed_kt',
            'drct': 'wind_dir_deg',
            'gust': 'wind_gust_kt',
            'skyc1': 'sky_cover1',
            'wxcodes': 'weather_codes'
        })
        
        # Convertir les dates
        df['valid'] = pd.to_datetime(df['valid'], errors='coerce')
        
        # Extraire la date du nom de fichier (format: STATION_YYYYMMDD_YYYYMMDD.csv)
        date_match = re.search(r'_(\d{8})_', filename)
        if date_match:
            file_date = datetime.strptime(date_match.group(1), '%Y%m%d').date()
            df['file_date'] = file_date
        else:
            df['file_date'] = pd.NaT
        
        # Ajouter les métadonnées
        df['processed_at'] = pd.Timestamp.now(tz='UTC')
        
        # Ajouter les informations de la station
        station_id = filename.split('_')[0].upper()
        station_info = next((s for s in stations_info if s['id'] == station_id), None)
        
        if station_info:
            df['station_name'] = station_info.get('name', 'Inconnu')
            df['station_zone'] = station_info.get('zone', 'Inconnu')
        else:
            df['station_name'] = station_id
            df['station_zone'] = 'Inconnu'
        
        df['source_file'] = filename
        
        # Sélectionner et ordonner les colonnes
        final_columns = [
            'station', 'valid', 'lon', 'lat', 'temp_f', 'precip_in', 'vis_miles',
            'wind_speed_kt', 'wind_dir_deg', 'wind_gust_kt', 'sky_cover1',
            'weather_codes', 'file_date', 'processed_at', 'station_name',
            'station_zone', 'source_file'
        ]
        
        # Ne garder que les colonnes existantes
        final_columns = [col for col in final_columns if col in df.columns]
        df = df[final_columns]
        
        logger.info(f"Fichier {filename} traité avec succès: {len(df)} lignes")
        return df
        
    except Exception as e:
        logger.error(f"Erreur lors du traitement du fichier {filename}: {str(e)}", exc_info=True)
        return None
        
    finally:
        # Nettoyer le fichier temporaire
        if temp_path and os.path.exists(temp_path):
            try:
                os.remove(temp_path)
            except Exception as e:
                logger.warning(f"Impossible de supprimer le fichier temporaire {temp_path}: {str(e)}")

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
                                 config: Dict[str, Any], logger: logging.Logger) -> int:
    """Traite et charge les fichiers météo dans BigQuery."""
    try:
        # Récupérer la configuration météo
        weather_config = config.get('weather', {})
        load_config = config.get('load_weather', {})
        
        # Configuration BigQuery
        dataset_id = load_config.get('dataset_id', 'weather_data')
        table_name = load_config.get('table_name', 'weather_observations')
        location = load_config.get('location', 'europe-west9')
        
        # S'assurer que le dataset existe
        ensure_dataset_exists(bq_client, dataset_id, location)
        
        # Supprimer la table si elle existe
        table_ref = f"{dataset_id}.{table_name}"
        try:
            bq_client.delete_table(table_ref, not_found_ok=True)
            logger.info(f"Table {table_ref} supprimée avec succès")
        except Exception as e:
            logger.warning(f"Erreur lors de la suppression de la table {table_ref}: {str(e)}")
        
        # Recréer la table avec le bon schéma
        schema = get_weather_table_schema()
        table = create_table_if_not_exists(
            client=bq_client,
            dataset_id=dataset_id,
            table_id=table_name,
            schema=schema,
            partition_field='valid',
            clustering_fields=['station']
        )
        
        # Lister les fichiers à traiter
        gcs_folder = weather_config.get('gcs_folder', 'weather_data/')
        files = list_gcs_files(
            storage_client,
            config['bucket_name'],
            gcs_folder,
            start_date=config.get('start_date'),
            end_date=datetime.now().strftime('%Y-%m-%d'),
            logger=logger
        )
        
        if not files:
            logger.warning("Aucun fichier météo trouvé dans le bucket")
            return 0
            
        # Récupérer la liste des fichiers déjà traités
        try:
            existing_files = get_existing_weather_files(bq_client, config, logger)
        except Exception as e:
            logger.warning(f"Erreur lors de la récupération des fichiers existants: {str(e)}")
            logger.warning("Cela peut être normal si la table n'existe pas encore.")
            existing_files = set()
        
        # Filtrer les fichiers déjà traités
        files_to_process = []
        for file_path in files:
            # Extraire uniquement le nom du fichier (dernière partie du chemin)
            file_name = file_path.split('/')[-1]
            if file_name not in existing_files:
                files_to_process.append(file_path)

        if not files_to_process:
            logger.info("Tous les fichiers ont déjà été traités")
            return 0
            
        logger.info(f"Traitement de {len(files_to_process)} fichiers météo...")
        
        # Créer un répertoire temporaire pour les fichiers téléchargés
        with tempfile.TemporaryDirectory() as temp_dir:
            total_processed = 0
            
            # Informations sur les stations
            stations_info = [
                {'id': 'JFK', 'name': 'John F. Kennedy International Airport', 'zone': 'New York'},
                {'id': 'LGA', 'name': 'LaGuardia Airport', 'zone': 'New York'}
            ]
            
            # Traiter chaque fichier
            for file_path in files_to_process:
                try:
                    # Télécharger et traiter le fichier
                    blob = storage_client.bucket(config['bucket_name']).blob(file_path)
                    df = process_weather_file(blob, temp_dir, stations_info, config, logger)
                    
                    if df is not None and not df.empty:
                        # Charger les données dans BigQuery
                        if load_to_bigquery(bq_client, df, table_ref, logger):
                            total_processed += 1
                            
                except Exception as e:
                    logger.error(f"Erreur lors du traitement du fichier {file_path}: {str(e)}", 
                               exc_info=True)
                    continue
                    
        logger.info(f"Traitement terminé. {total_processed} fichiers chargés avec succès.")
        return total_processed
        
    except Exception as e:
        logger.error(f"Erreur lors du traitement des données météo: {str(e)}", exc_info=True)
        raise

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

def main(config_path: str = None, debug: bool = False) -> None:
    """Fonction principale."""
    # Initialiser le logging
    log_stream, logger = init_logging(debug)
    
    try:
        # Charger la configuration
        config = load_config(config_path or 'config.yaml')
        
        # Initialiser les clients GCP
        storage_client, bq_client = create_gcp_clients(config['project_id'])
        
        # Exécuter le pipeline de chargement
        files_processed = process_and_load_weather_data(storage_client, bq_client, config, logger)
        
        logger.info(f"Chargement terminé. {files_processed} fichiers traités avec succès.")
        
    except Exception as e:
        logger.error(f"Erreur lors de l'exécution du script : {str(e)}", exc_info=True)
        return 1
    finally:
        # Télécharger les logs
        try:
            if 'log_folder' in config:
                upload_log(storage_client, config, log_stream, logger)
        except Exception as e:
            print(f"Erreur lors de l'enregistrement des logs : {str(e)}")
    
    return 0


if __name__ == '__main__':
    main()