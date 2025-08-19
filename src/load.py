import os
import logging
import argparse
from google.cloud import bigquery, storage
from datetime import datetime, timezone
import io
from concurrent.futures import ThreadPoolExecutor, as_completed
import yaml
from typing import Dict, Set, List, Tuple, Any, Optional
import time
import traceback

def setup_logging():
    """
    Configure la journalisation pour éviter les récursions.
    Cette version est spécialement conçue pour fonctionner avec Airflow.
    """
    # Utilise un logger nommé spécifique
    logger = logging.getLogger('nyc_taxi_elt')
    
    # Si le logger a déjà des handlers, ne rien faire
    if logger.handlers:
        return logger
    
    # Désactive la propagation pour éviter les doublons
    logger.propagate = False
    
    # Configure le niveau de log
    logger.setLevel(logging.INFO)
    
    # Crée un formateur
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    
    # Crée un handler de console
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    
    # Ajoute le handler au logger
    logger.addHandler(console_handler)
    
    return logger

def load_config(config_path: str, section: Optional[str] = None) -> Dict[str, Any]:
    """Charge la configuration depuis un fichier YAML, éventuellement une section spécifique."""
    logger = logging.getLogger('nyc_taxi_elt')
    
    try:
        with open(config_path, 'r') as file:
            full_config = yaml.safe_load(file)
            logger.debug(f"Configuration complète chargée: {list(full_config.keys())}")
        
        # Si une section spécifique est demandée
        if section and section in full_config:
            logger.debug(f"Extraction de la section '{section}'")
            
            # Extraire les paramètres globaux (qui ne sont pas des sections)
            global_params = {k: v for k, v in full_config.items() 
                           if not isinstance(v, dict)}
            logger.debug(f"Paramètres globaux: {global_params}")
            
            # Créer une nouvelle configuration en combinant les deux
            combined_config = global_params.copy()
            combined_config.update(full_config[section])
            logger.debug(f"Configuration combinée avant résolution: {combined_config}")
            
            # Résoudre les références
            resolved_config = resolve_config_references(combined_config)
            logger.debug(f"Configuration après résolution: {resolved_config}")
            return resolved_config
        
        # Si aucune section n'est spécifiée, résoudre les références dans la config complète
        logger.debug("Résolution des références dans la configuration complète")
        return resolve_config_references(full_config)
    
    except Exception as e:
        logger.error(f"Erreur lors du chargement de la configuration: {str(e)}")
        logger.debug(f"Traceback: {traceback.format_exc()}")
        raise

def resolve_config_references(config: Dict[str, Any]) -> Dict[str, Any]:
    """Résout les références dans la configuration, comme ${project_id}."""
    logger = logging.getLogger('nyc_taxi_elt')
    
    # Extraire d'abord toutes les valeurs de premier niveau
    global_values = {k: v for k, v in config.items() if isinstance(v, str)}
    
    # Convertir en YAML pour faciliter les remplacements de chaînes
    config_str = yaml.dump(config)
    
    # Remplacer les références par leurs valeurs
    for key, value in global_values.items():
        placeholder = f"${{{key}}}"
        if placeholder in config_str:
            logger.debug(f"Remplacement de '{placeholder}' par '{value}'")
            config_str = config_str.replace(placeholder, value)
    
    # Reconvertir en dictionnaire
    resolved_config = yaml.safe_load(config_str)
    
    return resolved_config

def initialize_clients(config: Dict[str, Any]) -> Tuple[bigquery.Client, storage.Client]:
    """Initialise et retourne les clients BigQuery et Storage."""
    project_id = config['project_id']
    bq_client = bigquery.Client(project=project_id, location=config.get('location', 'europe-west9'))
    storage_client = storage.Client(project=project_id)
    return bq_client, storage_client

def upload_log_to_gcs(storage_client: storage.Client, config: Dict[str, Any], log_stream: io.StringIO) -> None:
    """Télécharge le fichier journal vers GCS."""
    log_filename = f"{config['log_folder']}load_log_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.log"
    bucket = storage_client.bucket(config['bucket_name'])
    blob = bucket.blob(log_filename)
    blob.upload_from_string(log_stream.getvalue())
    logging.info(f"Fichier journal téléchargé vers {log_filename}")

def get_existing_files(bq_client: bigquery.Client, config: Dict[str, Any]) -> Set[str]:
    """Récupère la liste des fichiers déjà chargés dans BigQuery."""
    try:
        query = f"""
            SELECT DISTINCT {config['source_file_column']} 
            FROM `{config['destination_table']}`
            WHERE {config['source_file_column']} IS NOT NULL
        """
        query_job = bq_client.query(query, location=config['location'])
        return {row[0] for row in query_job.result()}
    except Exception as e:
        logging.warning(f"Erreur lors de la récupération des fichiers existants: {str(e)}")
        logging.warning("Cela peut être normal si la table cible n'existe pas encore.")
        return set()

def get_gcs_files(storage_client: storage.Client, config: Dict[str, Any]) -> Set[str]:
    """Récupère la liste des fichiers Parquet depuis GCS."""
    bucket = storage_client.bucket(config['bucket_name'])
    blobs = bucket.list_blobs(prefix=config['gcs_folder'])
    return {blob.name.split('/')[-1] for blob in blobs if blob.name.endswith(config['file_extension'])}

def generate_transformation_query(config: Dict[str, Any], temp_table: str, file_name: str) -> str:
    """Génère la requête SQL pour transformer et insérer les données."""
    # Construire dynamiquement la requête en fonction de la configuration
    field_transformations = []
    
    if 'fields' in config:
        for field in config['fields']:
            if 'transformation' in field:
                field_transformations.append(f"{field['transformation']} AS {field['name']}")
            else:
                field_transformations.append(field['name'])
    else:
        # Fallback si aucun champ n'est spécifié
        logging.warning("Aucun champ spécifié dans la configuration, utilisation de '*'")
        field_transformations.append("*")
    
    # Ajouter la colonne pour le nom du fichier source
    field_transformations.append(f"\"{file_name}\" AS {config['source_file_column']}")
    
    # Construire la requête complète
    query = f"""
    INSERT INTO `{config['destination_table']}`
    SELECT 
        {', '.join(field_transformations)}
    FROM `{temp_table}`
    """
    return query

def process_file(bq_client: bigquery.Client, config: Dict[str, Any], file_name: str) -> bool:
    """Traite un fichier et le charge dans BigQuery."""
    try:
        import time
        import pandas as pd
        import pyarrow.parquet as pq
        from io import BytesIO
        from google.cloud import storage
        
        logger = logging.getLogger(__name__)
        load_config = config.get('load', {})
        
        # Récupérer les paramètres de format
        file_format = load_config.get('file_format', 'parquet')
        format_params = config.get('file_formats', {}).get(file_format, {})
        
        # Configuration du job de chargement
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET if file_format == 'parquet' 
                       else bigquery.SourceFormat.CSV,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            autodetect=True,
            # Permet l'ajout de colonnes et la conversion de type
            schema_update_options=[
                bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
                bigquery.SchemaUpdateOption.ALLOW_FIELD_RELAXATION
            ]
        )
        
        # Configurations spécifiques au format CSV
        if file_format == 'csv':
            job_config.field_delimiter = format_params.get('delimiter', ',')
            job_config.encoding = format_params.get('encoding', 'UTF-8')
            job_config.skip_leading_rows = 1  # Ignorer l'en-tête pour les CSV
        
        # Chemin complet du fichier dans GCS
        gcs_folder = load_config.get('gcs_folder', 'taxi_data/').rstrip('/') + '/'
        uri = f"gs://{config['bucket_name']}/{gcs_folder}{file_name}"
        
        # Télécharger le fichier depuis GCS
        storage_client = storage.Client()
        bucket = storage_client.bucket(config['bucket_name'])
        blob = bucket.blob(f"{gcs_folder}{file_name}")
        
        # Lire le fichier en mémoire
        file_data = blob.download_as_bytes()
        
        # Si c'est un fichier Parquet, on peut le traiter avec pandas
        if file_format == 'parquet':
            # Lire le fichier Parquet
            table = pq.read_table(BytesIO(file_data))
            
            # Convertir en pandas DataFrame pour le traitement
            df = table.to_pandas()
            
            # Convertir passenger_count en entier si nécessaire
            if 'passenger_count' in df.columns:
                # Remplacer les valeurs NaN par 0 (ou une autre valeur par défaut)
                df['passenger_count'] = df['passenger_count'].fillna(0)
                # Convertir en entier
                df['passenger_count'] = df['passenger_count'].astype(int)
            
            # Convertir à nouveau en parquet en mémoire
            output = BytesIO()
            df.to_parquet(output, index=False)
            file_data = output.getvalue()
        
        # Charger les données modifiées dans BigQuery
        load_job = bq_client.load_table_from_file(
            BytesIO(file_data),
            load_config['destination_table'],
            job_config=job_config
        )
        
        # Attendre la fin du chargement avec un timeout
        load_job.result(timeout=300)  # 5 minutes de timeout
        
        if load_job.state == 'DONE':
            logger.info(f"Fichier {file_name} chargé avec succès")
            return True
        else:
            logger.error(f"Le chargement du fichier {file_name} a échoué")
            return False
        
    except Exception as e:
        logger.error(f"Erreur lors du chargement du fichier {file_name}: {str(e)}", 
                   exc_info=logger.level <= logging.DEBUG)
        return False

def load_new_files(bq_client: bigquery.Client, storage_client: storage.Client, 
                  config: Dict[str, Any], max_workers: int = 2) -> int:
    """Charge les nouveaux fichiers depuis GCS vers BigQuery."""
    try:
        logger = logging.getLogger(__name__)
        load_config = config.get('load', {})
        
        # Vérifier que la configuration est valide
        required_in_load = ['gcs_folder', 'destination_table', 'source_file_column']
        required_in_root = ['bucket_name']
        
        # Vérifier les clés dans load_config
        missing_in_load = [k for k in required_in_load if k not in load_config]
        if missing_in_load:
            logger.error(f"Clés manquantes dans load_config: {', '.join(missing_in_load)}")
            return 0
            
        # Vérifier les clés dans config
        missing_in_root = [k for k in required_in_root if k not in config]
        if missing_in_root:
            logger.error(f"Clés manquantes dans config: {', '.join(missing_in_root)}")
            return 0

        # Récupérer la liste des fichiers déjà chargés
        try:
            existing_files = get_existing_files(bq_client, load_config)
        except Exception as e:
            logger.warning(f"Erreur lors de la récupération des fichiers existants: {str(e)}")
            logger.warning("Cela peut être normal si la table cible n'existe pas encore.")
            existing_files = set()
        
        # Récupérer la liste des fichiers disponibles dans GCS
        gcs_config = {
            'bucket_name': config['bucket_name'],
            'gcs_folder': load_config.get('gcs_folder', ''),
            'file_extension': f".{load_config.get('file_format', 'parquet')}"
        }
        gcs_files = get_gcs_files(storage_client, gcs_config)
        
        # Filtrer les fichiers déjà chargés
        new_files = [f for f in gcs_files if f not in existing_files]
        
        if not new_files:
            logger.info("Aucun nouveau fichier à charger.")
            return 0
            
        logger.info(f"Chargement de {len(new_files)} nouveaux fichiers...")
        
        # Limiter le nombre de workers pour éviter les problèmes de taux
        max_workers = min(max_workers, 2)  # Ne pas dépasser 2 workers
        
        # Fonction pour traiter un fichier avec gestion des erreurs et réessais
        def process_with_retry(file_name: str, max_retries: int = 3) -> bool:
            for attempt in range(max_retries):
                try:
                    success = process_file(bq_client, config, file_name)
                    if success:
                        return True
                    
                    # Attendre avant de réessayer
                    wait_time = (attempt + 1) * 5  # Backoff exponentiel
                    logger.warning(f"Tentative {attempt + 1}/{max_retries} échouée pour {file_name}. "
                                 f"Nouvelle tentative dans {wait_time} secondes...")
                    time.sleep(wait_time)
                    
                except Exception as e:
                    if 'Exceeded rate limits' in str(e) and attempt < max_retries - 1:
                        wait_time = (attempt + 1) * 10  # Attendre plus longtemps pour les limites de taux
                        logger.warning(f"Limite de taux atteinte pour {file_name}. "
                                     f"Nouvelle tentative dans {wait_time} secondes...")
                        time.sleep(wait_time)
                        continue
                    logger.error(f"Erreur lors du chargement du fichier {file_name}: {str(e)}", 
                               exc_info=logger.level <= logging.DEBUG)
                    return False
            return False
        
        # Traiter les fichiers avec un ThreadPoolExecutor limité
        successful = 0
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Créer une liste de tâches
            future_to_file = {
                executor.submit(process_with_retry, file_name): file_name 
                for file_name in new_files
            }
            
            # Suivre la progression
            for future in as_completed(future_to_file):
                file_name = future_to_file[future]
                try:
                    if future.result():
                        successful += 1
                except Exception as e:
                    logger.error(f"Erreur lors du traitement de {file_name}: {str(e)}", 
                               exc_info=logger.level <= logging.DEBUG)
        
        logger.info(f"Chargement terminé : {successful}/{len(new_files)} fichiers chargés avec succès")
        return successful
        
    except Exception as e:
        logger.error(f"Erreur lors du chargement des fichiers : {str(e)}", 
                   exc_info=logger.level <= logging.DEBUG)
        return 0

def ensure_dataset_exists(client, dataset_id, location):
    try:
        dataset_ref = client.dataset(dataset_id)
        try:
            # Essayer de récupérer le dataset
            client.get_dataset(dataset_ref)
            print(f"Le dataset {dataset_id} existe déjà.")
            return
        except Exception as e:
            if "Not found" in str(e) or "404" in str(e) or "403" in str(e):
                # Créer le dataset s'il n'existe pas ou si accès refusé
                dataset = bigquery.Dataset(dataset_ref)
                dataset.location = location
                dataset = client.create_dataset(dataset, exists_ok=True)
                print(f"Dataset {dataset_id} créé avec succès.")
            else:
                print(f"Erreur inattendue: {str(e)}")
                raise
    except Exception as e:
        print(f"Erreur lors de la vérification/création du dataset: {str(e)}")
        raise

def load_pipeline(config_path: str = None) -> None:
    """Pipeline principal de chargement des données."""
    # Initialiser le logging
    log_stream = io.StringIO()
    logger = setup_logging()  # Utilise la fonction de configuration centralisée
    
    try:
        # Charger la configuration
        logger.info("Chargement de la configuration...")
        config = load_config(config_path)
        
        # Initialiser les clients GCP
        logger.info("Initialisation des clients GCP...")
        bq_client, storage_client = initialize_clients(config)
        
        # Charger les nouveaux fichiers
        logger.info("Début du chargement des fichiers...")
        files_loaded = load_new_files(bq_client, storage_client, config)
        
        logger.info(f"Chargement terminé. {files_loaded} fichiers chargés avec succès.")
        return 0
        
    except Exception as e:
        logger.error(f"Erreur inattendue lors de l'exécution du pipeline: {str(e)}", 
                   exc_info=logger.level <= logging.DEBUG)
        return 1
    finally:
        # Télécharger les logs vers GCS
        try:
            if 'storage_client' in locals() and 'config' in locals():
                log_content = log_stream.getvalue()
                if log_content.strip():
                    upload_log_to_gcs(storage_client, config, log_stream)
        except Exception as e:
            logger.error(f"Erreur lors de l'upload des logs: {str(e)}", 
                       exc_info=logger.level <= logging.DEBUG)
        log_stream.close()

def main(config_path: str = None) -> None:
    """Fonction principale."""
    if not config_path:
        config_path = os.path.join(os.path.dirname(__file__), '..', 'config.yaml')
    
    # Exécuter le pipeline
    return load_pipeline(config_path)


if __name__ == '__main__':
    main()