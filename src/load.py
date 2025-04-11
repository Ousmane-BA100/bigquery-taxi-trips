import logging
import argparse
from google.cloud import bigquery, storage
from datetime import datetime, UTC
import io
from concurrent.futures import ThreadPoolExecutor
import yaml
from typing import Dict, Set, List, Tuple, Any, Optional

def setup_logging() -> io.StringIO:
    """Configure et retourne un flux de journalisation avec affichage terminal."""
    log_stream = io.StringIO()
    
    # Créer un gestionnaire pour la sortie console
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_format = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    console_handler.setFormatter(console_format)
    
    # Créer un gestionnaire pour le flux StringIO
    stream_handler = logging.StreamHandler(log_stream)
    stream_handler.setLevel(logging.INFO)
    stream_format = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    stream_handler.setFormatter(stream_format)
    
    # Configurer le logger root avec les deux gestionnaires
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    root_logger.handlers = []  # Réinitialiser les gestionnaires existants
    root_logger.addHandler(console_handler)
    root_logger.addHandler(stream_handler)
    
    return log_stream

def load_config(config_path: str, section: Optional[str] = None) -> Dict[str, Any]:
    """Charge la configuration depuis un fichier YAML, éventuellement une section spécifique."""
    with open(config_path, 'r') as file:
        config = yaml.safe_load(file)
    
    # Résoudre les références dans la configuration
    resolved_config = resolve_config_references(config)
    
    # Si une section spécifique est demandée, retourner uniquement cette section
    if section and section in resolved_config:
        # Fusionner les paramètres globaux avec les paramètres spécifiques à la section
        # en excluant les autres sections
        section_config = {k: v for k, v in resolved_config.items() 
                        if k != 'extract' and k != 'load'}
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

def initialize_clients(config: Dict[str, Any]) -> Tuple[bigquery.Client, storage.Client]:
    """Initialise et retourne les clients BigQuery et Storage."""
    project_id = config['project_id']
    bq_client = bigquery.Client(project=project_id, location=config['location'])
    storage_client = storage.Client(project=project_id)
    return bq_client, storage_client

def upload_log_to_gcs(storage_client: storage.Client, config: Dict[str, Any], log_stream: io.StringIO) -> None:
    """Télécharge le fichier journal vers GCS."""
    log_filename = f"{config['log_folder']}load_log_{datetime.now(UTC).strftime('%Y%m%d_%H%M%S')}.log"
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
    """Traite un fichier individuel, le charge dans une table temporaire puis dans la table finale."""
    try:
        uri = f"gs://{config['bucket_name']}/{config['gcs_folder']}{file_name}"
        temp_table_id = f"{config['destination_table']}_temp_{file_name.replace('.', '_')}"
        logging.info(f"Chargement du fichier: {uri} vers {temp_table_id}")
        
        # 1) Charger le fichier dans une table temporaire
        temp_job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            autodetect=True
        )
        
        load_job = bq_client.load_table_from_uri(uri, temp_table_id, job_config=temp_job_config)
        load_job.result()
        logging.info(f"Fichier chargé dans la table temporaire: {temp_table_id}")
        
        # 2) Transformation et insertion finale
        query = generate_transformation_query(config, temp_table_id, file_name)
        query_job = bq_client.query(query)
        query_job.result()
        logging.info(f"Données de {temp_table_id} insérées dans {config['destination_table']}")
        
        # 3) Supprimer la table temporaire
        bq_client.delete_table(temp_table_id, not_found_ok=True)
        logging.info(f"Table temporaire supprimée: {temp_table_id}")
        
        return True
    except Exception as e:
        logging.error(f"Erreur lors du traitement du fichier {file_name}: {str(e)}")
        return False

def load_new_files(bq_client: bigquery.Client, storage_client: storage.Client, 
                  config: Dict[str, Any], max_workers: int = 4) -> int:
    """Charge les nouveaux fichiers depuis GCS vers BigQuery avec parallélisation.
    Retourne le nombre de fichiers traités avec succès."""
    new_files = list(get_gcs_files(storage_client, config) - get_existing_files(bq_client, config))
    
    if not new_files:
        logging.info("Aucun nouveau fichier à charger.")
        return 0
    
    logging.info(f"Fichiers à charger: {len(new_files)}")
    
    # Vérifier si la table de destination existe, sinon la créer
    try:
        bq_client.get_table(config['destination_table'])
        logging.info(f"Table destination {config['destination_table']} existe déjà.")
    except Exception:
        logging.info(f"Table destination {config['destination_table']} n'existe pas. " 
                   f"Elle sera créée lors du premier insert.")
    
    # Utiliser ThreadPoolExecutor pour le traitement parallèle
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        process_func = lambda file: process_file(bq_client, config, file)
        results = list(executor.map(process_func, new_files))
    
    successful_files = sum(results)
    
    # Vérification finale
    try:
        destination_table = bq_client.get_table(config['destination_table'])
        logging.info(f"Chargé {successful_files}/{len(new_files)} fichiers. "
                    f"La table {config['destination_table']} contient {destination_table.num_rows} lignes.")
    except Exception as e:
        logging.error(f"Erreur lors de la vérification finale: {str(e)}")
    
    return successful_files

def main():
    """Fonction principale du script."""
    parser = argparse.ArgumentParser(description='Charger des fichiers Parquet depuis GCS vers BigQuery')
    parser.add_argument('--config', required=True, help='Chemin vers le fichier de configuration YAML')
    parser.add_argument('--workers', type=int, default=4, help='Nombre de workers pour le chargement parallèle')
    args = parser.parse_args()
    
    # Configuration et initialisation
    log_stream = setup_logging()
    try:
        # Charger la configuration avec la section 'load'
        config = load_config(args.config, 'load')
        
        # Vérifier les paramètres requis
        required_params = ['project_id', 'location', 'bucket_name', 'gcs_folder', 
                          'destination_table', 'source_file_column', 'file_extension']
        missing_params = [param for param in required_params if param not in config]
        
        if missing_params:
            raise ValueError(f"Paramètres manquants dans la configuration: {', '.join(missing_params)}")
        
        # Initialiser les clients
        bq_client, storage_client = initialize_clients(config)
        
        # Définir le nombre de workers
        max_workers = config.get('max_workers', args.workers)
        
        # Exécution du chargement
        load_new_files(bq_client, storage_client, config, max_workers=max_workers)
        
    except Exception as e:
        logging.error(f"Erreur globale: {str(e)}")
    finally:
        # S'assurer que les logs sont téléchargés même en cas d'erreur
        try:
            upload_log_to_gcs(storage_client, config, log_stream)
        except Exception as e:
            print(f"Erreur lors du téléchargement des logs: {str(e)}")
            print(log_stream.getvalue())  # Afficher les logs en cas d'échec du téléchargement

if __name__ == "__main__":
    main()