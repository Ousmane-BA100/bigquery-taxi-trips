import os
import logging
import argparse
from google.cloud import bigquery, storage
from datetime import datetime, UTC
import io
from concurrent.futures import ThreadPoolExecutor
import yaml
from typing import Dict, Set, List, Tuple, Any, Optional

def setup_logging():
    """Initialise le logging avec les handlers par défaut d'Airflow."""
    # Formatter commun
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    
    # Récupérer le logger root
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    
    # Nettoyage des anciens handlers pour éviter les doublons
    if logger.hasHandlers():
        logger.handlers.clear()
    
    # Retourner simplement le logger sans ajouter de nouveaux handlers
    return logger

def load_config(config_path: str, section: Optional[str] = None) -> Dict[str, Any]:
    """Charge la configuration depuis un fichier YAML, éventuellement une section spécifique."""
    try:
        with open(config_path, 'r') as file:
            full_config = yaml.safe_load(file)
            print(f"Configuration complète chargée: {list(full_config.keys())}")
        
        # Si une section spécifique est demandée
        if section and section in full_config:
            print(f"Extraction de la section '{section}'")
            
            # Extraire les paramètres globaux (qui ne sont pas des sections)
            global_params = {k: v for k, v in full_config.items() 
                           if not isinstance(v, dict)}
            print(f"Paramètres globaux: {global_params}")
            
            # Créer une nouvelle configuration en combinant les deux
            combined_config = global_params.copy()
            combined_config.update(full_config[section])
            print(f"Configuration combinée avant résolution: {combined_config}")
            
            # Résoudre les références
            resolved_config = resolve_config_references(combined_config)
            print(f"Configuration après résolution: {resolved_config}")
            return resolved_config
        
        # Si aucune section n'est spécifiée, résoudre les références dans la config complète
        print("Résolution des références dans la configuration complète")
        return resolve_config_references(full_config)
    
    except Exception as e:
        print(f"Erreur lors du chargement de la configuration: {str(e)}")
        import traceback
        print(f"Traceback: {traceback.format_exc()}")
        raise

def resolve_config_references(config: Dict[str, Any]) -> Dict[str, Any]:
    """Résout les références dans la configuration, comme ${project_id}."""
    # Extraire d'abord toutes les valeurs de premier niveau
    global_values = {k: v for k, v in config.items() if isinstance(v, str)}
    
    # Convertir en YAML pour faciliter les remplacements de chaînes
    config_str = yaml.dump(config)
    
    # Remplacer les références par leurs valeurs
    for key, value in global_values.items():
        placeholder = f"${{{key}}}"
        if placeholder in config_str:
            print(f"Remplacement de '{placeholder}' par '{value}'")
            config_str = config_str.replace(placeholder, value)
    
    # Reconvertir en dictionnaire
    resolved_config = yaml.safe_load(config_str)
    
    return resolved_config
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

def load_pipeline(config_path: str = None):
    try:
        print(f"=== DÉBUT DE LOAD_PIPELINE ===")
        print(f"Paramètre config_path reçu: {config_path}")

        if not config_path:
            config_path = '/home/airflow/gcs/dags/config.yaml'
            print(f"Aucun chemin spécifié, utilisation du chemin par défaut: {config_path}")

        # ÉTAPE 1: Vérifier l'existence du fichier
        print(f"ÉTAPE 1: Vérification du fichier de configuration")
        if not os.path.exists(config_path):
            print(f"ERREUR: Le fichier de configuration {config_path} n'existe pas!")
            return
        print(f"OK - Le fichier de configuration existe: {config_path}")
        
        # ÉTAPE 2: Lecture du fichier et log des résultats
        print(f"ÉTAPE 2: Lecture du fichier de configuration")
        try:
            with open(config_path, 'r') as f:
                content = f.read()
                print(f"OK - Fichier lu, {len(content)} caractères")
        except Exception as e:
            print(f"ERREUR lors de la lecture: {str(e)}")
            return
            
        # ÉTAPE 3: Parsing YAML
        print(f"ÉTAPE 3: Parsing YAML")
        try:
            import yaml
            full_config = yaml.safe_load(content)
            print(f"OK - YAML parsé, sections: {list(full_config.keys())}")
        except Exception as e:
            print(f"ERREUR lors du parsing YAML: {str(e)}")
            return
            
        # ÉTAPE 4: Initialisation des logs
        print(f"ÉTAPE 4: Initialisation des logs")
        try:
            log_stream, logger = setup_logging()
            print(f"OK - Logs initialisés")
        except Exception as e:
            print(f"ERREUR lors de l'initialisation des logs: {str(e)}")
            return
            
        # ÉTAPE 5: Chargement de la configuration
        print(f"ÉTAPE 5: Chargement de la configuration section 'load'")
        try:
            config = load_config(config_path, 'load')
            print(f"OK - Configuration chargée")
        except Exception as e:
            print(f"ERREUR lors du chargement de la configuration: {str(e)}")
            import traceback
            print(f"Traceback: {traceback.format_exc()}")
            return
            
        # ÉTAPE 6: Initialisation des clients
        print(f"ÉTAPE 6: Initialisation des clients GCP")
        try:
            bq_client, storage_client = initialize_clients(config)
            print(f"OK - Clients initialisés")
        except Exception as e:
            print(f"ERREUR lors de l'initialisation des clients: {str(e)}")
            import traceback
            print(f"Traceback: {traceback.format_exc()}")
            return
        
        # Si vous arrivez ici, c'est que tout fonctionne jusqu'à l'étape 6
        print(f"Configuration et initialisation réussies!")
        return
            
    except Exception as e:
        print(f"Exception globale: {str(e)}")
        import traceback
        print(f"Traceback: {traceback.format_exc()}")

def main(config_path: str = None):
    try:
        if config_path is None and 'AIRFLOW_CTX_DAG_ID' not in os.environ:
            # Execution en local
            parser = argparse.ArgumentParser(description='Charger des fichiers depuis GCS vers BigQuery')
            parser.add_argument('--config', required=True, help='Chemin vers le fichier de configuration YAML')
            args = parser.parse_args()
            config_path = args.config

        return load_pipeline(config_path)

    except Exception as e:
        logging.error(f"Erreur dans la fonction main: {str(e)}")
        raise

if __name__ == '__main__':
    main()