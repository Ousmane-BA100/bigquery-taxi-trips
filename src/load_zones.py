import pandas as pd
from google.cloud import bigquery
import os
import yaml

def load_taxi_zones(config_path: str = None):
    """Charge les données des zones de taxi dans BigQuery."""
    # Charger la configuration
    if not config_path:
        config_path = '/opt/airflow/dags/config.yaml'
    
    try:
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        
        # URL du fichier des zones
        zones_url = "https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv"
        
        # Charger les données
        print("Téléchargement des données des zones...")
        df = pd.read_csv(zones_url)
        
        # Initialiser le client BigQuery
        client = bigquery.Client(project=config['project_id'])
        
        # Spécifier le dataset et la table de destination
        dataset_id = 'raw_yellow_taxi_trips'
        table_id = 'taxi_zone'
        dataset_ref = client.dataset(dataset_id)
        
        # Charger les données dans BigQuery
        table_ref = dataset_ref.table(table_id)
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",  # Écrase la table si elle existe
            autodetect=True
        )
        
        print(f"Chargement des données dans {dataset_id}.{table_id}...")
        job = client.load_table_from_dataframe(
            df, table_ref, job_config=job_config
        )
        job.result()  # Attendre la fin du chargement
        
        print(f"Données chargées avec succès dans {dataset_id}.{table_id}")
        return True
        
    except Exception as e:
        print(f"Erreur lors du chargement des zones de taxi: {str(e)}")
        raise