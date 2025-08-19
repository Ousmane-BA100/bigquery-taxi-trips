import os
import sys
import importlib.util

# Add the src directory to Python path once
sys.path.insert(0, '/opt/airflow/src')

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from google.cloud import storage
from airflow.models import Variable
from airflow.hooks.base import BaseHook


# Import modules from src
from extract import main as extract_main
from load import main as load_main
from extract_weather import main as extract_weather_main
from load_weather import main as load_weather_main  
from load_zones import load_taxi_zones

# Fonction pour télécharger un fichier depuis GCS si nécessaire
def download_file_if_needed(bucket_name: str, object_name: str, destination_file_name: str) -> None:
    """Télécharge un fichier depuis GCS uniquement s'il n'existe pas déjà ou s'il a été modifié."""
    try:
        # S'assurer que le répertoire parent existe
        os.makedirs(os.path.dirname(destination_file_name), exist_ok=True)
        
        # Si la destination est un répertoire, le supprimer
        if os.path.isdir(destination_file_name):
            import shutil
            shutil.rmtree(destination_file_name)
            
        # Vérifier si le fichier existe déjà
        if os.path.exists(destination_file_name):
            file_mtime = os.path.getmtime(destination_file_name)
            
            # Récupérer l'objet depuis GCS pour comparer
            storage_client = storage.Client()
            bucket = storage_client.bucket(bucket_name)
            blob = bucket.blob(object_name)
            
            # Si le blob existe et est plus récent, télécharger
            if blob.exists() and blob.updated and blob.updated.timestamp() > file_mtime:
                print(f"Le fichier GCS est plus récent, téléchargement...")
                blob.download_to_filename(destination_file_name)
                print(f"Fichier mis à jour depuis gs://{bucket_name}/{object_name} vers {destination_file_name}")
            else:
                print(f"Le fichier {destination_file_name} existe déjà et est à jour")
        else:
            # Le fichier n'existe pas, le télécharger
            print(f"Le fichier {destination_file_name} n'existe pas, téléchargement...")
            storage_client = storage.Client()
            bucket = storage_client.bucket(bucket_name)
            blob = bucket.blob(object_name)
            
            # Créer le répertoire cible s'il n'existe pas
            os.makedirs(os.path.dirname(destination_file_name), exist_ok=True)
            
            blob.download_to_filename(destination_file_name)
            print(f"Fichier téléchargé depuis gs://{bucket_name}/{object_name} vers {destination_file_name}")
            
    except Exception as e:
        print(f"Erreur lors du téléchargement du fichier: {str(e)}")
        raise

# Définition des arguments par défaut pour le DAG
default_args = {
    'owner': 'fv_data_engineer',  
    'depends_on_past': False,
    'start_date': days_ago(0),
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
    'email_on_failure': False,
    'email_on_retry': False,
}

# Définition du DAG avec la structure "with"
with DAG(
    dag_id="nyc_taxi_elt_pipeline",
    default_args=default_args,
    description="Pipeline ELT pour NYC Taxi et données météo",
    schedule_interval='0 0 28 * *',  # Exécution le 28ème jour de chaque mois à 00:00
    catchup=False,
    tags=["nyc_taxi", "elt"]
) as dag:

    # Tâche pour initialiser les répertoires et configurations
    def setup_env(**context):
        try:
            print('Starting setup_env function')
            dbt_dir = '/home/airflow/.dbt'
            cred_dir = '/tmp/dbt_credentials'
            
            print(f'Creating directory: {dbt_dir}')
            os.makedirs(dbt_dir, mode=0o777, exist_ok=True)
            print(f'Created directory: {dbt_dir}')
            
            print(f'Creating directory: {cred_dir}')
            os.makedirs(cred_dir, mode=0o777, exist_ok=True)
            print(f'Created directory: {cred_dir}')
            
            # Essayer de définir les permissions, mais continuer même en cas d'échec
            try:
                os.chmod(dbt_dir, 0o777)
                os.chmod(cred_dir, 0o777)
                print('Permissions set successfully')
            except PermissionError as pe:
                print(f'Warning: Could not set permissions: {pe}')
                print('Continuing without setting permissions...')
            
            # Vérifier que les répertoires existent
            if os.path.exists(dbt_dir) and os.path.exists(cred_dir):
                print('All directories exist')
                return 'Setup completed successfully'
            else:
                raise Exception('Directories were not created properly')
        except Exception as e:
            print(f'Error in setup_env: {str(e)}')
            raise

    setup_environment = PythonOperator(
        task_id='setup_environment',
        python_callable=setup_env
    )

    # Tâche pour télécharger profiles.yml depuis GCS uniquement si nécessaire
    download_configs = PythonOperator(
        task_id='download_configs',
        python_callable=lambda **kwargs: (
            download_file_if_needed(
                'nyc-taxi-bucket-ynov_3', 
                'dbt/profiles.yml', 
                '/home/airflow/.dbt/profiles.yml'
            ),
            download_file_if_needed(
                'nyc-taxi-bucket-ynov_3', 
                'dbt/credentials.json', 
                '/tmp/dbt_credentials/credentials.json'
            )
        ),
    )

    # Tâche pour mettre à jour profiles.yml si nécessaire
    update_profiles = BashOperator(
        task_id='update_profiles',
        bash_command=(
            'if grep -q "/tmp/dbt_credentials/credentials.json" /home/airflow/.dbt/profiles.yml; then '
            '  sed -i "s|/tmp/dbt_credentials/credentials.json|/tmp/dbt_credentials/credentials.json|g" /home/airflow/.dbt/profiles.yml && '
            '  echo "Profiles.yml mis à jour pour utiliser le nouveau chemin de credentials"; '
            'else '
            '  echo "Profiles.yml utilise déjà le bon chemin de credentials"; '
            'fi'
        )
    )

    extract_taxi = PythonOperator(
        task_id='extract_taxi_data',
        python_callable=extract_main,
        op_kwargs={'config_path': '/opt/airflow/dags/config.yaml'},
    )

    extract_weather = PythonOperator(
        task_id='extract_weather_data',
        python_callable=extract_weather_main,
        op_kwargs={'config_path': '/opt/airflow/dags/config.yaml'},
    )

    load_taxi = PythonOperator(
        task_id='load_taxi_data',
        python_callable=load_main,
        op_kwargs={'config_path': '/opt/airflow/dags/config.yaml'},
    )

    load_weather = PythonOperator(
        task_id='load_weather_data',
        python_callable=load_weather_main,
        op_kwargs={'config_path': '/opt/airflow/dags/config.yaml'},
    )

    load_zones_task = PythonOperator(
        task_id='load_taxi_zones',
        python_callable=load_taxi_zones,
        op_kwargs={'config_path': '/opt/airflow/dags/config.yaml'},
    )
    
    # Tâche d'exécution des transformations dbt
    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command=(
            'cd /opt/airflow/nyc_taxi_dbt && '
            'dbt run --profiles-dir /opt/airflow/.dbt'
        )
    )



    # Définition des dépendances (simplifiées)
    setup_environment >> download_configs >> update_profiles

    update_profiles >> extract_taxi >> load_taxi
    update_profiles >> extract_weather >> load_weather
    update_profiles >> load_zones_task
    
    [load_taxi, load_weather] >> dbt_run