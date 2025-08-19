# BigQuery Taxi Trips Analysis

## Aperçu du Projet
Ce projet implémente un pipeline ELT (Extract, Load, Transform) pour l'analyse des données de courses de taxi à New York. Il utilise Google Cloud Platform (GCP) pour le stockage et le traitement des données, avec Apache Airflow pour l'orchestration et dbt (Data Build Tool) pour la transformation des données.

## Architecture du Système

```
NYC TLC Data (CSV)  ──┐
                      │
Weather Data (CSV)  ──┘
        │
        ▼
  Google Cloud Storage (GCS)
        │
        ▼
  Apache Airflow (Orchestration)
        │
        ▼
  BigQuery (Données brutes)
        │
        ▼
  dbt (Transformation)
        │
        ▼
  BigQuery (Modèles analytiques)
```

## Composants Techniques

### 1. Extraction
- Téléchargement des données de courses de taxi depuis NYC TLC
- Récupération des données météorologiques
- Stockage initial dans Google Cloud Storage (GCS)

### 2. Chargement
- Ingestion des données brutes dans BigQuery
- Création des tables de base pour les trajets et la météo
- Validation de l'intégrité des données

### 3. Transformation (dbt)
- Nettoyage et standardisation des données
- Création de modèles intermédiaires
- Génération de modèles analytiques (marts)
- Tests de qualité des données

### 4. Modèles Principaux
- `stg_yellow_trips` : Données brutes des courses de taxi nettoyées
- `stg_weather_data` : Données météorologiques traitées
- `fct_taxi_trips_with_weather` : Vue unifiée des courses et météo
- `mart_daily_trips_all` : Agrégations quotidiennes des métriques clés

## Démarrage Rapide

### Prérequis
- Compte Google Cloud Platform (GCP)
- Projet GCP avec BigQuery activé
- Python 3.8+
- Docker et Docker Compose

### Installation

1. **Cloner le dépôt**
   ```bash
   git clone https://github.com/votre-utilisateur/bigquery-taxi-trips.git
   cd bigquery-taxi-trips
   ```

2. **Configurer l'environnement**
   - Mettre à jour les variables d'environnement (yaml)

3. **Démarrer les services**
   ```bash
   docker-compose up -d
   ```

4. **Exécuter le pipeline**
   - Accéder à l'interface Airflow sur `http://localhost:8080`
   - Déclencher le DAG `nyc_taxi_elt_pipeline`

## Structure du Projet

```
.
├── dags/                  # DAGs Airflow
├── nyc_taxi_dbt/          # Modèles dbt
│   ├── models/
│   │   ├── staging/      # Modèles de staging
│   │   ├── mart/         # Modèles analytiques
│   │   └── intermediate/ # Modèles intermédiaires
│   └── dbt_project.yml   # Configuration dbt
├── src/                   # Scripts Python
├── config.yaml           # Configuration de l'application
└── docker-compose.yml    # Configuration Docker
```

## Licence

Ce projet est sous licence MIT. Voir le fichier `LICENSE` pour plus de détails.

## Contact

Pour toute question ou suggestion, veuillez ouvrir une issue sur ce dépôt.
