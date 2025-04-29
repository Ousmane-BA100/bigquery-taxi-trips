🚀 **Pipeline d'analyse météo & Taxi NYC** 📊

📝 **Présentation du Projet**
Ce projet vise à analyser l’impact des conditions météorologiques sur plus de 100 000 000 de courses de taxi à New York. En exploitant un pipeline ELT sur GCP et une couche de visualisation Power BI, nous transformons des données brutes de trajets et de météos en insights actionnables pour optimiser la répartition des taxis et anticiper les pics de demande.

## Démo du Tableau de Bord

![Démonstration du tableau de bord](assets/screencast_dashboard.gif)

🔍 **Caractéristiques du Jeu de Données**
- **Sources** : NYC Taxi & Limousine Commission (trips parquet), Automated Observation System (météo horaires)
- **Période** : données historiques mensuelles couvrant plusieurs années (2022-2024)
- **Volume** : plus de 100 millions de courses et observations horaire météo pour plusieurs dizaines de stations

🛠 **Architecture Technique**
- **Google Cloud Storage (GCS)** : stockage versionné des fichiers CSV bruts
- **BigQuery** : tables **raw**, **staging**, **intermediate**, **mart**
- **dbt** : transformations modulaires (staging → intermediate → marts), tests (not_null, accepted_values, relationships) et modèles incrémentaux
- **Airflow**  : orchestration des extractions mensuelles
- **Power BI** : visualisation interactive des marts agrégés (date, zone, condition météo)

🧰 **Compétences Techniques Démontrées**
- **SQL & BigQuery** : optimisation de requêtes et partitionnement
- **dbt** : modélisation, tests et macros réutilisables
- **GCP** : configuration de buckets GCS et dataset BigQuery
- **Airflow/Python** : automatisation des workflows d’ingestion
- **Power BI & DAX** : modélisation de données et tableaux de bord réactifs

🚦 **Exécution & Orchestration**
Tous les workflows sont déclenchés de manière planifiée via Airflow , garantissant la fraîcheur des données à J+1. Les transformations dbt sont configurées en mode incrémental pour ne traiter que la donnée nouvelle.



