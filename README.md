# NYC Yellow Taxi Data Pipeline

Pipeline mensuel d'ingestion et de transformation des trajets Yellow Taxi de New York. Les fichiers Parquet de la Taxi and Limousine Commission (TLC) sont chargés dans Snowflake, puis préparés en données analytiques avec Airflow et Astro Runtime.

![Architecture du pipeline](docs/architecture.png)

## Architecture

```text
TLC Yellow Taxi Parquet ──> Snowflake RAW ──> STAGING ──> INTERMEDIATE ──> MARTS
TLC Taxi Zone Lookup ─────> Snowflake RAW ──> STAGING ───────────────────> MARTS
                                   Airflow orchestre les chargements, transformations et contrôles
```

- **RAW** conserve les données source et leur traçabilité. Le script local lit les Parquets dans `data/raw/`; le DAG Airflow télécharge le fichier mensuel TLC correspondant à sa date logique.
- **STAGING** expose des vues typées et renommées sur RAW et crée les tables de correspondance TLC.
- **INTERMEDIATE** classe les trajets selon les règles de qualité, puis filtre, déduplique et enrichit les trajets valides.
- **MARTS** contient les dimensions, la table de faits et les tables d'analyse quotidiennes, de qualité et de demande horaire par zone.
- **Airflow** ordonne les tâches et bloque la suite lorsqu'un contrôle échoue. Les requêtes de transformation restent dans des fichiers SQL distincts du code Python.

## Structure du dépôt

| Chemin | Rôle |
|---|---|
| `airflow/dags/taxi_pipeline.py` | DAG de chargement mensuel, transformations et contrôles |
| `airflow/include/sql/` | SQL des couches, tables intermédiaires et contrôles |
| `airflow/requirements.txt` | Providers Snowflake et SQL utilisés par Airflow |
| `airflow/.env.example` | Modèle de connexion Snowflake pour Astro |
| `ingestion/load_raw.py` | Chargement manuel d'un mois à partir d'un Parquet local |
| `pyproject.toml` | Dépendances Python du script local, gérées avec uv |
| `data/raw/` | Fichiers source locaux utilisés par le script d'ingestion; exclus du dépôt |
| `docs/` | Schémas et modèles de fiche source et de réponse |
| `snowflake/sql/00_infrastructure.sql` | Rôle, warehouse, base, schémas, droits et utilisateur de service |
| `snowflake/sql/01_raw.sql` | Format Parquet, stage et tables RAW conformes à `CONTRAT_RAW.md` |
| `snowflake/sql/90_verification_raw.sql` | Requêtes de vérification des tables et données RAW |

## Prérequis

- Un compte Snowflake et les droits nécessaires pour créer les objets et charger les données.
- Python 3.10 ou plus récent et [uv](https://docs.astral.sh/uv/) pour le script local.
- Docker Engine ou Docker Desktop et Astro CLI pour lancer Airflow localement.
- Une clé privée Snowflake au format PEM, non chiffrée pour le script local.
- Des fichiers Parquet dans `data/raw/` pour utiliser le script local. Le DAG Airflow télécharge son fichier depuis TLC et n'utilise pas ce dossier local.

## Installation et lancement

### Préparer Snowflake

Le pipeline utilise la base `NYC_TAXI` et les schémas `RAW`, `STAGING`, `INTERMEDIATE` et `MARTS`. Le stage `TAXI_STAGE`, le format Parquet, les tables RAW et les permissions doivent exister avant de lancer les chargements.

Exécutez `snowflake/sql/00_infrastructure.sql` avec `ACCOUNTADMIN`, puis appliquez à `AIRFLOW_SVC` la clé publique RSA selon le commentaire à la fin du script. Ensuite, exécutez `snowflake/sql/01_raw.sql` avec le rôle `TRANSFORMER`. Les vérifications manuelles sont dans `snowflake/sql/90_verification_raw.sql`.

### Lancer le DAG Airflow avec Astro

Depuis la racine du dépôt, copiez le modèle de connexion :

```bash
cp airflow/.env.example airflow/.env
```

Renseignez dans `airflow/.env` les informations du compte Snowflake, le rôle, le warehouse et l'utilisateur de service. Placez la clé privée à `airflow/include/airflow_rsa_key.p8`. Ne versionnez ni `.env`, ni la clé.

Validez puis démarrez Astro :

```bash
cd airflow
astro dev parse
astro dev start
```

Astro affiche l'adresse de l'interface Airflow. Dans celle-ci, repérez le DAG `nyc_taxi_pipeline`, puis activez-le. Pour lancer un mois, utilisez son premier jour comme date logique :

```bash
astro dev run dags unpause nyc_taxi_pipeline
astro dev run dags trigger -l 2025-01-01T00:00:00+00:00 nyc_taxi_pipeline
astro dev run dags trigger -l 2025-02-01T00:00:00+00:00 nyc_taxi_pipeline
astro dev run dags trigger -l 2025-03-01T00:00:00+00:00 nyc_taxi_pipeline
astro dev run dags list-runs nyc_taxi_pipeline
```

Dans Airflow, ouvrez un run pour voir l'état et les journaux de chaque tâche. En cas de contrôle en échec, les tâches en aval sont bloquées. Pour rejouer un run après une modification du DAG, utilisez **Clear** sur les tâches de cette exécution.

Arrêtez les conteneurs locaux depuis `airflow/` :

```bash
astro dev stop
```

### Utiliser le script local d'ingestion (optionnel)

Cette méthode est indépendante du DAG Airflow. Elle lit `data/raw/yellow_tripdata_YYYY-MM.parquet` et prend ses identifiants Snowflake dans un fichier `.env` à la racine du dépôt. Le chemin de la clé peut être précisé avec `SNOWFLAKE_PRIVATE_KEY_PATH`; par défaut, le script cherche `airflow_rsa_key.p8` à la racine.

Exemple de variables à renseigner dans le `.env` racine :

```dotenv
SNOWFLAKE_ACCOUNT=organisation-compte
SNOWFLAKE_USER=AIRFLOW_SVC
SNOWFLAKE_ROLE=TRANSFORMER
SNOWFLAKE_WAREHOUSE=NYC_TAXI_WH
SNOWFLAKE_DATABASE=NYC_TAXI
SNOWFLAKE_SCHEMA=RAW
SNOWFLAKE_PRIVATE_KEY_PATH=airflow/include/airflow_rsa_key.p8
```

Depuis la racine du dépôt, installez les dépendances puis indiquez le mois au format `YYYY-MM` :

```bash
uv sync
uv run python ingestion/load_raw.py 2025-01
```

Le script envoie le fichier local vers `TAXI_STAGE` avec `PUT`, puis le charge dans `YELLOW_TRIPDATA` avec `COPY INTO`.

## Choix techniques

- Les fichiers de trajets TLC sont en Parquet. Snowflake les reçoit dans un stage avec `PUT`, puis les charge avec `COPY INTO`.
- La date logique Airflow détermine le mois à télécharger et à transformer. `catchup=False` évite de lancer automatiquement toutes les dates historiques.
- Le DAG télécharge le fichier en flux, par blocs, dans un répertoire temporaire : le contenu complet n'est pas gardé en mémoire et le fichier temporaire est supprimé après le chargement.
- Les transformations sont écrites en SQL et orchestrées par des tâches Airflow regroupées par couche.
- Les contrôles valident la présence du mois en RAW, le taux de trajets rejetés et l'absence de doublons. Ils n'ont pas de nouvelle tentative automatique (`retries=0`).
- Les scripts mensuels suppriment puis réinsèrent le mois concerné. `FORCE=TRUE` autorise le rejeu explicite d'un fichier déjà chargé par Snowflake.

Les seuils de distance, durée et taux de rejet, ainsi que les bornes du calendrier, sont configurés dans les paramètres du DAG. Un seuil de rejet doit être choisi en fonction des règles métier et des données observées, pas simplement augmenté pour faire passer un contrôle.

## Vérifications et résultats de référence

Le contrôle `raw_mois_charge.sql` vérifie la présence des lignes du mois en RAW. `taux_trajets_rejetes.sql` compare le taux de rejet à `max_rejection_pct`. `trajets_en_double.sql` s'assure que les `trip_sk` sont uniques dans la table enrichie pour le mois traité.

Les nombres de référence indiqués par le brief après chargement de janvier à mars 2025 sont :

| Table | Nombre de lignes attendu |
|---|---:|
| `NYC_TAXI.RAW.YELLOW_TRIPDATA` | 11 198 026 |
| `NYC_TAXI.RAW.TAXI_ZONE_LOOKUP` | 265 |
| `NYC_TAXI.INTERMEDIATE.INT_TRIPS__FLAGGED` | 11 198 026 |
| `NYC_TAXI.MARTS.FCT_TRIPS` | 10 382 378 |
| `NYC_TAXI.MARTS.MART_ZONE_HOURLY_DEMAND` | 11 524 |
| `NYC_TAXI.MARTS.MART_DATA_QUALITY` | 18 |

Ce sont des résultats attendus, pas une mesure de votre compte. Vérifiez les journaux Airflow et les tables Snowflake après l'exécution.

## Scripts pour répondre aux questions

### 1. Où et quand y a-t-il le plus de départs ?
```
WITH demand AS (
    SELECT
        DATE_TRUNC('MONTH', t.pickup_date) AS month,
        z.borough AS pickup_borough,
        z.zone_name AS pickup_zone,
        t.pickup_hour,
        COUNT(*) AS trip_count
    FROM NYC_TAXI.MARTS.FCT_TRIPS AS t
    LEFT JOIN NYC_TAXI.MARTS.DIM_ZONE AS z
        ON t.pickup_zone_key = z.zone_key
    GROUP BY 1, 2, 3, 4
),
ranked_demand AS (
    SELECT
        *,
        RANK() OVER (
            PARTITION BY month
            ORDER BY trip_count DESC
        ) AS rank_in_month
    FROM demand
)
SELECT *
FROM ranked_demand
WHERE rank_in_month <= 10
ORDER BY month, rank_in_month;
```

### 2 . Quel montant est associé à un trajet selon zone, heure et paiement ?

```
SELECT
    z.borough AS pickup_borough,
    z.zone_name AS pickup_zone,
    t.pickup_hour,
    p.payment_type_label,
    COUNT(*) AS trip_count,
    ROUND(AVG(t.total_amount), 2) AS avg_total_per_trip,
    ROUND(SUM(t.total_amount), 2) AS total_revenue
FROM NYC_TAXI.MARTS.FCT_TRIPS AS t
LEFT JOIN NYC_TAXI.MARTS.DIM_ZONE AS z
    ON t.pickup_zone_key = z.zone_key
LEFT JOIN NYC_TAXI.MARTS.DIM_PAYMENT_TYPE AS p
    ON t.payment_type_key = p.payment_type_key
GROUP BY ALL
ORDER BY avg_total_per_trip DESC;

```

### 3. Verifier que les trajets anormaux issus de INT_TRIPS_FLAGGED correspondent à ceux de MART_DATA_QUALITY

``` 
WITH flagged_counts AS (
    SELECT
        source_file,
        source_file_month,
        rejection_reason AS status,
        COUNT(*) AS flagged_count
    FROM NYC_TAXI.INTERMEDIATE.INT_TRIPS__FLAGGED
    WHERE rejection_reason IS NOT NULL
    GROUP BY source_file, source_file_month, rejection_reason
),
mart_counts AS (
    SELECT
        source_file,
        source_file_month,
        status,
        nb_rows AS mart_count
    FROM NYC_TAXI.MARTS.MART_DATA_QUALITY
    WHERE status <> 'valid'
)
SELECT
    COALESCE(f.source_file, m.source_file) AS source_file,
    COALESCE(f.source_file_month, m.source_file_month) AS source_file_month,
    COALESCE(f.status, m.status) AS rejection_reason,
    COALESCE(f.flagged_count, 0) AS flagged_count,
    COALESCE(m.mart_count, 0) AS mart_count,
    COALESCE(f.flagged_count, 0) - COALESCE(m.mart_count, 0) AS difference
FROM flagged_counts AS f
FULL OUTER JOIN mart_counts AS m
    ON f.source_file = m.source_file
   AND f.source_file_month = m.source_file_month
   AND f.status = m.status
ORDER BY source_file_month, rejection_reason;

```
Résultats de la requete de controle de qualité
![alt text](image.png)