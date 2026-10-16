-- ============================================================================
--  Initialisation PostgreSQL — exécuté au premier démarrage du conteneur
--  Crée la base `scipulse` utilisée par dbt (séparée de la base `airflow`)
-- ============================================================================

CREATE USER scipulse WITH PASSWORD 'scipulse';
CREATE DATABASE scipulse OWNER scipulse;

\c scipulse

-- Schema de staging pour dbt
CREATE SCHEMA IF NOT EXISTS staging AUTHORIZATION scipulse;
CREATE SCHEMA IF NOT EXISTS marts   AUTHORIZATION scipulse;
