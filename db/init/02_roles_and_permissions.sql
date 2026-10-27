-- ==============================================================================
-- SismoWatch Lakehouse Serving Database Initialization
-- Step 02: Roles and Least Privilege Security
-- ==============================================================================

-- Create ETL / Spark Pipeline Role
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'role_spark_etl') THEN
        CREATE ROLE role_spark_etl WITH LOGIN PASSWORD 'spark_etl_secure_pass_2026';
    END IF;
END
$$;

-- Create API Consumer Read-Only Role
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'role_api_reader') THEN
        CREATE ROLE role_api_reader WITH LOGIN PASSWORD 'api_reader_secure_pass_2026';
    END IF;
END
$$;

-- Grant permissions for ETL Role on Gold & Curated
GRANT USAGE, CREATE ON SCHEMA gold, curated TO role_spark_etl;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA gold, curated TO role_spark_etl;
ALTER DEFAULT PRIVILEGES IN SCHEMA gold GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO role_spark_etl;
ALTER DEFAULT PRIVILEGES IN SCHEMA curated GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO role_spark_etl;

-- Grant read-only permissions for API Platform Role
GRANT USAGE ON SCHEMA gold, curated TO role_api_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA gold, curated TO role_api_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA gold GRANT SELECT ON TABLES TO role_api_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA curated GRANT SELECT ON TABLES TO role_api_reader;
