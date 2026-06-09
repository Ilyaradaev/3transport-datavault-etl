-- Инициализация базы данных dwh_etl_db
CREATE SCHEMA IF NOT EXISTS datavault;
CREATE SCHEMA IF NOT EXISTS datamart;

-- Создание пользователя для Superset (если нужно)
CREATE USER IF NOT EXISTS superset WITH PASSWORD 'superset';
GRANT CONNECT ON DATABASE dwh_etl_db TO superset;
GRANT USAGE ON SCHEMA datamart TO superset;
GRANT SELECT ON ALL TABLES IN SCHEMA datamart TO superset;
