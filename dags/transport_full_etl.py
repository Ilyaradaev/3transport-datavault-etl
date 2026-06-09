from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import numpy as np
import logging
import json
import os

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'etl_user3',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'transport_etl',
    default_args=default_args,
    description='Полный ETL для данных о ДТП',
    schedule_interval='@daily',
    catchup=False,
    tags=['transport', 'etl', 'full']
)

def load_config(**context):
    config_path = '/opt/airflow/dags/config/Modeldescription.json'
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config = json.load(f)
        logger.info(f"✅ Конфигурация загружена: {len(config['datasets'])} источников")
        context['ti'].xcom_push(key='config', value=config)
    except Exception as e:
        logger.error(f"❌ Ошибка загрузки конфигурации: {e}")
        raise

def extract_and_stage(**context):
    config = context['ti'].xcom_pull(key='config', task_ids='load_config')
    hook = PostgresHook(postgres_conn_id='postgres_dwh')
    engine = hook.get_sqlalchemy_engine()
    
    for dataset in config['datasets']:
        file_path = os.path.join(config['source_path'], dataset['filename'])
        table_name = f"raw_{dataset['name']}"
        
        if not os.path.exists(file_path):
            logger.error(f"❌ Файл не найден: {file_path}")
            raise FileNotFoundError(f"Файл не найден: {file_path}")
        
        logger.info(f"📖 Обработка: {dataset['name']} ({file_path})")
        
        # Чтение CSV по частям для больших файлов
        chunk_size = 50000
        chunks = []
        for chunk in pd.read_csv(file_path, encoding='utf-8', low_memory=False, chunksize=chunk_size):
            chunks.append(chunk)
        
        df = pd.concat(chunks, ignore_index=True)
        logger.info(f"   Загружено {len(df)} записей")
        
        # Удаление дубликатов
        before = len(df)
        df = df.drop_duplicates()
        logger.info(f"   Удалено дубликатов: {before - len(df)}")
        
        # Замена пропусков на None
        df = df.replace({np.nan: None, '': None, 'NaN': None})
        
        # Загрузка в staging
        df.to_sql(table_name, engine, if_exists='replace', index=False, chunksize=10000)
        logger.info(f"✅ Загружено в {table_name}: {len(df)} записей")

def create_datavault(**context):
    """Создание Data Vault таблиц"""
    hook = PostgresHook(postgres_conn_id='postgres_dwh')
    
    sql = """
    CREATE SCHEMA IF NOT EXISTS datavault;
    CREATE SCHEMA IF NOT EXISTS datamart;
    
    -- HUB таблицы
    CREATE TABLE IF NOT EXISTS datavault.hub_accident (
        hub_accident_hashkey VARCHAR(32) PRIMARY KEY,
        accident_id INTEGER NOT NULL UNIQUE,
        load_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    
    -- SAT таблицы
    CREATE TABLE IF NOT EXISTS datavault.sat_accident_details (
        sat_hashkey VARCHAR(32) PRIMARY KEY,
        hub_accident_hashkey VARCHAR(32),
        tags TEXT,
        category VARCHAR(100),
        region VARCHAR(100),
        county VARCHAR(100),
        address TEXT,
        longitude FLOAT,
        latitude FLOAT,
        nearby TEXT,
        accident_datetime TIMESTAMP,
        light VARCHAR(100),
        weather VARCHAR(200),
        road_conditions VARCHAR(200),
        participants_count INTEGER,
        participant_categories TEXT,
        severity VARCHAR(50),
        dead_count INTEGER,
        injured_count INTEGER,
        load_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    
    -- Индексы
    CREATE INDEX IF NOT EXISTS idx_sat_accident_hub ON datavault.sat_accident_details(hub_accident_hashkey);
    CREATE INDEX IF NOT EXISTS idx_sat_region ON datavault.sat_accident_details(region);
    """
    hook.run(sql)
    logger.info("✅ Data Vault схема создана")

def load_hubs(**context):
    """Загрузка HUB таблиц"""
    hook = PostgresHook(postgres_conn_id='postgres_dwh')
    
    hook.run("""
        INSERT INTO datavault.hub_accident (hub_accident_hashkey, accident_id)
        SELECT MD5(id::VARCHAR), id FROM raw_accidents
        ON CONFLICT (accident_id) DO NOTHING;
    """)
    logger.info("✅ HUB таблицы загружены")

def load_sats(**context):
    """Загрузка SAT таблиц"""
    hook = PostgresHook(postgres_conn_id='postgres_dwh')
    
    hook.run("""
        INSERT INTO datavault.sat_accident_details (
            sat_hashkey, hub_accident_hashkey, tags, category, region, county, address,
            longitude, latitude, nearby, accident_datetime, light, weather,
            road_conditions, participants_count, participant_categories,
            severity, dead_count, injured_count
        )
        SELECT 
            MD5(a.id::VARCHAR || COALESCE(a.datetime::VARCHAR, '')),
            MD5(a.id::VARCHAR),
            a.tags, a.category, a.region, a.county, a.address,
            a.longitude, a.latitude, a.nearby,
            CASE 
                WHEN a.datetime ~ '^\\d{4}-\\d{2}-\\d{2}' THEN TO_TIMESTAMP(a.datetime, 'YYYY-MM-DD HH24:MI:SS')
                ELSE NULL
            END,
            a.light, a.weather, a.road_conditions, a.participants_count,
            a.participant_categories, a.severity, a.dead_count, a.injured_count
        FROM raw_accidents a
        ON CONFLICT (sat_hashkey) DO NOTHING;
    """)
    logger.info("✅ SAT таблицы загружены")

def create_marts(**context):
    """Создание витрин данных"""
    hook = PostgresHook(postgres_conn_id='postgres_dwh')
    
    sql = """
    -- Витрина 1: Пострадавшие по регионам и времени суток
    DROP TABLE IF EXISTS datamart.victims_by_region_light;
    CREATE TABLE datamart.victims_by_region_light AS
    SELECT 
        COALESCE(region, 'Неизвестно') as region_name,
        CASE 
            WHEN light ILIKE '%темное%' OR light ILIKE '%ночь%' OR light ILIKE '%искусственное%' 
            THEN 'Ночное время'
            ELSE 'Дневное время'
        END as light_period,
        SUM(COALESCE(dead_count, 0) + COALESCE(injured_count, 0)) as total_victims,
        COUNT(DISTINCT accident_id) as accidents_count,
        ROUND(AVG(COALESCE(dead_count, 0) + COALESCE(injured_count, 0)), 2) as avg_victims_per_accident
    FROM datavault.sat_accident_details s
    JOIN datavault.hub_accident h ON s.hub_accident_hashkey = h.hub_accident_hashkey
    GROUP BY region, light_period
    ORDER BY total_victims DESC;
    
    -- Витрина 2: Поврежденные автомобили по маркам и регионам
    DROP TABLE IF EXISTS datamart.damaged_vehicles;
    CREATE TABLE datamart.damaged_vehicles AS
    SELECT 
        COALESCE(v.brand, 'Неизвестно') as brand,
        s.region,
        COUNT(DISTINCT v.vehicle_id) as damaged_vehicles_count,
        COUNT(DISTINCT v.accident_id) as accidents_count,
        SUM(COALESCE(s.dead_count, 0) + COALESCE(s.injured_count, 0)) as total_victims
    FROM raw_vehicles v
    JOIN raw_accidents a ON v.accident_id = a.id
    JOIN datavault.sat_accident_details s ON s.region = a.region
    GROUP BY v.brand, s.region
    ORDER BY damaged_vehicles_count DESC;
    
    -- Витрина 3: ДТП по месяцам и регионам
    DROP TABLE IF EXISTS datamart.accidents_timeline;
    CREATE TABLE datamart.accidents_timeline AS
    SELECT 
        s.region,
        EXTRACT(YEAR FROM s.accident_datetime) as year,
        EXTRACT(MONTH FROM s.accident_datetime) as month,
        TO_CHAR(s.accident_datetime, 'Month') as month_name,
        COUNT(DISTINCT h.accident_id) as accidents_count,
        SUM(COALESCE(s.dead_count, 0) + COALESCE(s.injured_count, 0)) as total_victims,
        SUM(CASE WHEN s.dead_count > 0 THEN 1 ELSE 0 END) as fatal_accidents
    FROM datavault.sat_accident_details s
    JOIN datavault.hub_accident h ON s.hub_accident_hashkey = h.hub_accident_hashkey
    WHERE s.accident_datetime IS NOT NULL
    GROUP BY s.region, EXTRACT(YEAR FROM s.accident_datetime), EXTRACT(MONTH FROM s.accident_datetime), TO_CHAR(s.accident_datetime, 'Month')
    ORDER BY s.region, year, month;
    
    SELECT '✅ Витрины данных созданы' as status;
    """
    hook.run(sql)
    logger.info("✅ Витрины данных созданы")

def validate_data(**context):
    """Проверка качества данных"""
    hook = PostgresHook(postgres_conn_id='postgres_dwh')
    
    result = hook.get_pandas_df("""
        SELECT 
            'raw_accidents' as table_name, COUNT(*) as rows FROM raw_accidents
        UNION ALL
        SELECT 'raw_vehicles', COUNT(*) FROM raw_vehicles
        UNION ALL
        SELECT 'hub_accident', COUNT(*) FROM datavault.hub_accident
        UNION ALL
        SELECT 'sat_accident_details', COUNT(*) FROM datavault.sat_accident_details
        UNION ALL
        SELECT 'victims_by_region_light', COUNT(*) FROM datamart.victims_by_region_light
        UNION ALL
        SELECT 'damaged_vehicles', COUNT(*) FROM datamart.damaged_vehicles
        UNION ALL
        SELECT 'accidents_timeline', COUNT(*) FROM datamart.accidents_timeline;
    """)
    
    for _, row in result.iterrows():
        logger.info(f"📊 {row['table_name']}: {row['rows']} записей")
    
    context['ti'].xcom_push(key='validation', value=result.to_dict('records'))

start = DummyOperator(task_id='start', dag=dag)
load_config_task = PythonOperator(task_id='load_config', python_callable=load_config, dag=dag)
extract_task = PythonOperator(task_id='extract_and_stage', python_callable=extract_and_stage, dag=dag)
create_dv_task = PythonOperator(task_id='create_datavault', python_callable=create_datavault, dag=dag)
load_hubs_task = PythonOperator(task_id='load_hubs', python_callable=load_hubs, dag=dag)
load_sats_task = PythonOperator(task_id='load_sats', python_callable=load_sats, dag=dag)
create_marts_task = PythonOperator(task_id='create_marts', python_callable=create_marts, dag=dag)
validate_task = PythonOperator(task_id='validate_data', python_callable=validate_data, dag=dag)
end = DummyOperator(task_id='end', dag=dag)

start >> load_config_task >> extract_task >> create_dv_task >> load_hubs_task >> load_sats_task >> create_marts_task >> validate_task >> end
