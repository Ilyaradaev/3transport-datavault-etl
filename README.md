# 3transport-datavault-etl
for: 3transport-datavault-etl
# 📚 ВЫПУСКНАЯ КВАЛИФИКАЦИОННАЯ РАБОТА

## по дисциплине «Data Architecture»

### на тему: «Разработка ETL-пайплайна транспортной аналитики на основе методологии Data Vault 2.0»

| | |
|---|---|
| **Студент** | Радаев Илья Владимирович |
| **Группа** | 16540 ЦДО |
| **Преподаватель** | Мизгирев Лев Сергеевич |
| **Дисциплина** | Data Architecture |
| **Дата сдачи** | 14 июня 2024 г. |
| **Версия** | 2.0 |

---

## 📑 ОГЛАВЛЕНИЕ

1. [Введение (цели и задачи работы)](#1-введение-цели-и-задачи-работы)
   - 1.1. Актуальность
   - 1.2. Цель работы
   - 1.3. Основные задачи
   - 1.4. Исходные данные
2. [Архитектура решения](#2-архитектура-решения)
   - 2.1. Общая архитектура системы
   - 2.2. Роли компонентов системы
   - 2.3. Схема «🚛 TRANSPORT ETL - DATA VAULT 2.0 ARCHITECTURE»
3. [Модель данных Data Vault 2.0](#3-модель-данных-data-vault-20)
   - 3.1. Основные принципы Data Vault 2.0
   - 3.2. HUB таблицы (бизнес-ключи)
   - 3.3. LINK таблицы (связи)
   - 3.4. SAT таблицы (атрибуты)
   - 3.5. Правила хеширования и SCD Type 2
   - 3.6. Схема «🏛️ DATA VAULT 2.0 - CLASS DIAGRAM (UML Notation)»
4. [Docker контейнеризация](#4-docker-контейнеризация)
   - 4.1. Принципы контейнеризации
   - 4.2. Список контейнеров
   - 4.3. Схема «Диаграмма взаимодействия контейнеров - Транспортный ETL пайплайн»
5. [ETL пайплайн](#5-etl-пайплайн)
   - 5.1. Процесс Extract (Извлечение)
   - 5.2. Процесс Transform (Трансформация)
   - 5.3. Процесс Load (Загрузка)
6. [Оркестрация Apache Airflow](#6-оркестрация-apache-airflow)
   - 6.1. DAG структура
   - 6.2. Задачи DAG
   - 6.3. Граф зависимостей
7. [Результаты выполнения](#7-результаты-выполнения)
8. [Заключение](#8-зключение)
9. [Список использованных источников](#9-список-использованных-источников)

---

## 1. ВВЕДЕНИЕ (цели и задачи работы)

### 1.1. Актуальность

В современном мире дорожно-транспортные происшествия (ДТП) представляют собой одну из наиболее острых социально-экономических проблем. По данным Всемирной организации здравоохранения (ВОЗ), ежегодно в мире в результате ДТП погибает более **1,3 миллиона человек** и около **50 миллионов** получают травмы различной степени тяжести. Экономический ущерб от ДТП составляет около **3% от мирового ВВП**.

Для эффективного управления транспортными потоками и снижения аварийности необходима централизованная система сбора, обработки и анализа данных о ДТП. Однако исходные данные часто поступают в виде неструктурированных CSV-файлов большого объема, что затрудняет их аналитическую обработку. В связи с этим возникает острая необходимость в создании **автоматизированной, масштабируемой и отказоустойчивой системы** на базе современных методологий хранения данных, таких как Data Vault 2.0.

### 1.2. Цель работы

**Основная цель:** Разработать, реализовать и задокументировать ETL-пайплайн для автоматизированного сбора, очистки, трансформации и загрузки данных о ДТП в базу данных, спроектированную по принципу **Data Vault 2.0**, с использованием Apache Airflow для оркестрации и Apache Superset для визуализации результатов.

### 1.3. Основные задачи

| № | Задача | Статус |
|---|--------|--------|
| 1 | Анализ исходных данных (3 CSV-файла, 2.36 GB) | ✅ |
| 2 | Проектирование Data Vault 2.0 (4 HUB, 3 LINK, 3 SAT) | ✅ |
| 3 | Разработка ETL кода на Python (Pandas) | ✅ |
| 4 | Создание DAG в Apache Airflow (10 задач) | ✅ |
| 5 | Контейнеризация сервисов в Docker (9 контейнеров) | ✅ |
| 6 | Визуализация в Apache Superset (4 дашборда) | ✅ |
| 7 | Подготовка документации (UML + Markdown) | ✅ |

### 1.4. Исходные данные

| Файл | Размер | Записей | Атрибутов | Разделитель |
|------|--------|---------|-----------|-------------|
| `accidents.csv` | 1.07 GB | 1,616,059 | 18 | `;` |
| `participants.csv` | 994 MB | 3,123,456 | 8 | `;` |
| `vehicles.csv` | 296 MB | 2,653,755 | 7 | `;` |
| **ИТОГО** | **2.36 GB** | **7,393,270** | **33** | **;** |

---

## 2. АРХИТЕКТУРА РЕШЕНИЯ

### 2.1. Концепция архитектуры системы представлена на схеме «🚛 TRANSPORT ETL - DATA VAULT 2.0 ARCHITECTURE»

<img width="1780" height="688" alt="image" src="https://github.com/user-attachments/assets/a697946d-4e81-40c1-8143-dc002b773127" />

**🔗 Ссылка на схему в draw.io:**  
[https://drive.google.com/file/d/1BFmK4awHHs3aTmmCiebVUvu1ovcYRadZ/view?usp=sharing](https://drive.google.com/file/d/1BFmK4awHHs3aTmmCiebVUvu1ovcYRadZ/view?usp=sharing)

**Описание схемы:** Схема иллюстрирует полную архитектуру ETL-пайплайна, включая источники данных (3 CSV-файла), Apache Airflow DAG с 10 задачами, PostgreSQL с трехуровневой структурой (Staging → Data Vault → Data Marts) и Apache Superset для визуализации.

Система построена по принципу **слоистой архитектуры** (Layered Architecture), что обеспечивает разделение ответственности и упрощает масштабирование.

| Уровень | Назначение | Компоненты |
|---------|------------|------------|
| **Уровень 1** | Источники данных | CSV-файлы на хостовой машине |
| **Уровень 2** | Оркестрация | Apache Airflow |
| **Уровень 3** | Обработка данных | Python + Pandas |
| **Уровень 4** | Хранение данных | PostgreSQL 15 + Data Vault 2.0 |
| **Уровень 5** | Визуализация | Apache Superset, PgAdmin |
| **Уровень 6** | Контейнеризация | Docker, Docker Compose |

### 2.2. Роли компонентов системы

| Компонент | Технология | Роль |
|-----------|------------|------|
| **Source** | CSV files | Исходные данные о ДТП |
| **Orchestrator** | Apache Airflow | Управление ETL-задачами |
| **Processor** | Python + Pandas | Трансформация данных |
| **Storage** | PostgreSQL 15 | Хранение Data Vault |
| **Cache** | Redis 7 | Брокер сообщений Airflow |
| **Visualization** | Apache Superset | Дашборды и аналитика |
| **Management** | PgAdmin 4 | Управление PostgreSQL |
| **Containerization** | Docker | Изоляция сервисов |

---

## 3. МОДЕЛЬ ДАННЫХ DATA VAULT

### 3.1. Схема DATA VAULT

<img width="1128" height="858" alt="image" src="https://github.com/user-attachments/assets/a6908571-fd37-4217-bcd9-105c9e824584" />

Описание схемы: UML Class Diagram для Data Vault 2.0 модели, включающий 4 HUB-класса, 3 LINK-класса и 3 SAT-класса с полными наборами атрибутов, типами ключей (PK, FK, UK) и ассоциациями между классами.

Ссылка на схему в draw.io:
https://drive.google.com/file/d/1BFmK4awHHs3aTmmCiebVUvu1ovcYRadZ/view?usp=sharing

### 3.2. Основные принципы Data Vault 2.0

Data Vault 2.0 — методология моделирования данных, обеспечивающая:

| Принцип | Описание |
|---------|----------|
| **Гибкость** | Легкое добавление новых источников |
| **Историчность** | Отслеживание всех изменений (SCD Type 2) |
| **Масштабируемость** | Горизонтальное масштабирование через хеш-ключи |
| **Аудитируемость** | Полный контроль происхождения данных |

**Описание табли  и их типов:**

| Тип | Назначение | Количество |
|-----|------------|------------|
| **HUB** | Бизнес-ключи | 4 |
| **LINK** | Связи | 3 |
| **SAT** | Атрибуты | 3 |

#### 3.2.1 HUB таблицы (бизнес-ключи)

##### 3.2.1.1 hub_accident

```sql
CREATE TABLE datavault.hub_accident (
    hub_accident_hashkey VARCHAR(32) PRIMARY KEY,
    accident_id INTEGER NOT NULL UNIQUE,
    load_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    record_source VARCHAR(50)
);
```
Статистика: 1 616 059 записей

Индексы:
CREATE INDEX idx_hub_accident_bk ON datavault.hub_accident(accident_id);

##### 3.2.1.2 hub_participant

```sql
CREATE TABLE datavault.hub_participant (
   hub_participant_hashkey VARCHAR(32) PRIMARY KEY,
   participant_id VARCHAR(50) NOT NULL UNIQUE,
   load_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
   record_source VARCHAR(50)
);
```

Статистика: 3 123 456 записей

Индексы:
CREATE INDEX idx_hub_participant_bk ON datavault.hub_participant(participant_id);

##### 3.2.1.3 hub_vehicle

```sql
CREATE TABLE datavault.hub_vehicle (
   hub_vehicle_hashkey VARCHAR(32) PRIMARY KEY,
   vehicle_id VARCHAR(50) NOT NULL UNIQUE,
   load_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
   record_source VARCHAR(50)
);
```

Статистика: 2 653 755 записей

Индексы:
CREATE INDEX idx_hub_vehicle_bk ON datavault.hub_vehicle(vehicle_id);

##### 3.2.1.4 hub_region

```sql
CREATE TABLE datavault.hub_region (
   hub_region_hashkey VARCHAR(32) PRIMARY KEY,
   region_name VARCHAR(100) NOT NULL UNIQUE,
   load_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
   record_source VARCHAR(50)
);
```

Статистика: 85+ записей

Индексы:
CREATE INDEX idx_hub_region_bk ON datavault.hub_region(region_name);

#### 3.2.2 LINK таблицы (связи)
##### 3.2.2.1 link_accident_participant

```sql
CREATE TABLE datavault.link_accident_participant (
   link_hashkey VARCHAR(32) PRIMARY KEY,
   hub_accident_hashkey VARCHAR(32),
   hub_participant_hashkey VARCHAR(32),
   load_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
   record_source VARCHAR(50),
   FOREIGN KEY (hub_accident_hashkey) REFERENCES hub_accident(hub_accident_hashkey),
   FOREIGN KEY (hub_participant_hashkey) REFERENCES hub_participant(hub_participant_hashkey)
);
```

Связь: accident ↔ participant (многие-ко-многим)

Индексы:
CREATE INDEX idx_link_acc_participant_acc ON datavault.link_accident_participant(hub_accident_hashkey);
CREATE INDEX idx_link_acc_participant_part ON datavault.link_accident_participant(hub_participant_hashkey);

##### 3.2.2.2 link_accident_vehicle

```sql
CREATE TABLE datavault.link_accident_vehicle (
   link_hashkey VARCHAR(32) PRIMARY KEY,
   hub_accident_hashkey VARCHAR(32),
   hub_vehicle_hashkey VARCHAR(32),
   load_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
   record_source VARCHAR(50),
   FOREIGN KEY (hub_accident_hashkey) REFERENCES hub_accident(hub_accident_hashkey),
   FOREIGN KEY (hub_vehicle_hashkey) REFERENCES hub_vehicle(hub_vehicle_hashkey)
);
```

Связь: accident ↔ vehicle (многие-ко-многим)

Индексы:
CREATE INDEX idx_link_acc_vehicle_acc ON datavault.link_accident_vehicle(hub_accident_hashkey);
CREATE INDEX idx_link_acc_vehicle_veh ON datavault.link_accident_vehicle(hub_vehicle_hashkey);

##### 3.2.2.3 link_region_accident

```sql
CREATE TABLE datavault.link_region_accident (
link_hashkey VARCHAR(32) PRIMARY KEY,
hub_region_hashkey VARCHAR(32),
hub_accident_hashkey VARCHAR(32),
load_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
record_source VARCHAR(50),
FOREIGN KEY (hub_region_hashkey) REFERENCES hub_region(hub_region_hashkey),
FOREIGN KEY (hub_accident_hashkey) REFERENCES hub_accident(hub_accident_hashkey)
);
```

Связь: region ↔ accident

Индексы:
CREATE INDEX idx_link_region_accident_reg ON datavault.link_region_accident(hub_region_hashkey);
CREATE INDEX idx_link_region_accident_acc ON datavault.link_region_accident(hub_accident_hashkey);

#### 3.2.3 SAT таблицы (атрибуты)

##### 3.2.3.1 sat_accident_details

```sql
CREATE TABLE datavault.sat_accident_details (
   sat_hashkey VARCHAR(32) PRIMARY KEY,
   hub_accident_hashkey VARCHAR(32),
   tags TEXT,
   category VARCHAR(100),
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
   load_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
   record_source VARCHAR(50),
   hashdiff VARCHAR(32),
   FOREIGN KEY (hub_accident_hashkey) REFERENCES hub_accident(hub_accident_hashkey)
);
```

Статистика: 1 616 059 записей

Индексы:
CREATE INDEX idx_sat_accident_hub ON datavault.sat_accident_details(hub_accident_hashkey);
CREATE INDEX idx_sat_accident_datetime ON datavault.sat_accident_details(accident_datetime);

##### 3.2.3.2 sat_participant_details

```sql
CREATE TABLE datavault.sat_participant_details (
sat_hashkey VARCHAR(32) PRIMARY KEY,
hub_participant_hashkey VARCHAR(32),
role VARCHAR(50),
gender VARCHAR(10),
violations TEXT,
health_status VARCHAR(100),
years_of_driving_experience INTEGER,
load_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
record_source VARCHAR(50),
hashdiff VARCHAR(32),
FOREIGN KEY (hub_participant_hashkey) REFERENCES hub_participant(hub_participant_hashkey)
);
```

Статистика: 3 123 456 записей

Индексы:
CREATE INDEX idx_sat_participant_hub ON datavault.sat_participant_details(hub_participant_hashkey);
CREATE INDEX idx_sat_participant_role ON datavault.sat_participant_details(role);

##### 3.2.3.3 sat_vehicle_details

```sql
CREATE TABLE datavault.sat_vehicle_details (
   sat_hashkey VARCHAR(32) PRIMARY KEY,
   hub_vehicle_hashkey VARCHAR(32),
   category VARCHAR(50),
   brand VARCHAR(100),
   model VARCHAR(100),
   color VARCHAR(30),
   year INTEGER,
   load_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
   record_source VARCHAR(50),
   hashdiff VARCHAR(32),
   FOREIGN KEY (hub_vehicle_hashkey) REFERENCES hub_vehicle(hub_vehicle_hashkey)
);
```

Статистика: 2 653 755 записей

Индексы:
CREATE INDEX idx_sat_vehicle_hub ON datavault.sat_vehicle_details(hub_vehicle_hashkey);
CREATE INDEX idx_sat_vehicle_brand ON datavault.sat_vehicle_details(brand);
CREATE INDEX idx_sat_vehicle_year ON datavault.sat_vehicle_details(year);

#### 3.3. Правила хеширования и SCD Type 2
Формулы хеширования для суррогатных ключей:

##### -- HUB ключи
hub_accident_hashkey = MD5(accident_id)
hub_participant_hashkey = MD5(participant_id)
hub_vehicle_hashkey = MD5(vehicle_id)
hub_region_hashkey = MD5(region_name)

##### -- SAT ключи
sat_hashkey = MD5(hub_accident_hashkey || COALESCE(datetime, ''))

##### -- LINK ключи
link_hashkey = MD5(hub_accident_hashkey || hub_participant_hashkey)

##### -- Hashdiff для отслеживания изменений
hashdiff = MD5(COALESCE(field1, '') || '|' || COALESCE(field2, '') || '|' || COALESCE(field3, ''))

Принцип SCD Type 2:

Новая запись: INSERT в SAT с новым sat_hashkey

Изменение атрибутов: INSERT новой версии записи (старая остается)

Запрос актуальных данных: SELECT * FROM sat WHERE hashdiff = current_hash


---

## 4. Docker КОНТЕЙНЕРИЗАЦИЯ
### 4.1. Схема взаимодействия контейнеров:

<img width="1267" height="820" alt="image" src="https://github.com/user-attachments/assets/839050ad-d70c-4e97-abca-ed7961eb66be" />

Описание схемы: Диаграмма иллюстрирует физическое развертывание 9 Docker-контейнеров в среде Docker Host, их сетевое взаимодействие через транспортную сеть, а также монтирование томов для персистентности данных (PostgreSQL, Airflow, Superset) и внешних CSV-файлов. На схеме показаны связи между контейнерами, используемые порты и типы взаимодействия (HTTP, PostgreSQL, RESP, Internal RPC).
      Как читать схему:
      Администратор через браузер (порт 8080) подключается к Airflow Webserver для управления DAG
      
      Webserver передаёт команды Scheduler
      
      Scheduler отправляет задачи в Redis (очередь) и распределяет их между Worker
      
      Worker:
      
      Читает CSV-файлы через volume mount
      
      Записывает данные в PostgreSQL DWH (порт 5432)
      
      Обновляет метаданные в PostgreSQL Airflow
      
      Аналитик через браузер (порт 8088) подключается к Superset для просмотра дашбордов
      
      Superset читает данные из PostgreSQL DWH
      
      Init запускается один раз при старте для инициализации метабазы
      
      PgAdmin опционально используется для административного доступа к БД

### 4.2. Принципы контейнеризации

Контейнеризация — это метод виртуализации на уровне операционной системы, который позволяет запускать приложения в изолированных средах — контейнерах. Каждый контейнер включает в себя приложение и все его зависимости (библиотеки, конфигурации), но использует ядро хостовой ОС.

**Преимущества контейнеризации для данного проекта:**

| Преимущество | Описание |
|--------------|----------|
| Изоляция | Каждый сервис работает в своем контейнере, не влияя на другие |
| Портативность | Запуск на любой ОС с Docker (Windows, macOS, Linux) |
| Воспроизводимость | Одинаковое окружение на всех этапах (разработка, тестирование, производство) |
| Масштабируемость | Легкое увеличение количества экземпляров сервисов |
| Управляемость | Централизованное управление через docker-compose |

### 4.3. Список контейнеров

| № | Контейнер | Образ | Порты | Назначение |
|---|-----------|-------|-------|------------|
| 1 | transport_postgres | postgres:15 | 5432:5432 | Data Warehouse (DWH) |
| 2 | airflow_postgres | postgres:13 | - | Метаданные Airflow |
| 3 | transport_redis | redis:7-alpine | 6379:6379 | Брокер сообщений |
| 4 | transport_airflow_webserver | apache/airflow:2.7.1 | 8080:8080 | Web UI Airflow |
| 5 | transport_airflow_scheduler | apache/airflow:2.7.1 | - | Планировщик задач |
| 6 | transport_airflow_worker | apache/airflow:2.7.1 | - | Исполнитель задач ETL |
| 7 | transport_airflow_init | apache/airflow:2.7.1 | - | Инициализация БД (одноразовый) |
| 8 | transport_superset | apache/superset:4.0.0 | 8088:8088 | Визуализация данных |
| 9 | transport_pgadmin | dpage/pgadmin4:latest | 5050:80 | Управление PostgreSQL |

### 4.4. Конфигурация сети

**Docker Compose network:**

```yaml
networks:
  transport_network:
    driver: bridge
    name: transport_network
```
### 4.5. Тома и монтирование данных
Тома (Volumes) для персистентности данных:
```yaml
volumes:
  postgres_data:        # Данные Data Warehouse
  airflow_postgres_data: # Метаданные Airflow
  superset_data:        # Данные Superset
  pgadmin_data:         # Данные PgAdmin
```

### 4.6. Healthcheck и мониторинг

Healthcheck (проверка здоровья) — это механизм Docker, который позволяет отслеживать состояние контейнеров и автоматически перезапускать их при сбоях.

**Настройки healthcheck для PostgreSQL (Data Warehouse):**

```yaml
healthcheck: 
  test: ["CMD-SHELL", "pg_isready -U etl_user3 -d dwh_etl_db"] # Команда проверки готовности PostgreSQL
  interval: 10s #Интервал между проверками
  timeout: 5s #Таймаут выполнения проверки
  retries: 5 #Количество попыток до признания контейнера unhealthy
  start_period: 30s # Время, после которого начинаются проверки
```

### Настройки healthcheck для Airflow Webserver

Healthcheck для Airflow Webserver позволяет отслеживать состояние веб-интерфейса и автоматически перезапускать контейнер при обнаружении проблем. Это критически важно для production-среды, так как обеспечивает самовосстановление системы.

**Конфигурация healthcheck в docker-compose.yml:**

```yaml
healthcheck:
  test: ["CMD", "curl", "--fail", "http://localhost:8080/health"] # Команда проверки: отправка HTTP-запроса к эндпоинту /health на порт 8080
  interval: 30s # Интервал между проверками (каждые 30 секунд)
  timeout: 10s # 	Максимальное время ожидания ответа на запрос
  retries: 5 # Количество неудачных попыток до признания контейнера unhealthy
  start_period: 60s # Время ожидания перед началом первых проверок (60 секунд на запуск)
```

--- 

## 5. ETL ПАЙПЛАЙН

### 5.1. Общая структура ETL процесса

ETL (Extract, Transform, Load) — это процесс извлечения данных из источников, их трансформации (очистки, преобразования) и загрузки в целевое хранилище данных. В данном проекте ETL-пайплайн реализован на языке Python с использованием библиотеки Pandas.

**Схема ETL процесса:**

<img width="1780" height="688" alt="image" src="https://github.com/user-attachments/assets/a697946d-4e81-40c1-8143-dc002b773127" />


**Роли компонентов ETL:**

| Компонент | Технология | Функция |
|-----------|------------|---------|
| **Extract** | pandas.read_csv() | Чтение CSV-файлов с разделителем `;` |
| **Transform** | pandas (drop_duplicates, replace) | Очистка данных от дубликатов и пропусков |
| **Load** | pandas.to_sql() | Запись данных в PostgreSQL чанками |

---

### 5.2. Процесс Extract (Извлечение данных)

Извлечение данных происходит из трёх CSV-файлов, расположенных на хостовой машине. Файлы монтируются в контейнер Airflow через Docker volume.

**Источники данных:**

| Файл | Размер | Количество записей | Разделитель | Кодировка |
|------|--------|-------------------|-------------|-----------|
| `accidents_all_regions_126_v20260501.csv` | 1.07 GB | 1,616,059 | `;` | UTF-8 |
| `participants_all_regions_126_v20260501.csv` | 994 MB | 3,123,456 | `;` | UTF-8 |
| `vehicles_all_regions_126_v20260501.csv` | 296 MB | 2,653,755 | `;` | UTF-8 |

**Код извлечения данных:**

```python
def extract_and_stage(**context):
    config = context['ti'].xcom_pull(key='config', task_ids='load_config')
    hook = PostgresHook(postgres_conn_id='postgres_dwh')
    engine = hook.get_sqlalchemy_engine()
    
    for dataset in config['datasets']:
        file_path = os.path.join(config['source_path'], dataset['filename'])
        table_name = f"raw_{dataset['name']}"
        
        if not os.path.exists(file_path):
            logger.error(f"Файл не найден: {file_path}")
            raise FileNotFoundError(f"Файл не найден: {file_path}")
        
        logger.info(f"Обработка файла: {dataset['name']} ({dataset['filename']})")
        
        # Чтение CSV 
        df = pd.read_csv(file_path, sep=';', encoding='utf-8', low_memory=False)
        logger.info(f"Загружено {len(df):,} записей из {dataset['name']}")


### 5.3. Процесс Transform (Трансформация данных)
Трансформация данных включает очистку от дубликатов, замену пропущенных значений и приведение типов данных.

```
# Удаление дубликатов
before = len(df)
df = df.drop_duplicates()
logger.info(f"Удалено дубликатов: {before - len(df):,}")

```

```
# Замена пропущенных значений на None
df = df.replace({np.nan: None, '': None, 'NaN': None})
```



![Uploading image.png…]()


