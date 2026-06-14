# 3transport-datavault-etl
for: 3transport-datavault-etl


cat > docs/README.md << 'EOF'
# 📚 Документация ETL-пайплайна транспортной аналитики

**Автор:** Ilyaradaev  
**Дата:** 14 июня 2024 г.  
**Версия:** 2.0  

---

## 📑 Оглавление

1. [Цели и задачи работы](#1-цели-и-задачи-работы)
2. [Архитектура решения](#2-архитектура-решения)
3. [Data Vault модель](#3-data-vault-модель)
4. [ETL пайплайн](#4-etl-пайплайн)
5. [Оркестрация Apache Airflow](#5-оркестрация-apache-airflow)
6. [Docker контейнеризация](#6-docker-контейнеризация)
7. [Результаты выполнения](#7-результаты-выполнения)
8. [Заключение](#8-заключение)

---

## 1. Цели и задачи работы

### 1.1. Актуальность

Анализ дорожно-транспортных происшествий является критически важной задачей для повышения безопасности дорожного движения.

### 1.2. Цель работы

Разработать ETL-пайплайн для сбора, обработки и загрузки данных о ДТП в базу данных Data Vault с визуализацией в Superset.

### 1.3. Исходные данные

| Файл | Размер | Количество записей | Атрибутов |
|------|--------|-------------------|-----------|
| accidents.csv | 1.07 GB | 1,616,059 | 18 |
| participants.csv | 994 MB | 3,123,456 | 8 |
| vehicles.csv | 296 MB | 2,653,755 | 7 |
| **ИТОГО** | **2.36 GB** | **7,393,270** | **33** |

---

## 2. Архитектура решения

### 2.1. Общая архитектура

Система построена по принципу слоистой архитектуры:

1. **Уровень источников данных** - CSV файлы
2. **Уровень оркестрации** - Apache Airflow
3. **Уровень обработки** - Python ETL скрипты
4. **Уровень хранения** - PostgreSQL Data Vault
5. **Уровень визуализации** - Apache Superset

### 2.2. Схемы в draw.io

| Диаграмма | Ссылка |
|-----------|--------|
| DFD диаграмма | [https://app.diagrams.net/#Uhttps://raw.githubusercontent.com/Ilyaradaev/3transport-datavault-etl/main/docs/diagrams/dfd-diagram.drawio](https://app.diagrams.net/#Uhttps://raw.githubusercontent.com/Ilyaradaev/3transport-datavault-etl/main/docs/diagrams/dfd-diagram.drawio) |
| Контейнерная диаграмма | [https://app.diagrams.net/#Uhttps://raw.githubusercontent.com/Ilyaradaev/3transport-datavault-etl/main/docs/diagrams/container-diagram.drawio](https://app.diagrams.net/#Uhttps://raw.githubusercontent.com/Ilyaradaev/3transport-datavault-etl/main/docs/diagrams/container-diagram.drawio) |
| Data Vault Class | [https://app.diagrams.net/#Uhttps://raw.githubusercontent.com/Ilyaradaev/3transport-datavault-etl/main/docs/diagrams/datavault-class-diagram.drawio](https://app.diagrams.net/#Uhttps://raw.githubusercontent.com/Ilyaradaev/3transport-datavault-etl/main/docs/diagrams/datavault-class-diagram.drawio) |
| Sequence диаграмма | [https://app.diagrams.net/#Uhttps://raw.githubusercontent.com/Ilyaradaev/3transport-datavault-etl/main/docs/diagrams/sequence-diagram.drawio](https://app.diagrams.net/#Uhttps://raw.githubusercontent.com/Ilyaradaev/3transport-datavault-etl/main/docs/diagrams/sequence-diagram.drawio) |

---

## 3. Data Vault модель

### 3.1. HUB таблицы

| HUB таблица | Бизнес-ключ | Количество записей |
|-------------|-------------|-------------------|
| hub_accident | accident_id | 1,616,059 |
| hub_participant | participant_id | 3,123,456 |
| hub_vehicle | vehicle_id | 2,653,755 |
| hub_region | region_name | 85+ |

### 3.2. LINK таблицы

| LINK таблица | Связь |
|--------------|-------|
| link_accident_participant | accident ↔ participant |
| link_accident_vehicle | accident ↔ vehicle |
| link_region_accident | region ↔ accident |

### 3.3. SAT таблицы

| SAT таблица | Основные атрибуты |
|-------------|-------------------|
| sat_accident_details | tags, category, datetime, light, severity, dead_count, injured_count |
| sat_participant_details | role, gender, violations, health_status, experience |
| sat_vehicle_details | category, brand, model, color, year |

---

## 4. ETL пайплайн

### 4.1. Процесс Extract

```python
df = pd.read_csv(file_path, sep=';', encoding='utf-8', low_memory=False)
