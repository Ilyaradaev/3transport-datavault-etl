# 3transport-datavault-etl
for: 3transport-datavault-etl
# МГТУ им Н.Э. Баумана
# Выпускная квалификационная работа

# ETL-пайплайн транспортной аналитики

**Автор:** Радаев Илья Владимирович   
**Дата:** 14 июня 2024 г.  
**Версия:** 1.0  

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

# 📚 РАЗДЕЛ: ЦЕЛИ И ЗАДАЧИ РАБОТЫ

## Полное описание для документации

---

## 1. ВВЕДЕНИЕ

### 1.1. Актуальность проблемы

В современном мире дорожно-транспортные происшествия (ДТП) представляют собой одну из наиболее острых социально-экономических проблем. По данным статистики, ежегодно в мире в результате ДТП погибает более 1,3 миллиона человек и около 50 миллионов получают травмы различной степени тяжести. Экономический ущерб от ДТП составляет около 3% от мирового ВВП.

**Ключевые проблемы, решаемые в работе:**

| Проблема | Описание |
|----------|----------|
| **Разрозненность данных** | Информация о ДТП хранится в различных форматах и источниках |
| **Отсутствие стандартизации** | Данные не приведены к единому формату и структуре |
| **Сложность анализа** | Объемы данных достигают миллиардов записей, что затрудняет ручной анализ |
| **Отсутствие историчности** | Невозможно отследить изменения данных во времени |
| **Низкая производительность** | Традиционные СУБД не справляются с нагрузкой при анализе |

### 1.2. Актуальность темы

**Для науки и практики:**
- Разработка эффективных ETL-пайплайнов для больших данных
- Внедрение Data Vault архитектуры в аналитические системы
- Автоматизация процессов обработки данных о ДТП
- Создание основы для предиктивной аналитики и ML-моделей

**Для общества:**
- Повышение безопасности дорожного движения
- Выявление опасных участков дорог
- Анализ факторов, влияющих на аварийность
- Обоснованное принятие управленческих решений

---

## 2. ЦЕЛЬ РАБОТЫ

### 2.1. Основная цель

**Разработать, реализовать и документировать ETL-пайплайн для автоматизированного сбора, обработки и загрузки данных о дорожно-транспортных происшествиях, участниках и транспортных средствах в базу данных, построенную по принципу Data Vault, с обеспечением возможностей оркестрации процессов в Apache Airflow и визуализации результатов в Apache Superset.**

### 2.2. Ключевые аспекты цели

```mermaid
graph TD
    A["ЦЕЛЬ РАБОТЫ"] --> B["Сбор данных"]
    A --> C["Обработка данных"]
    A --> D["Хранение данных"]
    A --> E["Оркестрация"]
    A --> F["Визуализация"]
    
    B --> B1["CSV файлы (3 источника)"]
    B --> B2["Объем 2.36 GB"]
    B --> B3["7.4 млн записей"]
    
    C --> C1["Удаление дубликатов"]
    C --> C2["Замена пропусков"]
    C --> C3["Приведение типов"]
    
    D --> D1["Data Vault архитектура"]
    D --> D2["HUB, LINK, SAT"]
    D --> D3["PostgreSQL 15"]
    
    E --> E1["Apache Airflow"]
    E --> E2["10 задач в DAG"]
    E --> E3["Планирование @daily"]
    
    F --> F1["Apache Superset"]
    F --> F2["4 витрины данных"]
    F --> F3["Интерактивные дашборды"]

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
