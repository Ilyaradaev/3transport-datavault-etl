# 3transport-datavault-etl
for: 3transport-datavault-etl
# МГТУ им Н.Э. Баумана
# Выпускная квалификационная работа

# ETL-пайплайн транспортной аналитики

---

# 📄 Документация к заданию №1: ETL-пайплайн транспортной аналитики (Data Vault)

**Автор:** Радаев Илья Владимирович   

**Группа:** 16540 ФДО

**Дисциплина:** Data Architecture  

**Репозиторий:** `3transport-datavault-etl` 

**Дата:** 14 июня 2024 г.

---

## 📑 Оглавление

1. [Цели и задачи работы](#1-цели-и-задачи-работы)
2. [Архитектура решения](#2-архитектура-решения)
   - 2.1. [Диаграмма компонентов и развертывания (UML Deployment)](#21-диаграмма-компонентов-и-развертывания-uml-deployment)
   - 2.2. [Схема потоков данных (Data Flow Diagram - DFD)](#22-схема-потоков-данных-data-flow-diagram-dfd)
3. [Модель данных Data Vault (UML Class Diagram)](#3-модель-данных-data-vault-uml-class-diagram)
4. [Детали ETL-пайплайна](#4-детали-etl-пайплайна)
   - 4.1. [Диаграмма последовательности (Sequence Diagram)](#41-диаграмма-последовательности-sequence-diagram)
   - 4.2. [Диаграмма активности DAG (Activity Diagram)](#42-диаграмма-активности-dag-activity-diagram)
5. [Оркестрация Apache Airflow](#5-оркестрация-apache-airflow)
6. [Docker контейнеризация](#6-docker-контейнеризация)
7. [Результаты выполнения и тест-кейсы](#7-результаты-выполнения-и-тест-кейсы)
8. [Заключение](#8-заключение)

---

## 1. Цели и задачи работы

### 1.1. Актуальность
Обработка и анализ больших массивов данных о ДТП критически важна для выявления аварийно-опасных участков, анализа причинности и принятия обоснованных управленческих решений.

### 1.2. Цель работы
Разработать, реализовать и документировать отказоустойчивый ETL-пайплайн для автоматизированного сбора, очистки, трансформации и загрузки данных о ДТП в базу данных, спроектированную по **методологии Data Vault**, с использованием Apache Airflow и визуализацией в Apache Superset.

### 1.3. Основные задачи , выполняемы в рамках работы
1. Анализ 3 CSV-файлов (2.36 GB, 7.4 млн записей).
2. Проектирование Data Vault: 4 Hub, 3 Link, 3 Satellite.
3. Разработка ETL на Python (Pandas): удаление дубликатов, замена NaN, чанковая загрузка.
4. Оркестрация в Airflow: DAG из 10 задач.
5. Контейнеризация: 8 Docker-контейнеров.
6. Визуализация: 4 дашборда в Superset.
7. Полное документирование (UML + Markdown).

---

## 2. Архитектура решения

### 2.1. Диаграмма компонентов и развертывания (UML Deployment)

**🔗 Ссылка на draw.io (Deployment Diagram):**  
[https://app.diagrams.net/?lightbox=1&edit=_blank#Uhttps://raw.githubusercontent.com/Ilyaradaev/3transport-datavault-etl/main/docs/diagrams/deployment-diagram.drawio](https://app.diagrams.net/?lightbox=1&edit=_blank#Uhttps://raw.githubusercontent.com/Ilyaradaev/3transport-datavault-etl/main/docs/diagrams/deployment-diagram.drawio)

**Описание:** Физическое развертывание в Docker, логические связи компонентов.

---

### 2.2. Схема потоков данных (Data Flow Diagram - DFD)

**🔗 Ссылка на draw.io (DFD):**  
[https://app.diagrams.net/?lightbox=1&edit=_blank#Uhttps://raw.githubusercontent.com/Ilyaradaev/3transport-datavault-etl/main/docs/diagrams/dfd-diagram.drawio](https://app.diagrams.net/?lightbox=1&edit=_blank#Uhttps://raw.githubusercontent.com/Ilyaradaev/3transport-datavault-etl/main/docs/diagrams/dfd-diagram.drawio)

---

## 3. Модель данных Data Vault (UML Class Diagram)

**🔗 Ссылка на draw.io (Data Vault Class Diagram):**  
[https://app.diagrams.net/?lightbox=1&edit=_blank#Uhttps://raw.githubusercontent.com/Ilyaradaev/3transport-datavault-etl/main/docs/diagrams/datavault-class-diagram.drawio](https://app.diagrams.net/?lightbox=1&edit=_blank#Uhttps://raw.githubusercontent.com/Ilyaradaev/3transport-datavault-etl/main/docs/diagrams/datavault-class-diagram.drawio)

**Структура Data Vault:**
- **HUB таблицы (4):** hub_accident, hub_participant, hub_vehicle, hub_region
- **LINK таблицы (3):** link_accident_participant, link_accident_vehicle, link_region_accident
- **SAT таблицы (3):** sat_accident_details, sat_participant_details, sat_vehicle_details

---

## 4. Детали ETL-пайплайна

### 4.1. Диаграмма последовательности (Sequence Diagram)

**🔗 Ссылка на draw.io (Sequence Diagram):**  
[https://app.diagrams.net/?lightbox=1&edit=_blank#Uhttps://raw.githubusercontent.com/Ilyaradaev/3transport-datavault-etl/main/docs/diagrams/sequence-diagram.drawio](https://app.diagrams.net/?lightbox=1&edit=_blank#Uhttps://raw.githubusercontent.com/Ilyaradaev/3transport-datavault-etl/main/docs/diagrams/sequence-diagram.drawio)

---

### 4.2. Диаграмма активности DAG (Activity Diagram)

**🔗 Ссылка на draw.io (Activity Diagram):**  
[https://app.diagrams.net/?lightbox=1&edit=_blank#Uhttps://raw.githubusercontent.com/Ilyaradaev/3transport-datavault-etl/main/docs/diagrams/activity-diagram.drawio](https://app.diagrams.net/?lightbox=1&edit=_blank#Uhttps://raw.githubusercontent.com/Ilyaradaev/3transport-datavault-etl/main/docs/diagrams/activity-diagram.drawio)

---

## 5. Оркестрация Apache Airflow

**Имя DAG:** `transport_full_etl`  
**Расписание:** `@daily`  
**Количество задач:** 10

**Граф зависимостей:**
```text
start → load_config → extract_and_stage → create_datavault → load_hubs → load_sats → load_links → create_marts → validate_quality → end


