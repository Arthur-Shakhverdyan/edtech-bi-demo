# 📊 EdTech BI Demo

Полноценная демонстрация BI-архитектуры на open-source технологиях: от генерации данных до визуализации.

---

## 📌 Цель проекта

Показать архитектурный подход к построению BI-системы:
- автоматическая генерация данных (эмуляция CRM);
- обработка через DAG в Airflow;
- построение витрин в PostgreSQL и ClickHouse;
- визуализация в DataLens.

---

## 🧱 Архитектура

[CRM (эмуляция)] --> [RAW (PostgreSQL)] --> [ODM (PostgreSQL)] --> [BI (ClickHouse)] --> [DataLens]
|
[Airflow DAGs]


**Компоненты:**
- **PostgreSQL** — два слоя: raw и ODM
- **ClickHouse** — хранение витрин
- **Apache Airflow** — DAG’и для генерации, трансформации и репликации
- **Yandex DataLens** — визуализация отчётов
- **Docker** — контейнеризация и запуск всех сервисов

---

## ⚙️ Как запустить

```bash
git clone https://github.com/Arthur-Shakhverdyan/edtech-bi-demo.git
cd edtech-bi-demo
docker-compose up -d

## 🚀 Структура DAG'ов

- **data_generate_crm_data_dag** — эмуляция данных, запись в RAW  
- **etl_odm_dag** — обрабатывает RAW → ODM  
- **replication_dag** — реплицирует ODM → ClickHouse  

---

## 📊 Пример визуализации

_(Здесь будут скриншоты из DataLens — метрики, графики, таблицы)_
