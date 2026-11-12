# Airflow Currency Exchange Rates ETL Pipeline

## Project Overview
This project implements a daily ETL pipeline using the Astronomer Astro CLI (Apache Airflow), Docker Compose, MongoDB, and Mongo Express. It extracts daily EUR currency exchange rates (USD, GBP, JPY) from the Frankfurter API (v2/rates), transforms the records to a standardized schema using Airflow Named XComs, and loads them idempotently into MongoDB backed by persistent Docker volumes and a custom bridge network.

## Prerequisites
- Docker Desktop (running)
- Astro CLI (brew install astro)
- Git

## How to Run
Clone the repository onto your Desktop (or any directory) and start the stack with zero manual configuration required:

    cd ~/Desktop
    git clone [https://github.com/abyaqoob/airflow-currency-etl.git](https://github.com/abyaqoob/airflow-currency-etl.git) airflow_currency_etl
    cd ~/Desktop/airflow_currency_etl
    astro dev start

## Service URLs and Local Credentials
| Service | URL / Host | Username | Password |
| :--- | :--- | :--- | :--- |
| Airflow Web UI | http://localhost:8080 | admin | admin |
| Mongo Express Web UI | http://localhost:8081 | expressadmin | expresspassword |
| MongoDB (Internal / Host) | mongodb:27017 / localhost:27017 | admin | adminpassword |

## DAG Architecture (currency_exchange_rates_etl)
- Schedule: @daily (catchup=False)
- Task 1 (extract_exchange_rates): Queries [https://api.frankfurter.dev/v2/rates?base=EUR&quotes=USD,GBP,JPY](https://api.frankfurter.dev/v2/rates?base=EUR&quotes=USD,GBP,JPY) and pushes the JSON array to Named XCom key raw_exchange_rates.
- Task 2 (transform_exchange_rate_data): Pulls raw_exchange_rates, normalizes each quote into {base_currency, target_currency, exchange_rate, rate_date, source, retrieved_at}, and pushes the result to Named XCom key transformed_exchange_rates.
- Task 3 (load_data_into_mongodb): Pulls transformed_exchange_rates and performs idempotent upserts into currency_db.exchange_rates using a unique compound index on (base_currency, target_currency, rate_date).
