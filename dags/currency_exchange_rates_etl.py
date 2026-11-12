import logging
import os
from datetime import datetime, timedelta, timezone
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from pymongo import ASCENDING, MongoClient
from pymongo.errors import PyMongoError

logger = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


def extract_currency_rates(**context):
    ti = context["ti"]
    api_url = os.getenv("FRANKFURTER_API_URL", "https://api.frankfurter.dev/v2/rates")
    params = {"base": "EUR", "quotes": "USD,GBP,JPY"}
    logger.info("Requesting exchange rates from %s with params %s", api_url, params)
    response = requests.get(api_url, params=params, timeout=15)
    response.raise_for_status()
    raw_data = response.json()
    if not isinstance(raw_data, list) or not raw_data:
        raise ValueError(f"Expected non-empty JSON list from Frankfurter API, got: {raw_data}")
    logger.info("Extracted %d rate records: %s", len(raw_data), raw_data)
    ti.xcom_push(key="raw_exchange_rates", value=raw_data)


def transform_currency_rates(**context):
    ti = context["ti"]
    raw_data = ti.xcom_pull(
        task_ids="extract_exchange_rates",
        key="raw_exchange_rates",
    )
    if not raw_data:
        raise ValueError("No data pulled from XCom key 'raw_exchange_rates'")
    retrieved_at = (
        datetime.now(timezone.utc)
        .replace(microsecond=0, tzinfo=None)
        .isoformat()
    )
    transformed_records = []
    for item in raw_data:
        record = {
            "base_currency": str(item["base"]),
            "target_currency": str(item["quote"]),
            "exchange_rate": float(item["rate"]),
            "rate_date": str(item["date"]),
            "source": "Frankfurter API",
            "retrieved_at": retrieved_at,
        }
        transformed_records.append(record)
    logger.info("Transformed %d records: %s", len(transformed_records), transformed_records)
    ti.xcom_push(key="transformed_exchange_rates", value=transformed_records)


def load_rates_to_mongodb(**context):
    ti = context["ti"]
    records = ti.xcom_pull(
        task_ids="transform_exchange_rate_data",
        key="transformed_exchange_rates",
    )
    if not records:
        raise ValueError("No data pulled from XCom key 'transformed_exchange_rates'")
    mongo_uri = os.getenv(
        "MONGO_URI",
        "mongodb://admin:adminpassword@mongodb:27017/?authSource=admin",
    )
    db_name = os.getenv("MONGO_DB_NAME", "currency_db")
    collection_name = os.getenv("MONGO_COLLECTION_NAME", "exchange_rates")
    client = MongoClient(mongo_uri, serverSelectionTimeoutMS=10000)
    try:
        client.admin.command("ping")
        collection = client[db_name][collection_name]
        collection.create_index(
            [
                ("base_currency", ASCENDING),
                ("target_currency", ASCENDING),
                ("rate_date", ASCENDING),
            ],
            unique=True,
            name="uniq_base_target_date",
        )
        inserted_count = 0
        updated_count = 0
        for record in records:
            filter_query = {
                "base_currency": record["base_currency"],
                "target_currency": record["target_currency"],
                "rate_date": record["rate_date"],
            }
            result = collection.update_one(
                filter_query,
                {"$set": record},
                upsert=True,
            )
            if result.upserted_id is not None:
                inserted_count += 1
            elif result.modified_count > 0:
                updated_count += 1
        total_in_collection = collection.count_documents({})
        logger.info(
            "MongoDB load complete. Inserted: %d, Updated: %d, Total documents in %s.%s: %d",
            inserted_count,
            updated_count,
            db_name,
            collection_name,
            total_in_collection,
        )
    except PyMongoError as exc:
        logger.error("MongoDB operation failed: %s", exc)
        raise
    finally:
        client.close()


with DAG(
    dag_id="currency_exchange_rates_etl",
    default_args=DEFAULT_ARGS,
    description="Daily ETL pipeline integrating Frankfurter currency exchange rates into MongoDB",
    schedule="@daily",
    start_date=datetime(2026, 11, 1, tzinfo=timezone.utc),
    catchup=False,
    tags=["currency", "frankfurter", "mongodb", "etl"],
) as dag:
    extract_exchange_rates = PythonOperator(
        task_id="extract_exchange_rates",
        python_callable=extract_currency_rates,
    )

    transform_exchange_rate_data = PythonOperator(
        task_id="transform_exchange_rate_data",
        python_callable=transform_currency_rates,
    )

    load_data_into_mongodb = PythonOperator(
        task_id="load_data_into_mongodb",
        python_callable=load_rates_to_mongodb,
    )

    extract_exchange_rates >> transform_exchange_rate_data >> load_data_into_mongodb
