FROM astrocrpublic.azurecr.io/runtime:3.3-12
LABEL project.name="airflow_currency_etl"
LABEL docker.networking="currency_etl_network (user-defined bridge network connecting Airflow, MongoDB, and Mongo Express)"
LABEL docker.persistent_storage="mongodb_data (/data/db) and mongodb_config (/data/configdb) named Docker volumes"
