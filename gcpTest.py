from datetime import timedelta

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.contrib.operators import gcs_to_bq, bigquery_get_data


default_args = {
    'start_date': days_ago(2),
}


with DAG('seadata_test', default_args=default_args, schedule_interval=timedelta(days=1)) as dag:

    # First load the files from storage to BigQuery.
    # No schema is described so auto_detect is default to ON.
    load_files = gcs_to_bq.GoogleCloudStorageToBigQueryOperator(
        task_id='gcs_to_bq',
        bucket='seadata-investing',
        source_objects=['*.gz'],
        destination_project_dataset_table='testseadatastore.test',
        write_disposition='WRITE_TRUNCATE',
        dag=dag)

    # Query the data in the table to check if it exists.
    check_data = bigquery_get_data.BigQueryGetDataOperator(
        task_id='check_bq_data',
        dataset_id='testseadatastore',
        table_id='test',
        key_path='amdocs-mesos-cas-upgrade-fa98cca347a6.json',
        depends_on_past=True  # Only work after "parent" task was successful.
    )

    # Set the load_files task as the "parent" of "check_data" task
    load_files >> check_data
