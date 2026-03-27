from datetime import datetime ,timedelta
import uuid
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator

#dag default arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay" : timedelta(minutes=5),
    "start_date": datetime(2026,2,26)
}

with DAG(
    dag_id = "transformed_weather_data_to_bq",
    default_args=default_args,
    schedule_interval=None,
    catchup=False
) as dag:
    
    #generate the unique batcj ID using UUID
    batch_id = f"weathear-data-batch-{str(uuid.uuid4())[:8]}"

    #submit pyspark job to dataproc serverless
    # Task 2: Submit PySpark job to Dataproc Serverless
    batch_details = {
        "pyspark_batch": {
            "main_python_file_uri": f"gs://weather-data-kds/script/weather_data_processing.py",  # Main Python file
            "python_file_uris": [],  # Python WHL files
            "jar_file_uris": [],  # JAR files
            "args": []
        },
        "runtime_config": {
            "version": "2.2",  # Specify Dataproc version (if needed)
        },
        "environment_config": {
            "execution_config": {
                "service_account": "715970340101-compute@developer.gserviceaccount.com",
                "network_uri": "projects/mythic-aloe-457912-d5/global/networks/default",
                "subnetwork_uri": "projects/mythic-aloe-457912-d5/regions/us-central1/subnetworks/default",
            }
        },
    }

    pyspark_task = DataprocCreateBatchOperator(
        task_id="spark_job_on_dataproc_serverlessß",
        batch=batch_details,
        batch_id=batch_id,
        project_id="mythic-aloe-457912-d5",
        region="us-central1",
        gcp_conn_id="google_cloud_default",
    )

    pyspark_task

