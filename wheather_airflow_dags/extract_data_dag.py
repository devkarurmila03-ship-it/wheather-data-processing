from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python import PythonVirtualenvOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.models import Variable
from datetime import timedelta,datetime

default_args = {
    "owner": "airflow",
    "depends_on_past":False,
    "retries" : 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2026,2,26)
}

with DAG(
    dag_id = 'openwheather_api_to_gcs',
    default_args = default_args,
    description = "Fetch Openwhether with pandas+request in a venv upload to GCS triggering the downstream job",
    schedule_interval=None,
    catchup=False,
    tags=["wheather","gcs"]
) as dag:
    
    #-------------------------------
    #task 1 : Extract data in venv that REUSES system site-packages
    #--------

    def _extract_openwheather(api_key:str)->str:
        import requests
        import pandas as pd

        endpoint = "https://api.openweathermap.org/data/2.5/forecast"
        params = {"q": "Toronto,CA","appid":api_key}
        
        resp = requests.get(endpoint,params=params)
        resp.raise_for_status()

        df = pd.json_normalize(resp.json()["list"])
        return df.to_csv(index=False)

    extract_wheather = PythonVirtualenvOperator(
        task_id = "extract_wheather_data",
        python_callable = _extract_openwheather,
        #install pandas and request but reuse system nupy
        requirements=[
            "pandas==1.5.3",
            "requests==2.31.0",
        ],
        system_site_packages=True,
        op_kwargs = {"api_key":Variable.get("openwheather_api_key")}
    )
    
    def _upload_to_gcs(ds: str,**kwargs):
        ti = kwargs["ti"]
        csv_data = ti.xcom_pull(task_ids="extract_weather_data")
        hook = GCSHook()

        hook.upload(
            bucket_name="weather-data-kds",
            object_name=f"weather/{ds}/forecast.csv",
            data = csv_data,
            mine_type = "text/csv"
        )

    
    upload_to_gcs = PythonOperator(
        task_id = "upload_to_gcs",
        python_callable=_upload_to_gcs,
        op_kwargs={"ds" : "{{ ds }}"}
    )

    #task 3 : trigger donwstream dag
    trigger_transform = TriggerDagRunOperator(
        task_id = "trigger_data_transform_dag",
        trigger_dag_id="transformed_weather_data_to_bq",
        wait_for_completion=False
    )

    #dag dependencey
    extract_wheather >> upload_to_gcs >> trigger_transform