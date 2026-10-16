from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

def check_training_environment():
    import pandas as pd
    from pyspark.sql import SparkSession

    print("Pandas version:", pd.__version__)

    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("AirflowTrainingSmokeTest")
        .getOrCreate()
    )
    try:
        result = spark.createDataFrame(
            [(501, 399.00), (502, 175.50)],
            ["customer_id", "revenue"],
        )
        result.show()
        assert result.count() == 2
        print("Airflow + Python + Spark: OK")
    finally:
        spark.stop()

with DAG(
    dag_id="training_environment_smoke_test",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["training", "smoke-test"],
) as dag:
    check_environment = PythonOperator(
        task_id="check_python_pandas_spark",
        python_callable=check_training_environment,
    )