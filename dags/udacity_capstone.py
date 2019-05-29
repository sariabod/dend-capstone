import os
from datetime import datetime, timedelta
import logging
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import Row
from airflow.models import Variable

data_path = Variable.get("s3_data")
os.environ["AWS_ACCESS_KEY_ID"] = Variable.get("aws_key")
os.environ["AWS_SECRET_ACCESS_KEY"] = Variable.get("aws_secret")

default_args = {
    "owner": "udacity",
    "depends_on_past": False,
    "start_date": datetime(2013, 1, 1),
    "email": ["sariabod@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "catchup_by_default": False,
    "end_date": datetime(2016, 12, 31)
}


dag = DAG(
    "udacity_captone",
    default_args=default_args,
    description="Load and transform data in Redshift with Airflow",
    schedule_interval="@daily",
    max_active_runs=10,
)


def create_spark_session():
    """ Create a Spark Session for Tasks """
    spark = (
        SparkSession.builder.config(
            "spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0"
        )
        .appName("sparkstand")
        .getOrCreate()
    )
    logging.info("Spark Session Created")
    return spark


def load_temperature(**kwargs):
    """ Load a days worth of temperture data """
    task_instance = kwargs["task_instance"]
    working_day = kwargs["execution_date"].date()
    spark = create_spark_session()
    temperature_data = "{}/temperature/{}/{}/{}/*.json".format(
        data_path, working_day.year, working_day.month, working_day.day
    )
    df = spark.read.json(temperature_data)
    temperature = df.agg({"Los Angeles": "avg"}).collect()[0][0]
    logging.info("Temperature is: {}".format(temperature))
    task_instance.xcom_push(key="temperature", value=temperature)


def check_temperature(**kwargs):
    """ Check to make sure temperature is greater then zero """
    task_instance = kwargs["task_instance"]
    temperature = task_instance.xcom_pull(
        task_ids="load_temperature", key="temperature"
    )
    # lets make sure temp is greater then 0
    if temperature <= 0:
        raise ValueError("Temperature is less then or equal to zero")


def load_crimes(**kwargs):
    """ Load a days worth of crime data """
    task_instance = kwargs["task_instance"]
    working_day = kwargs["execution_date"].date()
    spark = create_spark_session()
    crime_data = "{}/crimes/{}/{}/{}/*.csv".format(
        data_path,
        working_day.year,
        str(working_day.month).zfill(2),
        str(working_day.day).zfill(2),
    )
    df = spark.read.csv(crime_data, header=True)
    unique_count = df.select("Date Occurred").distinct().count()
    crime_count = df.count()
    logging.info("Crime Count is: {}".format(crime_count))
    task_instance.xcom_push(key="crime_count", value=crime_count)
    task_instance.xcom_push(key="unique_count", value=unique_count)


def check_crimes(**kwargs):
    """ Check data values to make sure extra days did not get mixed in """
    task_instance = kwargs["task_instance"]
    crime_count = task_instance.xcom_pull(task_ids="load_crimes", key="crime_count")
    unique_count = task_instance.xcom_pull(task_ids="load_crimes", key="unique_count")
    # lets make sure temp is greater then 0
    if unique_count > 1:
        raise ValueError("Too many dates being processed")


def save_dataframe(**kwargs):
    """ Create an append a days data to a parquet structure on S3 to be queried later """
    task_instance = kwargs["task_instance"]
    target_day = kwargs["ds"]
    working_day = kwargs["execution_date"].date()
    spark = create_spark_session()
    crime_count = task_instance.xcom_pull(task_ids="load_crimes", key="crime_count")
    temperature = task_instance.xcom_pull(
        task_ids="load_temperature", key="temperature"
    )
    logging.info("Creating Dataframe With Date: {}".format(working_day))
    Output = Row("date", "crimes", "temperature", "year", "month", "day")
    output = Output(
        target_day,
        crime_count,
        temperature,
        working_day.year,
        working_day.month,
        working_day.day,
    )
    dframe = spark.createDataFrame([output])
    dframe = dframe.withColumn(
        "date", F.to_date(dframe.date.cast(dataType=T.TimestampType()))
    )
    dframe.write.partitionBy("year", "month", "day").parquet(
        "{}/results".format(data_path), mode="append"
    )
    logging.info("Finished Saving Dataframe")


# Begin Operator Creation

start_operator = DummyOperator(task_id="begin_execution", dag=dag)

load_temperature_operator = PythonOperator(
    task_id="load_temperature",
    python_callable=load_temperature,
    provide_context=True,
    dag=dag,
)

load_crimes_operator = PythonOperator(
    task_id="load_crimes", python_callable=load_crimes, provide_context=True, dag=dag
)

check_crimes_operator = PythonOperator(
    task_id="check_crimes", python_callable=check_crimes, provide_context=True, dag=dag
)

check_temperature_operator = PythonOperator(
    task_id="check_temperature",
    python_callable=check_temperature,
    provide_context=True,
    dag=dag,
)

save_dataframe_operator = PythonOperator(
    task_id="save_dataframe",
    python_callable=save_dataframe,
    provide_context=True,
    dag=dag,
)

end_operator = DummyOperator(task_id="end_execution", dag=dag)


start_operator >> [load_temperature_operator, load_crimes_operator]
load_crimes_operator >> check_crimes_operator
load_temperature_operator >> check_temperature_operator
check_crimes_operator >> save_dataframe_operator
check_temperature_operator >> save_dataframe_operator
save_dataframe_operator >> end_operator
