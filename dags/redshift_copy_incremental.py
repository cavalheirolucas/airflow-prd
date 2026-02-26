from datetime import datetime
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator

S3_BUCKET = "pipeline-clima-transporte"
S3_PREFIX_BASE = "silver/visualcrossing_weather_sp"  # sem / no final

TARGET_DS = "{{ data_interval_start | ds }}"

REDSHIFT_SCHEMA = "silver"
REDSHIFT_TABLE = "clima"

# Coluna de partição na tabela (ajuste se for diferente)
PARTITION_COL = "open_date_col"

# teste workflow
# (RECOMENDADO) Role do Redshift p/ ler S3 (ajuste)
REDSHIFT_IAM_ROLE_ARN = "arn:aws:iam::599942835378:role/service-role/AmazonRedshift-CommandsAccessRole-20260115T194543"

with DAG(
    dag_id="redshift_copy_incremental",
    start_date=datetime(2026, 1, 28),
    schedule="0 10 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["redshift", "copy", "incremental"],
) as dag:

    delete_partition = PostgresOperator(
        task_id="delete_dt_partition",
        postgres_conn_id="redshift_default",
        sql=f"""
        DELETE FROM {REDSHIFT_SCHEMA}.{REDSHIFT_TABLE}
        WHERE {PARTITION_COL} = '{TARGET_DS}'::date;
        """,
    )

    copy_partition = S3ToRedshiftOperator(
    task_id="copy_dt_partition",
    redshift_conn_id="redshift_default",
    aws_conn_id="aws_default",
    schema=REDSHIFT_SCHEMA,
    table=REDSHIFT_TABLE,
    s3_bucket=S3_BUCKET,
    s3_key=f"{S3_PREFIX_BASE}/open_date={TARGET_DS}/",
    copy_options=[
        "FORMAT AS PARQUET",
        "COMPUPDATE OFF",
        "STATUPDATE OFF",
    ],
    method="APPEND",
)


    delete_partition >> copy_partition
