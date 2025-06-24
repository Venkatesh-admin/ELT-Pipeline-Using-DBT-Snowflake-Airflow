from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.utils.dates import days_ago
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping
from airflow.operators.dummy_operator import DummyOperator
import os
import logging

default_args = {
    'owner': 'airflow',
}


profile_config = ProfileConfig(
    profile_name='netflix',
    target_name='Dev',
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id='snowflake_con',
        profile_args={"database": "movielens", "schema": "Dev"},  # Default to SYSADMIN if not set
    )
)

execution_config = ExecutionConfig(
    dbt_executable_path="/usr/local/airflow/dbt_venv/bin/dbt",
)

with DAG(
    dag_id='ELT_pipeline_movielens',
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=['snowflake','dbt'],
) as dag:


    start_task = DummyOperator(
        task_id='start_task',
        dag=dag,
    )
    copy_into_movies = SnowflakeOperator(
    task_id='raw_movies',
    sql="""
        COPY INTO movielens.raw.raw_movies
        FROM @movielens.raw.netflix_stage/movies.csv
        ;
    """,
    snowflake_conn_id='snowflake_con',
    )

    copy_into_tags = SnowflakeOperator(
        task_id='raw_tags',
        sql="""
            COPY INTO movielens.raw.raw_tags
            FROM @movielens.raw.netflix_stage/tags.csv
            ON_ERROR = 'CONTINUE';
        """,
        snowflake_conn_id='snowflake_con',
    )

    copy_into_ratings = SnowflakeOperator(
        task_id='raw_ratings',
        sql="""
            COPY INTO movielens.raw.raw_ratings
            FROM @movielens.raw.netflix_stage/ratings.csv
            ;
        """,
        snowflake_conn_id='snowflake_con',
    )

    copy_into_links = SnowflakeOperator(
        task_id='raw_links',
        sql="""
            COPY INTO movielens.raw.raw_links
            FROM @movielens.raw.netflix_stage/links.csv
            ;
        """,
        snowflake_conn_id='snowflake_con',
    )

    copy_into_genome_scores = SnowflakeOperator(
        task_id='raw_genome_scores',
        sql="""
            COPY INTO movielens.raw.raw_genome_scores
            FROM @movielens.raw.netflix_stage/genome-scores.csv
            ;
        """,
        snowflake_conn_id='snowflake_con',
    )

    copy_into_genome_tags = SnowflakeOperator(
        task_id='raw_genome_tags',
        sql="""
            COPY INTO movielens.raw.raw_genome_tags
            FROM @movielens.raw.netflix_stage/genome-tags.csv
            ;
        """,
        snowflake_conn_id='snowflake_con',
    )

    transform_data = DbtTaskGroup(
        group_id="transform_data",
        project_config=ProjectConfig("/usr/local/airflow/dags/netflix"),
        profile_config=profile_config,
        execution_config=execution_config,
        default_args={"retries": 2},
    )
    end_task = DummyOperator(
        task_id='end_task',
        dag=dag
    )
    start_task >> [copy_into_movies, copy_into_tags, copy_into_ratings, copy_into_links, copy_into_genome_scores, copy_into_genome_tags] >> transform_data >> end_task