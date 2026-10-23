import os
from pathlib import Path

from airflow.sdk import Asset, dag # Sintaxe Airflow 3.0
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping
from pendulum import datetime

DEFAULT_DBT_ROOT_PATH = Path(__file__).parent.parent /  "fraud_credit"
DBT_ROOT_PATH = Path(os.getenv("DBT_ROOT_PATH", DEFAULT_DBT_ROOT_PATH))

# 1. Definir o Asset com uma URI válida para o dbt
dbt_seed_asset = Asset("dbt://fraud_credit/seed_complete")

profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="dbt_postgres",
        profile_args={"schema": "postgres"},
    )
)

@dag(
    dag_id="fraud_credit_seed",
    start_date=datetime(2022, 11, 27),
    schedule=None,
    catchup=False,
)
def fraud_seed_dag():
    
    DbtTaskGroup(
        group_id="dbt_seed_execution",
        project_config=ProjectConfig(
            (DBT_ROOT_PATH).as_posix(),
            # Os pacotes dbt ja vem resolvidos na imagem (ver Dockerfile), entao
            # nao ha motivo para rodar "dbt deps" no parse nem em cada task.
            install_dbt_deps=False,
        ),
        execution_config=ExecutionConfig(
            dbt_executable_path="/usr/local/airflow/dbt_venv/bin/dbt"
        ),
        render_config=RenderConfig(select=["path:seeds"]),
        operator_args={
            # Injeta o Asset no outlet do grupo do dbt
            "outlets": [dbt_seed_asset],
        },
        profile_config=profile_config,
        default_args={"retries": 2},
    )

fraud_seed_dag()
