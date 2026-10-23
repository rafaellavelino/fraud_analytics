import os
from pathlib import Path

from airflow.sdk import Asset, dag # Sintaxe Airflow 3.0
from airflow.providers.standard.operators.empty import EmptyOperator
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping
from pendulum import datetime

DEFAULT_DBT_ROOT_PATH = Path(__file__).parent.parent / "fraud_credit"
DBT_ROOT_PATH = Path(os.getenv("DBT_ROOT_PATH", DEFAULT_DBT_ROOT_PATH))

# DAG DE SETUP, nao de ciclo. Rode manualmente:
#   - na primeira instalacao do projeto;
#   - depois de todo upgrade do pacote elementary (o schema das tabelas dele
#     pode mudar entre versoes).
#
# Ela executa "dbt run --select elementary", que apenas CRIA as estruturas: os
# models do pacote sao "incremental" com corpo vazio
# (get_dbt_models_empty_table_query). Quem popula sao os hooks
# on-run-start/on-run-end, que gravam direto nas tabelas finais a cada execucao
# do dbt do projeto — ver upload_run_results, que insere em dbt_run_results sem
# passar por area intermediaria.
#
# Por isso ela saiu da cadeia: com as tabelas criadas, roda-la a cada ciclo nao
# acrescentava dado, so somava ~2 min. O que continua valendo e a exigencia de
# que as tabelas EXISTAM antes do primeiro fraud_credit_run — do contrario os
# resultados daquela execucao se perdem.
elementary_ready_asset = Asset("dbt://fraud_credit/elementary_ready")


profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="dbt_postgres",
        profile_args={"schema": "postgres"},
    )
)

@dag(
    dag_id="build_elementery",
    start_date=datetime(2022, 11, 27),
    # Manual: e setup, nao faz parte do ciclo de dados.
    schedule=None,
    catchup=False,
)
def fraud_run_dag():
    
    pre_dbt_workflow = EmptyOperator(task_id="pre_dbt_workflow")

    fraud_credit_models = DbtTaskGroup(
        group_id="elementery_models",
        project_config=ProjectConfig(
            (DBT_ROOT_PATH).as_posix(),
            # Os pacotes dbt ja vem resolvidos na imagem (ver Dockerfile), entao
            # nao ha motivo para rodar "dbt deps" no parse nem em cada task.
            install_dbt_deps=False,
        ),
        execution_config=ExecutionConfig(
            dbt_executable_path="/usr/local/airflow/dbt_venv/bin/dbt"
        ),
        render_config=RenderConfig(select=["elementary"]),
        profile_config=profile_config,
        default_args={"retries": 2},
    )

    post_dbt_workflow = EmptyOperator(task_id="post_dbt_workflow", outlets=[elementary_ready_asset])

    pre_dbt_workflow >> fraud_credit_models >> post_dbt_workflow

fraud_run_dag()
