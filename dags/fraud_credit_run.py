import os
from pathlib import Path

from airflow.sdk import Asset, dag # Sintaxe Airflow 3.0
from airflow.providers.standard.operators.empty import EmptyOperator
from cosmos import (
    DbtTaskGroup,
    ProjectConfig,
    ProfileConfig,
    ExecutionConfig,
    RenderConfig,
    SourceRenderingBehavior,
)
from cosmos.profiles import PostgresUserPasswordProfileMapping
from pendulum import datetime

DEFAULT_DBT_ROOT_PATH = Path(__file__).parent.parent / "fraud_credit"
DBT_ROOT_PATH = Path(os.getenv("DBT_ROOT_PATH", DEFAULT_DBT_ROOT_PATH))

# Disparada assim que a carga do CSV termina (emitido pela fraud_credit_ingest).
#
# A build_elementery NAO fica mais no meio do caminho. Ela so CRIA as estruturas
# do pacote Elementary: os models dele sao "incremental" com corpo vazio
# (get_dbt_models_empty_table_query), e quem popula sao os hooks
# on-run-start/on-run-end, que gravam direto nas tabelas finais
# (ver upload_run_results: insere em dbt_run_results sem area intermediaria).
# Logo, com as tabelas ja criadas, rodar "dbt run --select elementary" a cada
# ciclo nao acrescenta dado nenhum — so custava ~2 min por execucao.
# A build_elementery virou DAG de setup, manual: rode na instalacao e depois de
# cada upgrade do pacote, quando o schema das tabelas dele pode mudar.
dbt_seed_asset = Asset("dbt://fraud_credit/seed_complete")
# Emitido quando models + tests terminam; consumido pela elementary_report.
dbt_build_asset = Asset("dbt://fraud_credit/build_complete")

profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="dbt_postgres",
        profile_args={"schema": "postgres"},
    )
)

@dag(
    dag_id="fraud_credit_run",
    start_date=datetime(2022, 11, 27),
    # 2. Agenda com base na atualização do Asset
    schedule=[dbt_seed_asset],
    catchup=False,
)
def fraud_run_dag():
    
    pre_dbt_workflow = EmptyOperator(task_id="pre_dbt_workflow")

    fraud_credit_models = DbtTaskGroup(
        group_id="fraud_credit_models",
        project_config=ProjectConfig(
            (DBT_ROOT_PATH).as_posix(),
            # Os pacotes dbt ja vem resolvidos na imagem (ver Dockerfile), entao
            # nao ha motivo para rodar "dbt deps" no parse nem em cada task.
            install_dbt_deps=False,
        ),
        execution_config=ExecutionConfig(
            dbt_executable_path="/usr/local/airflow/dbt_venv/bin/dbt"
        ),
        render_config=RenderConfig(
            # "source:raw+" em vez de "stg_raw_df_fraud_credit+": a selecao
            # antiga comecava no staging e deixava de fora os testes definidos
            # na SOURCE, que sao o contrato de entrada do pipeline.
            select=["source:raw+"],
            # Sem isto o Cosmos nao cria task nenhuma para nodes de source
            # (o padrao e NONE desde o Cosmos 1.6), e os testes de source
            # simplesmente nao rodariam.
            source_rendering_behavior=SourceRenderingBehavior.WITH_TESTS_OR_FRESHNESS,
        ),
        profile_config=profile_config,
        default_args={"retries": 2},
    )

    post_dbt_workflow = EmptyOperator(task_id="post_dbt_workflow", outlets=[dbt_build_asset])

    pre_dbt_workflow >> fraud_credit_models >> post_dbt_workflow

fraud_run_dag()
