"""Gera o relatorio HTML do Elementary (edr report) a partir das tabelas que o
pacote elementary materializa no schema <schema>_elementary.

Pre-requisitos (nesta ordem):
  1. fraud_credit_seed  -> carrega os seeds
  2. build_elementery   -> cria as tabelas do pacote elementary ANTES do run,
                           para que os hooks on-run-start/on-run-end capturem
                           os resultados da execucao seguinte
  3. fraud_credit_run   -> roda models + tests e emite o asset de build
  4. esta DAG           -> le essas tabelas e gera o HTML

O relatorio sai em include/elementary/, que e montado na sua maquina pelo
astro dev, entao o arquivo aparece direto no repositorio local.
"""

import os
from pathlib import Path

from airflow.sdk import Asset, dag
from airflow.providers.standard.operators.bash import BashOperator
from pendulum import datetime

# Asset emitido pela fraud_credit_run quando models + tests terminam. E o
# ultimo elo da cadeia: neste ponto as tabelas do Elementary ja existem (criadas
# pela build_elementery) E ja contem os resultados do run que acabou de rodar.
dbt_build_asset = Asset("dbt://fraud_credit/build_complete")

DEFAULT_DBT_ROOT_PATH = Path(__file__).parent.parent / "fraud_credit"
DBT_ROOT_PATH = Path(os.getenv("DBT_ROOT_PATH", DEFAULT_DBT_ROOT_PATH))

EDR_EXECUTABLE = "/usr/local/airflow/elementary_venv/bin/edr"
REPORT_DIR = "/usr/local/airflow/include/elementary"
# target-path recebe logs e artefatos intermediarios do edr; fica fora do repo.
EDR_TARGET_PATH = "/tmp/edr_target"

# Carimbo de data do relatorio.
#
# NAO use {{ ds }} aqui: esta DAG e disparada por Asset, e no Airflow 3 um run
# disparado por Asset nao tem logical_date — entao "ds" fica indefinido e o
# Jinja do bash_command quebra com UndefinedError ANTES de o edr rodar. A DAG
# falhava sempre no primeiro passo, sem nenhuma mensagem do edr.
# "run_after" existe em qualquer tipo de run; o filtro "ds" o formata como data.
REPORT_DS = "{{ dag_run.run_after | ds }}"

# Conexao Airflow usada pelo Cosmos — reaproveitada aqui para o edr,
# assim host/usuario/senha vivem num lugar so.
DB_CONN_ID = "dbt_postgres"
DBT_SCHEMA = "postgres"


@dag(
    dag_id="elementary_report",
    start_date=datetime(2022, 11, 27),
    schedule=[dbt_build_asset],
    catchup=False,
    tags=["elementary", "dbt", "data-quality"],
    doc_md=__doc__,
)
def elementary_report():

    generate_report = BashOperator(
        task_id="generate_elementary_report",
        env={
            "ELEMENTARY_DB_HOST": f"{{{{ conn.{DB_CONN_ID}.host }}}}",
            "ELEMENTARY_DB_PORT": f"{{{{ conn.{DB_CONN_ID}.port or 5432 }}}}",
            "ELEMENTARY_DB_USER": f"{{{{ conn.{DB_CONN_ID}.login }}}}",
            "ELEMENTARY_DB_PASSWORD": f"{{{{ conn.{DB_CONN_ID}.password }}}}",
            "ELEMENTARY_DB_NAME": f"{{{{ conn.{DB_CONN_ID}.schema }}}}",
            "ELEMENTARY_DB_SCHEMA": DBT_SCHEMA,
            "PATH": "/usr/local/airflow/dbt_venv/bin:/usr/local/bin:/usr/bin:/bin",
        },
        append_env=False,
        bash_command=f"""
            set -euo pipefail

            mkdir -p "{REPORT_DIR}" "{EDR_TARGET_PATH}"

            echo "== edr version =="
            {EDR_EXECUTABLE} --version

            echo "== gerando relatorio =="
            {EDR_EXECUTABLE} report \
                --profiles-dir "{DBT_ROOT_PATH}" \
                --project-dir "{DBT_ROOT_PATH}" \
                --profile-target dev \
                --target-path "{EDR_TARGET_PATH}" \
                --days-back 7 \
                --env dev \
                --file-path "{REPORT_DIR}/elementary_report_{REPORT_DS}.html"

            cp "{REPORT_DIR}/elementary_report_{REPORT_DS}.html" \
               "{REPORT_DIR}/elementary_report_latest.html"

            echo "== relatorio gerado =="
            ls -lh "{REPORT_DIR}"
        """,
    )

    generate_report


elementary_report()
