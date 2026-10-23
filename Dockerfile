FROM astrocrpublic.azurecr.io/runtime:3.3-7

# venv do dbt (usado pelo Cosmos via ExecutionConfig)
RUN python3 -m venv dbt_venv && source dbt_venv/bin/activate && \
    pip install --no-cache-dir -r dbt-requirements.txt && deactivate

# Pacotes dbt (elementary, dbt_utils, dbt_expectations) resolvidos ja na imagem.
# Sem isto o Cosmos precisa rodar "dbt deps" a cada parse de DAG e no inicio de
# cada task — era o que fazia o import das DAGs de dbt estourar o timeout do
# DagBag e elas nem aparecerem na UI.
RUN source dbt_venv/bin/activate && \
    dbt deps --project-dir fraud_credit --profiles-dir fraud_credit && deactivate

# venv do Elementary CLI (edr) — isolado do dbt para nao conflitar dependencias.
#
# Por que uv e nao "python3 -m venv": este runtime roda Python 3.14, e nenhuma
# versao do elementary-data suporta 3.14 (a mais nova, 0.26.0, exige
# >=3.10,<3.14). O uv provisiona um CPython 3.13 isolado so para este venv,
# sem tocar no Python do Airflow.
RUN pip install --no-cache-dir uv && \
    ~/.local/bin/uv venv --python 3.13 elementary_venv && \
    ~/.local/bin/uv pip install --no-cache \
        --python /usr/local/airflow/elementary_venv/bin/python \
        -r elementary-requirements.txt

RUN pip install --no-cache-dir -r requirements.txt
