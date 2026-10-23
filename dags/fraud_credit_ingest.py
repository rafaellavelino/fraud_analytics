"""Ingestao do CSV bruto de transacoes para o Postgres, via COPY.

Substitui a `fraud_credit_seed` (que usava `dbt seed`). O motivo e de escala,
medido neste projeto com o arquivo real de 1.048.575 linhas:

| Metodo      | Tempo    | Observacao                                     |
|-------------|----------|------------------------------------------------|
| `dbt seed`  | ~54 min  | 326 linhas/s; carrega o CSV inteiro em memoria |
| `COPY`      | ~5 s     | streaming, memoria constante                   |

O `dbt seed` monta uma tabela `agate` com o arquivo todo antes de emitir os
INSERTs — com 167MB de texto isso vira alguns GB de RAM, e o modo de falha
deixa de ser lentidao e passa a ser o container morrer por OOM. O `dbt seed`
foi feito para tabela de dominio (lista de paises, de-para), nao para carga.

Nada muda daqui para baixo na cadeia: os models nunca leram o seed, eles leem
uma SOURCE (`source('raw','df_fraud_credit')`). Esta DAG apenas popula a mesma
tabela que o seed populava, e emite o mesmo Asset.

O CSV vive em `include/data/`, que o `astro dev` monta por bind-mount. E de
proposito: `fraud_credit/` entra na IMAGEM, entao um arquivo de 167MB ali
somaria 167MB a cada build — e o git rejeita arquivo acima de 100MB.
"""

import os
import time
from pathlib import Path

from airflow.sdk import Asset, dag, task
from pendulum import datetime

# Mesmo Asset que a antiga fraud_credit_seed emitia: a cadeia downstream
# (build_elementery -> fraud_credit_run -> elementary_report) segue intacta.
dbt_seed_asset = Asset("dbt://fraud_credit/seed_complete")

CSV_PATH = Path(
    os.getenv("FRAUD_CREDIT_CSV", "/usr/local/airflow/include/data/df_fraud_credit.csv")
)

DB_CONN_ID = "dbt_postgres"
TARGET_SCHEMA = "postgres"
TARGET_TABLE = "df_fraud_credit"
FQN = f"{TARGET_SCHEMA}.{TARGET_TABLE}"

# Ordem identica a do cabecalho do CSV — o COPY casa por POSICAO, nao por nome.
COLUMNS = [
    ("timestamp", "integer"),
    ("sending_address", "text"),
    ("receiving_address", "text"),
    ("amount", "text"),
    ("transaction_type", "text"),
    ("location_region", "text"),
    ("ip_prefix", "double precision"),
    ("login_frequency", "integer"),
    ("session_duration", "integer"),
    ("purchase_pattern", "text"),
    ("age_group", "text"),
    ("risk_score", "text"),
    ("anomaly", "text"),
]

# amount e risk_score ficam como TEXT de proposito: a origem usa o sentinela
# 'none' nessas duas colunas, e um cast na ingestao abortaria a carga inteira.
# A conversao acontece em int_fraud_credit_cleaned, com nullif().


@dag(
    dag_id="fraud_credit_ingest",
    start_date=datetime(2022, 11, 27),
    # Manual por escolha, como a seed que ela substitui. Para automatizar a
    # cadeia toda basta trocar por um cron aqui: as outras DAGs ja reagem
    # sozinhas, por Asset.
    schedule=None,
    catchup=False,
    # UMA execucao por vez, obrigatoriamente. Esta e uma carga FULL:
    # truncate + copy na mesma tabela. Duas execucoes simultaneas se atropelam
    # de duas formas:
    #
    # 1. "create schema if not exists" NAO e atomico no Postgres. As duas
    #    transacoes veem o schema como ausente, as duas tentam criar, e a
    #    segunda falha com
    #      UniqueViolation: duplicate key value violates unique constraint
    #      "pg_namespace_nspname_index"
    #    Foi exatamente o que aconteceu em 2026-09-21, com dois triggers
    #    manuais a 13 segundos de distancia.
    #
    # 2. Pior que a falha visivel: se o schema ja existisse, as duas passariam
    #    do DDL e disputariam o truncate/copy. Uma poderia truncar a tabela no
    #    meio do copy da outra, e o resultado seria uma carga silenciosamente
    #    incompleta — com a DAG verde.
    #
    # Falhar rapido no segundo trigger e o comportamento correto aqui.
    max_active_runs=1,
    tags=["ingestao", "csv", "postgres"],
    doc_md=__doc__,
)
def fraud_credit_ingest():

    @task(outlets=[dbt_seed_asset])
    def copy_csv_to_postgres() -> dict:
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        if not CSV_PATH.exists():
            raise FileNotFoundError(
                f"CSV nao encontrado em {CSV_PATH}. O arquivo precisa estar em "
                "include/data/ — esse diretorio e bind-mount do astro dev, "
                "entao o container o enxerga sem rebuild da imagem."
            )

        tamanho_mb = CSV_PATH.stat().st_size / 1024 / 1024
        print(f"Origem: {CSV_PATH} ({tamanho_mb:.1f} MB)")

        hook = PostgresHook(postgres_conn_id=DB_CONN_ID)

        colunas_ddl = ",\n    ".join(f'"{nome}" {tipo}' for nome, tipo in COLUMNS)
        colunas_lista = ", ".join(f'"{nome}"' for nome, _ in COLUMNS)

        # TRUNCATE, e nao DROP: preserva a identidade da tabela para o
        # Elementary, que rastreia schema por objeto. Carga full — a origem e um
        # arquivo fechado, nao um fluxo incremental.
        hook.run(
            [
                f"create schema if not exists {TARGET_SCHEMA};",
                f"create table if not exists {FQN} (\n    {colunas_ddl}\n);",
                f"truncate table {FQN};",
            ]
        )

        # copy_expert DO HOOK, nao do cursor: este runtime usa psycopg 3, onde
        # o cursor nao tem mais copy_expert (virou o context manager
        # cur.copy(sql)). O metodo do hook cobre os dois drivers e faz o
        # streaming do arquivo em blocos — a memoria fica constante,
        # independente do tamanho do CSV.
        inicio = time.monotonic()
        hook.copy_expert(
            f"copy {FQN} ({colunas_lista}) from stdin with (format csv, header true)",
            str(CSV_PATH),
        )
        duracao = time.monotonic() - inicio

        linhas = hook.get_first(f"select count(*) from {FQN};")[0]

        taxa = linhas / duracao if duracao else 0
        print(f"Carregadas {linhas:,} linhas em {duracao:.1f}s ({taxa:,.0f} linhas/s)")

        if linhas == 0:
            raise ValueError("A carga terminou com 0 linhas — verifique o CSV.")

        return {"linhas": linhas, "segundos": round(duracao, 1), "mb": round(tamanho_mb, 1)}

    copy_csv_to_postgres()


fraud_credit_ingest()
