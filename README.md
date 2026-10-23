# Pipeline de Fraude em Cartao de Credito

Pipeline de dados conteinerizada que importa um CSV de transacoes, limpa,
transforma e monitora a qualidade do dado ponta a ponta.

**Stack:** Airflow 3 (orquestracao) + Cosmos (integracao dbt/Airflow) +
dbt (transformacao e testes) + Elementary (observabilidade e deteccao de
anomalia) + Postgres (warehouse), tudo em containers.

## O que a pipeline entrega

| Entregavel | Onde |
|---|---|
| Importacao do CSV | `dags/fraud_credit_ingest.py` (COPY para o Postgres) |
| Limpeza dos dados | `fraud_credit/models/intermediate/int_fraud_credit_cleaned.sql` |
| Regioes por media de `risk_score` (desc) | `fraud_credit/models/marts/mart_average.sql` |
| Top 3 `receiving_address` pela venda mais recente | `fraud_credit/models/marts/mart_top_vendas.sql` |
| Indicadores de qualidade (registros, erros, % conformidade) | `fraud_credit/models/marts/mart_dq_indicadores.sql` |
| Relatorio de problemas e anomalias | `include/elementary/elementary_report_<data>.html` |

Detalhes de data quality estao em [`docs_elementary.md`](docs_elementary.md).

---

## 1. Pre-requisitos

| Item | Versao | Observacao |
|---|---|---|
| Docker | qualquer recente | com Compose v2 |
| Astro CLI | **>= 1.34** | o 1.29 envia comandos do Airflow 2 para um runtime Airflow 3 e o ambiente nao sobe |

```bash
astro version    # validado com 1.45.0
```

## 2. Arquivos locais que voce precisa criar

Tres arquivos ficam **fora do git** e sao obrigatorios. Sem eles o ambiente
sobe, mas as DAGs falham.

### 2.1 `airflow_settings.yaml` — credenciais do warehouse

E a fonte unica de credenciais: o Cosmos a consome nas DAGs de dbt, e a DAG de
relatorio a le para montar as variaveis do `edr`.

```yaml
airflow:
  connections:
    - conn_id: dbt_postgres
      conn_type: postgres
      conn_host: dbt-postgres     # nome do servico no compose
      conn_schema: postgres       # no provider Postgres, "schema" e a DATABASE
      conn_login: postgres
      conn_password: postgres
      conn_port: 5432
      conn_extra:
  pools: []
  variables: []
```

### 2.2 `.env` — timeouts de parse

```bash
AIRFLOW__CORE__TEST_CONNECTION=Enabled
AIRFLOW__CORE__DAGBAG_IMPORT_TIMEOUT=180
AIRFLOW__DAG_PROCESSOR__DAG_FILE_PROCESSOR_TIMEOUT=240
```

Os dois timeouts nao sao opcionais: o Cosmos roda `dbt ls` durante o parse para
montar as tasks a partir do grafo do dbt, e com o default de 30s as DAGs de dbt
nem chegam a aparecer na UI.

### 2.3 O CSV de origem

```bash
mkdir -p include/data
cp /caminho/do/df_fraud_credit.csv include/data/
```

**Tem que ser em `include/data/`.** O `astro dev` monta por bind-mount apenas
`dags/`, `plugins/`, `include/` e `tests/`; o resto do projeto vai para dentro
da imagem. Um CSV de 167MB em `fraud_credit/seeds/` seria recopiado a cada
build, e o GitHub rejeita arquivo acima de 100MB. O diretorio ja esta no
`.gitignore`.

## 3. Subir o ambiente

```bash
astro dev start
```

Sobem 7 containers. Note que sao **dois Postgres diferentes**:

| Container | Papel | Porta no host |
|---|---|---|
| `airflow-dev_*-postgres-1` | metadados do Airflow | 5432 |
| `dbt-postgres` | **warehouse** do dbt/Elementary | **5433** |
| `*-scheduler-1` | agenda e executa tasks | — |
| `*-dag-processor-1` | faz o parse das DAGs | — |
| `*-api-server-1` | UI e API | 8080 |
| `*-triggerer-1` | tasks deferidas | — |

- **UI do Airflow:** http://localhost:8080 (`admin` / `admin`)
- **Warehouse:** `postgresql://postgres:postgres@localhost:5433/postgres`

O warehouse e definido em `docker-compose.override.yml`. O nome do arquivo
importa: o Astro CLI so le `.yml` — com `.yaml` ele ignora em silencio e o
servico nunca sobe.

## 4. Ordem de execucao das DAGs

### 4.1 Uma vez, na primeira instalacao

```
build_elementery        (manual, ~2 min)
```

Cria as estruturas do pacote Elementary (`dbt run --select elementary`).
**Rode antes de tudo**: se as tabelas nao existirem quando o primeiro
`fraud_credit_run` executar, os resultados daquela execucao se perdem.

Depois disso ela sai do caminho. Rode de novo apenas apos um **upgrade do
pacote `elementary`**, porque o schema das tabelas dele muda entre versoes.

Ela nao precisa rodar a cada ciclo: os models do pacote sao `incremental` com
corpo vazio — quem popula sao os hooks `on-run-start`/`on-run-end`, que gravam
direto nas tabelas finais a cada execucao do dbt.

### 4.2 O ciclo de dados

```
    fraud_credit_ingest          disparo MANUAL
            │                    carrega o CSV via COPY  (~7s / 1M linhas)
            │
            │  Asset: dbt://fraud_credit/seed_complete
            ▼
    fraud_credit_run             AUTOMATICO
            │                    models + 65 testes      (~4 min / 1M linhas)
            │
            │  Asset: dbt://fraud_credit/build_complete
            ▼
    elementary_report            AUTOMATICO
                                 gera o HTML             (~50s)
```

**Voce aciona apenas a primeira.** As outras duas reagem por Asset, sem cron:

```bash
astro dev bash -s
airflow dags trigger fraud_credit_ingest
```

Ou pelo botao ▶ na UI.

O gatilho manual e escolha, nao limitacao. Para automatizar o ciclo inteiro,
troque uma linha em `dags/fraud_credit_ingest.py`:

```python
schedule="0 3 * * *",   # no lugar de schedule=None
```

### 4.3 DAG desativada

`fraud_credit_seed` fica **pausada** e foi substituida pela
`fraud_credit_ingest`. Ela usava `dbt seed`, que para este volume levaria
~54 minutos (medido: 326 linhas/s) contra ~7 segundos do `COPY`, alem de
carregar o CSV inteiro em memoria. Nao despause: ela recria a tabela a partir
do CSV e apaga a carga.

## 5. Conferir os resultados

```bash
# tabelas-resultado
docker exec -i dbt-postgres psql -U postgres -d postgres -c "select * from postgres.mart_average;"
docker exec -i dbt-postgres psql -U postgres -d postgres -c "select * from postgres.mart_top_vendas;"

# indicadores de qualidade
docker exec -i dbt-postgres psql -U postgres -d postgres -c "select * from postgres.mart_dq_indicadores;"

# linhas que falharam nos testes
docker exec -i dbt-postgres psql -U postgres -d postgres -c "\dt postgres_dq_failures.*"
```

O relatorio do Elementary sai em `include/elementary/`, que e bind-mount —
o HTML aparece direto no repositorio local. Abra
`elementary_report_latest.html` no navegador.

## 6. Estrutura

```
dags/
  fraud_credit_ingest.py      carga do CSV (COPY)         -> Asset seed_complete
  fraud_credit_run.py         models + testes (Cosmos)    -> Asset build_complete
  dag_elementery_report.py    relatorio HTML (edr)
  project_elementery.py       setup do Elementary (manual)
  fraud_credit_seed.py        OBSOLETA (pausada)

fraud_credit/                 projeto dbt  (vai para a IMAGEM)
  models/staging/             source + contrato de entrada
  models/intermediate/        limpeza e normalizacao
  models/marts/               tabelas-resultado + indicadores de qualidade
  tests/                      testes singulares de regra de negocio

include/
  data/                       CSV de origem   (bind-mount, fora do git)
  elementary/                 relatorio HTML  (bind-mount)
```

## 7. Armadilhas conhecidas

**Editar um model nao tem efeito imediato.** O `astro dev` monta apenas `dags/`,
`plugins/`, `include/` e `tests/`. Como `fraud_credit/` esta na raiz, ele entra
na imagem — mudanca em model ou teste so vale depois de `astro dev restart`.
Para hot-reload, mova o projeto dbt para `include/fraud_credit/` e ajuste
`DEFAULT_DBT_ROOT_PATH` nas DAGs.

**`edr` nao roda no Python do runtime.** O runtime 3.3 usa Python 3.14, e o
`elementary-data` exige `>=3.10,<3.14`. O `Dockerfile` provisiona um CPython
3.13 isolado via `uv` so para esse venv. O **pacote dbt** `elementary` nao tem
essa restricao: e Jinja/SQL, executado pelo dbt.

**`packages.yml` declara `dbt_date` de proposito.** Ele ja e dependencia
transitiva do `dbt_expectations`, mas o Cosmos exclui o `package-lock.yml` ao
montar o projeto temporario do parse; sem o lock, o dbt compara a contagem de
pacotes declarados com as pastas em `dbt_packages/` e aborta.

**`{{ ds }}` nao existe em DAG disparada por Asset.** No Airflow 3 esses runs
nao tem `logical_date`. Use `{{ dag_run.run_after | ds }}`.

## 8. Versoes validadas

| Componente | Versao |
|---|---|
| Astro Runtime | 3.3-7 (Airflow 3, Python 3.14.7) |
| Astro CLI | 1.45.0 |
| dbt-core / dbt-postgres | 1.12.5 / 1.11.0 |
| pacote dbt `elementary` | 0.25.0 |
| CLI `edr` (`elementary-data`) | 0.25.1 (Python 3.13.15) |
| Postgres (warehouse) | 16 |
