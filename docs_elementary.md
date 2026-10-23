# Data Quality no Airflow (dev local)

Stack: **dbt** (transformacao + testes) + **Elementary** (observabilidade e
deteccao de anomalia) + **Cosmos** (orquestracao no Airflow 3).

## Os entregaveis do desafio

| O que o desafio pede | Onde esta |
|---|---|
| Importacao de um CSV para processamento | `fraud_credit/seeds/` carregado pela DAG `fraud_credit_seed` (`dbt seed`) |
| Limpeza dos dados importados | `models/intermediate/int_fraud_credit_cleaned.sql` |
| Tabela-resultado: `location_region` por media de `risk_score`, decrescente | `models/marts/mart_average.sql` |
| Tabela-resultado: top 3 `receiving_address` pela venda mais recente de cada | `models/marts/mart_top_vendas.sql` |
| Orquestracao automatizada | Airflow 3 + Cosmos; 4 DAGs encadeadas por Asset (abaixo). Gatilho da primeira e manual por escolha — ver nota |
| Conteinerizacao | `Dockerfile` + `docker-compose.override.yml`; sobe inteiro com `astro dev start` |
| Indicadores e metricas de qualidade | `models/marts/mart_dq_indicadores.sql` |
| Reporte de problemas e anomalias | testes dbt (`store_failures`) + relatorio Elementary em `include/elementary/` |

As tres camadas de data quality respondem perguntas diferentes, e e por isso
que coexistem:

| Camada | Pergunta que responde | Formato |
|---|---|---|
| Testes dbt | passou ou quebrou? | booleano, por regra, com as linhas culpadas gravadas |
| `mart_dq_indicadores` | qual o tamanho do problema? | qtd. registros, qtd. erros, % de conformidade |
| Elementary | mudou em relacao ao historico? | anomalia de volume, dimensao, coluna e schema |

## Cadeia de DAGs

```
fraud_credit_ingest --(dbt://fraud_credit/seed_complete)-->
fraud_credit_run    --(dbt://fraud_credit/build_complete)-->
elementary_report   --> include/elementary/elementary_report_<data>.html

build_elementery    (fora da cadeia: DAG de setup, manual)
```

Cada DAG e disparada por Asset, entao basta acionar a `fraud_credit_ingest`
manualmente que as outras duas rodam em sequencia.

**O gatilho da cadeia e manual por escolha, nao por falta.** A
`fraud_credit_ingest` esta com `schedule=None` para que a pipeline rode sob
demanda durante a demonstracao, sem execucoes automaticas no meio do caminho.
As outras duas ja sao automaticas: nenhuma tem cron, todas reagem ao Asset da
anterior — o encadeamento e que e a orquestracao, nao o relogio. Para ligar a
execucao periodica basta uma linha na DAG do seed:

```python
schedule="0 3 * * *",   # no lugar de schedule=None
```

A partir dai a cadeia inteira roda sozinha, porque a propagacao por Asset ja
esta montada.

**Por que a `build_elementery` esta fora da cadeia.** Ela so CRIA as estruturas
do pacote Elementary. Os models do pacote sao `incremental` com corpo vazio
(`get_dbt_models_empty_table_query`), e quem os popula sao os hooks
`on-run-start`/`on-run-end`, que gravam **direto nas tabelas finais** — o
`upload_run_results` insere em `dbt_run_results` sem area intermediaria nem
consolidacao posterior.

Consequencia pratica: com as tabelas ja criadas, rodar `dbt run --select
elementary` a cada ciclo nao acrescenta dado nenhum, so soma ~2 min por
execucao. Por isso ela virou DAG de setup, disparada manualmente:

- na primeira instalacao do projeto;
- depois de todo upgrade do pacote `elementary`, quando o schema das tabelas
  dele pode mudar entre versoes.

O que continua valendo e a exigencia de que as tabelas **existam** antes do
primeiro `fraud_credit_run`: se nao existirem, os resultados daquela execucao
se perdem e o relatorio so enxerga o ciclo seguinte.

## O que cada peca faz

| Peca | Papel |
|---|---|
| `fraud_credit/packages.yml` | pacotes dbt: `elementary` 0.25.0, `dbt_utils` 1.3.3, `dbt_expectations` 0.10.10 |
| `elementary-requirements.txt` | CLI `edr` 0.25.1, instalado no venv `elementary_venv` |
| `fraud_credit/dbt_project.yml` | `elementary: +schema: elementary`, `store_failures`, flag de override de materializacao |
| `fraud_credit/profiles.yml` | profiles `fraud_credit` e `elementary`, parametrizados por env var |
| `dags/dag_elementery_report.py` | roda `edr report` e salva o HTML em `include/elementary/` |

A versao do CLI `edr` tem que ser da mesma minor do pacote dbt (0.25.x com 0.25.x).

## Infra local

```bash
astro dev start
```

Sobem dois Postgres, com papeis diferentes:

| Container | Papel | Porta no host |
|---|---|---|
| `airflow-dev_*-postgres-1` | metadados do Airflow | 5432 |
| `dbt-postgres` | **warehouse** do dbt/Elementary | 5433 |

O warehouse e definido em `docker-compose.override.yml`. Dois detalhes que
fazem esse arquivo funcionar (e que custaram debug):

- o nome tem que ser `docker-compose.override.yml`. Com `.yaml` o Astro CLI
  ignora o arquivo em silencio e o servico nunca sobe;
- o servico precisa de `networks: [airflow]`. O `astro dev` cria uma rede
  propria e so anexa os servicos dele; um servico declarado no override cai na
  rede `default`, e o Airflow responde
  `could not translate host name "dbt-postgres"`.

A conexao `dbt_postgres` sobe junto com o ambiente, via `airflow_settings.yaml`
(arquivo local, fora do git por conter senha). E a fonte unica de credenciais:
o Cosmos a consome pelo `PostgresUserPasswordProfileMapping` e a DAG de
relatorio a le para montar as env vars do `edr`.

### Requisitos de versao

| Item | Restricao | Por que |
|---|---|---|
| Astro CLI | **>= 1.34** | O CLI 1.29 manda comandos do Airflow 2 (`airflow users`, `airflow webserver`) para um runtime Airflow 3, e scheduler/webserver morrem no boot |
| `edr` (Elementary CLI) | **Python 3.10–3.13** | O runtime 3.3 roda Python 3.14, que o `elementary-data` ainda nao suporta. O Dockerfile usa `uv` para provisionar um CPython 3.13 so para esse venv |
| dbt | **>= 1.10** | Os testes usam a sintaxe `arguments:` para parametros de teste generico |

### `packages.yml` declara uma dependencia transitiva de proposito

`godatadriven/dbt_date` e dependencia do `dbt_expectations`, mas esta listada
explicitamente em `packages.yml`. Nao e redundancia:

o Cosmos monta o `dbt ls` do parse num diretorio temporario e **exclui o
`package-lock.yml`** (ver `create_symlinks` em `cosmos/dbt/project.py`). Sem o
lock, o dbt compara o numero de pacotes declarados em `packages.yml` com o
numero de pastas em `dbt_packages/` e aborta:

```
dbt expects 3 package(s) based on packages specified in packages.yml,
but found only 4 package(s) installed in dbt_packages.
```

Declarando o `dbt_date`, as contas fecham em 4 dos dois lados.

### Variaveis em `.env`

```
AIRFLOW__CORE__TEST_CONNECTION=Enabled          # botao "Test" na tela de conexoes
AIRFLOW__CORE__DAGBAG_IMPORT_TIMEOUT=180        # o parse do Cosmos roda "dbt ls"
AIRFLOW__DAG_PROCESSOR__DAG_FILE_PROCESSOR_TIMEOUT=240
```

## A cobertura de data quality

### Testes de schema, por camada

A severidade segue a camada: a origem e suja por natureza e sai em `warn`; o
que chega no mart e contrato e sai em `error`.

| Camada | Arquivo | O que cobre |
|---|---|---|
| source | `models/staging/__raw_sources.yaml` | `not_null` nas chaves, `accepted_values` nos categoricos |
| staging | `models/staging/__raw_models.yaml` | `schema_changes` (contrato da origem) |
| intermediate | `models/intermediate/__intermediate_models.yml` | nulos, faixas (`accepted_range`), anomalias Elementary |
| marts | `models/marts/__marts_models.yml` | `unique`, `not_null`, contagem de linhas esperada |

### Testes singulares (regra de negocio)

Em `fraud_credit/tests/`. Sao os que pegam defeito que nenhum teste de coluna
isolada enxerga, porque cada coluna esta dentro da faixa dela:

- `assert_risk_score_matches_anomaly_status.sql` — o rotulo tem que ser coerente
  com o score (`<60` low, `60–90` moderate, `>=90` high; faixas disjuntas
  verificadas na base).
- `assert_fraud_transactions_are_high_risk.sql` — transacao `phishing`/`scam`
  tem que estar classificada como `high_risk`.

### Deteccao de anomalia (Elementary)

| Teste | Onde | Precisa de historico? |
|---|---|---|
| `schema_changes` | staging, intermediate, marts | nao — compara com o ultimo schema conhecido |
| `volume_anomalies` | intermediate (bucket por `transaction_at`), marts | no intermediate nao: a base ja tem 4 anos de historico |
| `dimension_anomalies` | intermediate, por `transaction_type` e `location_region` | idem |
| `column_anomalies` | `amount` e `risk_score` | idem |

Todos estao em `severity: warn` de proposito: deteccao de anomalia alerta, nao
barra deploy. Os testes que representam contrato e que sao `error`.

Por que a maior parte da deteccao esta no **intermediate**: e a unica camada com
uma coluna de tempo utilizavel (`transaction_at`, de 2020-01 a 2024-01), o que
da ao Elementary uma serie historica real para bucketizar por dia. Nos marts os
testes so podem agrupar por horario de execucao, entao so acusam algo depois de
acumular varios dias de runs.

Foi por isso tambem que `int_fraud_credit_cleaned` deixou de ser `ephemeral` e
virou `view`: model ephemeral nao existe no banco, e o Elementary nao mede o que
nao tem relacao fisica.

### Indicadores de qualidade (`mart_dq_indicadores`)

Os testes dizem se quebrou; este model diz o **tamanho** do problema. Uma linha
por regra de qualidade, com `qtd_registros`, `qtd_erros`, `pct_erro` e
`pct_conformidade`, mais duas linhas consolidadas.

A medicao e feita sobre a camada **raw** de proposito: e o dado como chegou do
CSV, antes da limpeza. Medir depois da limpeza daria conformidade alta
justamente porque a limpeza corrigiu — o indicador mediria o proprio conserto,
nao a qualidade da origem.

Resultado na base atual (3.168 registros):

| Regra | Erros | Conformidade |
|---|---|---|
| TOTAL — linhas com defeito de conteudo | 62 | 98,04% |
| TOTAL — linhas com qualquer defeito (inclui formatacao) | 3.168 | 0,00% |
| sufixo `;;;` presente no valor | 3.168 | 0,00% |
| `risk_score` ausente (sentinela `'none'`) | 22 | 99,31% |
| `location_region` fora do dominio (placeholder `'0'`) | 21 | 99,34% |
| `amount` ausente (sentinela `'none'`) | 20 | 99,37% |
| as outras 8 regras | 0 | 100% |

**Por que dois totais.** O sufixo `;;;` atinge 100% das linhas: e um defeito de
formatacao do arquivo inteiro, nao um problema de registros especificos.
Somado aos demais, ele zera o indicador consolidado e esconde a evolucao do
resto — com os dois numeros lado a lado, da para acompanhar as duas coisas.

O cast para numero e defensivo (regex antes do `::numeric`): valor nao numerico
vira NULL e e contado como regra propria, em vez de derrubar o model. Um model
de qualidade de dados que quebra com dado ruim nao serve para nada.

### store_failures

`dbt_project.yml` liga `+store_failures: true` para os testes do projeto: em vez
de so contar quantas linhas falharam, o dbt grava as linhas no schema
`postgres_dq_failures`. E a diferenca entre "o teste quebrou" e "estas sao as 21
linhas com problema". Os testes do Elementary ficam de fora, porque o pacote ja
guarda resultado e amostra nas tabelas de metadados dele.

## Defeitos encontrados na base, e o que foi feito com cada um

Nenhum e falso positivo. Os tres primeiros sao sujeira da origem; o quarto era
um bug do nosso proprio codigo, e o mais grave dos quatro.

**1. `location_region` com o placeholder `'0'`** — 21 linhas trazem `'0'` em vez
de uma das 5 regioes. Vira `'unknown'` em `int_fraud_credit_cleaned`, e nao
NULL, para que a linha continue contavel nos indicadores: o defeito fica
explicito, nao apagado. `mart_average` exclui `'unknown'`, porque `'0'` nao e
uma regiao para entrar num ranking de regioes — antes aparecia como 6a linha do
resultado, com media 33,46.

**2. Sufixo `;;;` nos valores** — a coluna `anomaly;;;` do CSV era so renomeada
para `anomaly_status`, entao os VALORES seguiam como `low_risk;;;`. Atingia 100%
das linhas. Corrigido com `replace("anomaly;;;", ';;;', '')`. Com a limpeza no
lugar, o `accepted_values` do intermediate passou de `warn` para `error`: depois
de limpo, valor fora do dominio significa que a origem mudou, e isso e
incidente.

**3. `amount` e `risk_score` com 20 e 22 nulos** — sentinela `'none'` da origem,
convertido para NULL. Segue como `warn` no intermediate: e uma caracteristica
conhecida do dado, nao um incidente. Os marts e que nao podem propagar isso.

**4. `mart_top_vendas` devolvia tres linhas nulas** — este era nosso. O model
terminava em `ORDER BY amount DESC`, e no Postgres `DESC` implica **NULLS
FIRST**. Como 8 das 689 vendas mais recentes tem `amount` nulo (defeito 3), as
tres primeiras linhas do resultado eram tres nulos — a tabela-resultado pedida
no desafio estava errada. O proprio pipeline expunha isso: o
`not_null_mart_top_vendas_amount` falhava com `Got 3 results, configured to fail
if != 0`, derrubando a DAG `fraud_credit_run`. Corrigido com
`AND amount IS NOT NULL` antes do ranking — uma venda de valor desconhecido nao
pode ser "a de maior amount" — mais um desempate estavel por `receiving_address`
para o resultado nao alternar entre execucoes.

Depois das correcoes: **PASS=69, WARN=3, ERROR=0**. Os 3 warnings sao os
defeitos 1, 2 e 3 medidos na camada raw, que e onde eles devem continuar
visiveis.

## Rodar o edr na mao (debug)

```bash
astro dev bash -s
/usr/local/airflow/elementary_venv/bin/edr report \
  --profiles-dir /usr/local/airflow/fraud_credit \
  --project-dir /usr/local/airflow/fraud_credit \
  --profile-target dev \
  --target-path /tmp/edr_target \
  --file-path /usr/local/airflow/include/elementary/teste.html
```

Se o edr reclamar que nao acha as tabelas, e porque a `build_elementery` ainda
nao rodou: `dbt run --select elementary` e o que cria o schema
`postgres_elementary`.

## Limitacao conhecida: o projeto dbt nao e bind-mount

O `astro dev` monta apenas `dags/`, `plugins/`, `include/` e `tests/`. Como
`fraud_credit/` esta na raiz, ele entra na IMAGEM — entao **editar um model ou
um teste so tem efeito depois de `astro dev restart`** (que reconstroi).

Para ter hot-reload, mova o projeto para `include/fraud_credit/` e ajuste
`DEFAULT_DBT_ROOT_PATH` nas quatro DAGs. E o layout que a Astronomer documenta
para Cosmos.
