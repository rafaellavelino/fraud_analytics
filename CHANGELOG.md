# Changelog

Formato baseado em [Keep a Changelog](https://keepachangelog.com/pt-BR/1.1.0/).

## [0.3.0] — 2026-09-21

Versao que fecha as duas pendencias de observabilidade abertas na 0.2.0: a
deteccao de anomalia operando sobre janela vazia e os recursos do Elementary
ainda nao explorados. Nenhum model foi alterado — a mudanca e toda em
configuracao de teste.

### Corrigido

- **Os testes de anomalia do Elementary passavam verdes sem medir nada.** A
  0.2.0 ja registrava isto como pendencia conhecida e ja apontava
  `detection_delay` como saida; esta versao confirma o diagnostico com medicao
  e aplica a correcao.

  O Elementary calcula a janela como `[detection_end - training_period,
  detection_end]`, com `detection_end = run_started_at`
  (`get_buckets_configuration.sql`). O `transaction_at` do dataset termina em
  2024-01-02, 993 dias atras, e os defaults sao `training_period` de 14 dias e
  `detection_period` de 2 dias. A janela caia inteira em territorio sem linhas.

  A evidencia esta em `data_monitoring_metrics`: os 14 buckets de `row_count`
  de `int_fraud_credit_cleaned` tinham todos `metric_value = 0`. Variancia zero
  nao produz anomalia, e `fail_on_zero` e `false` por default — entao nem o
  zero acusava. Os testes eram verdes e cegos ao mesmo tempo.

  Corrigido com `detection_delay: 993 dias`, `training_period: 180 dias` e
  `detection_period: 7 dias`, declarados uma unica vez no bloco
  `config.elementary` do model. O Elementary resolve argumentos na ordem
  argumento explicito no teste > esse bloco > `vars` global
  (`get_test_argument.sql`), entao todos os testes Elementary do model herdam a
  janela sem repeticao.

  Depois da correcao: 686 pontos de `row_count` com media de 398 linhas/dia,
  buckets de 2023-07-06 a 2024-01-01. E o `dimension_anomalies_tipo_regiao`,
  que ja existia no projeto e sempre passara, acusou **1 anomalia de
  distribuicao** no primeiro run com a janela corrigida.

  Adicionado `fail_on_zero: true` como rede de seguranca: o `detection_delay` e
  um numero fixo, entao `detection_end` recua um dia por dia de calendario. A
  janela continua sobre dado real por anos (o historico tem 4), mas se um dia
  sair, o teste passa a FALHAR em vez de voltar a passar cego. Para recalcular
  o valor: `select (now()::date - max(transaction_at)::date) from
  postgres.int_fraud_credit_cleaned`.

- **Comentarios citavam numeros do CSV de AMOSTRA, nao do arquivo real.** Sete
  lugares erravam a contagem por duas ordens de magnitude: `amount` dizia "20
  linhas" com `'none'` (real: 5.641), `risk_score` dizia "22" (5.695), o
  placeholder `'0'` dizia "21" em tres arquivos (5.795), `mart_top_vendas`
  dizia "8 das 689 transacoes" (sao 8 de 1.166 carteiras) e `mart_average`
  registrava `unknown` com media "33,46" (real: 44,53 sobre 5.761 transacoes).

  Nao e cosmetico: sao esses comentarios que justificam `severity: warn` em vez
  de `error` nos testes de `not_null`. Quem os lia dimensionava o problema em
  dezenas de linhas quando sao milhares, e podia concluir que valia promover o
  teste a `error` — o que derrubaria a DAG. Todos conferidos contra o banco e
  corrigidos.

  Aproveitado para registrar em `mart_top_vendas` uma decisao que estava
  implicita no codigo: nao ha fallback para a penultima venda quando a mais
  recente tem `amount` nulo. A carteira sai do ranking, porque o enunciado pede
  "somente a transacao mais recente".

- **Estado de build do dbt vazava para a imagem Docker.** O diretorio
  `fraud_credit/` NAO e bind-mount — so `dags/`, `plugins/`, `include/` e
  `tests/` sao —, entao ele e copiado para a imagem no build. O
  `.dockerignore` nao excluia `fraud_credit/target/` nem
  `fraud_credit/logs/`, e o resultado eram **61,8 MB** de artefato local
  entrando na imagem a cada build (6,8 MB de `target/`, 55 MB de `logs/`).

  Nao era so peso. O `target/partial_parse.msgpack` carrega o profile e o
  target com que foi gerado. Um `dbt run` local contra `localhost:5433`
  deixava ali um manifest apontando para fora do Docker; no primeiro parse
  dentro do container o dbt o descartava
  (`Unable to do partial parsing because profile has changed`) e, nessa
  transicao, a resolucao de namespace de pacote falhava:

  ```
  Compilation Error in test formato_sending_address
    'dbt_expectations' is undefined
  ```

  O run seguinte, ja com `target/` nativo do container, compilava o mesmo
  teste sem erro — o que torna o defeito intermitente e dificil de atribuir.
  Corrigido excluindo os dois diretorios no `.dockerignore`.
  `dbt_packages/` ficou de fora de proposito: o Dockerfile roda `dbt deps` no
  build para popula-lo, e o Cosmos depende dele com `install_dbt_deps=False`.

- **`fraud_credit_ingest` aceitava execucoes simultaneas.** A DAG nao
  declarava `max_active_runs`, entao o default do Airflow (16) valia para uma
  carga FULL — `truncate` + `copy` na mesma tabela. Dois triggers manuais a 13
  segundos de distancia derrubaram o segundo:

  ```
  UniqueViolation: duplicate key value violates unique constraint
  "pg_namespace_nspname_index"
  DETAIL:  Key (nspname)=(postgres) already exists.
    em fraud_credit_ingest.py, no "create schema if not exists"
  ```

  `IF NOT EXISTS` nao e atomico no Postgres: as duas transacoes veem o schema
  como ausente, as duas tentam criar, e a segunda viola o indice unico de
  `pg_namespace`.

  A falha visivel e o menor dos problemas. Se o schema ja existisse, as duas
  execucoes passariam do DDL e disputariam o `truncate`/`copy` — uma poderia
  truncar a tabela no meio do `copy` da outra, e o resultado seria uma **carga
  silenciosamente incompleta, com a DAG verde**. Esse e o modo de falha que
  importa, e ele nao aparece em log nenhum.

  Corrigido com `max_active_runs=1`. Falhar rapido no segundo trigger e o
  comportamento correto para carga full.

### Adicionado

Dez testes novos, todos sobre `int_fraud_credit_cleaned` salvo indicacao:

| Teste | O que detecta |
|---|---|
| `volume_anomalies_diario` (`seasonality: day_of_week`) | compara segunda com segunda, em vez de contra a media geral |
| `volume_anomalies_fraude_spike` | pico de `phishing`/`scam` via `where_expression`, com `anomaly_direction: spike` |
| `volume_threshold_diario` | alternativa por percentual ao z-score, com warn e error separados no mesmo teste |
| `freshness_anomalies_transacao` | buraco temporal dentro do dia, que a contagem de volume nao ve |
| `dimension_anomalies_perfil_usuario` | desvio em `age_group`, `purchase_pattern` e na combinacao dos dois |
| `all_columns_anomalies_cobertura` | metricas por tipo em todas as colunas; rede de arrasto para o que ninguem pensou em testar |
| `collect_metrics_conformidade` | serie temporal diaria de `not_null_percent` e `not_missing_percent` |
| `column_anomalies` ampliado em `amount` e `risk_score` | `standard_deviation`, `variance`, `zero_count`, `sum` |
| `column_anomalies_*_address_len` | comprimento das carteiras: endereco truncado ou com sufixo colado |
| `formato_sending_address` / `formato_receiving_address` | contrato `^0x[0-9a-f]{40}$`, com colunas de contexto na linha que falha |
| `faixa_amount` | mesma faixa do `dbt_utils.accepted_range`, mas com contexto na falha |
| `schema_changes_from_baseline` (em `stg_raw_df_fraud_credit`) | contrato declarado das 13 colunas com tipo |

Tres pontos merecem nota:

- **`schema_changes_from_baseline` e o teste que pegaria o incidente do
  `anomaly;;;`.** O `schema_changes` comum compara contra o ULTIMO SCHEMA
  VISTO: avisa uma vez e adota a mudanca como o novo normal. O baseline compara
  contra o contrato declarado no yml, coluna por coluna e tipo por tipo, e nao
  para de reportar enquanto o desvio existir. As 13 colunas declaradas em
  `__raw_models.yaml` sao a mesma lista de `COLUMNS` em
  `dags/fraud_credit_ingest.py`, que faz o `COPY` casar por posicao — se as
  duas divergirem, a ingestao quebra. Configurado com `enforce_types: true`
  (sem isso o Elementary apenas emite warning e o contrato fica vazio sem
  ninguem notar) e `fail_on_added: false` (coluna nova e aditiva).

- **`collect_metrics` nao aceita `detection_delay`** — so `days_back`
  (`test_collect_metrics.sql`), e a janela dele e sempre
  `[agora - days_back, agora]`. Por isso esta com `days_back: 1100` em vez dos
  180 herdados: e o minimo para alcancar 2024-01-02. Os buckets de 2024-01 ate
  hoje vem zerados, e aqui isso e inofensivo, porque este teste nao alerta
  (o macro se marca com `meta.elementary.include = false`). Resultado: 108 dias
  de serie, com `pct_nao_nulo_amount` medio de **99,46%** — identico ao que o
  `mart_dq_indicadores` calcula por SQL proprio, o que valida os dois caminhos
  um contra o outro.

- **`dimensions` aceita expressao SQL, mas nao aspas simples.** A dimensao
  composta usa `age_group || chr(47) || purchase_pattern`. Parece rebuscado e e
  proposital: o Elementary monta o rotulo da dimensao concatenando a lista
  dentro de um literal SQL e nao escapa aspas. Escrever `'/'` fecha o literal
  no meio e o Postgres le a barra como divisao, falhando com
  `operator is not unique: unknown / unknown`. `chr(47)` entrega a mesma barra
  sem introduzir aspas.

### Alterado

- **`fraud_credit/.gitignore`** — adicionado `.user.yml`, arquivo de id anonimo
  que o dbt cria ao rodar fora do container.

### Resultado

Janela de deteccao, medida em `data_monitoring_metrics` antes e depois:

| | Antes | Depois |
|---|---|---|
| pontos de `row_count` | 14 | 686 |
| valor dos buckets | todos `0` | media 398 linhas/dia |
| intervalo coberto | 2026-09-04 a 2026-09-20 (vazio) | 2023-07-06 a 2024-01-01 |

A janela do `volume_anomalies_diario` vai de 2020-07-21 a 2024-01-01 (2.880
buckets): o `seasonality: day_of_week` multiplica o `training_period` por 7
internamente (`get_days_back.sql`), entao os 180 dias configurados viram 1.260
— ainda dentro do historico, que comeca em 2020-01-01.

Suite completa no arquivo real de 1.048.575 linhas:

```
PASS=74  WARN=4  ERROR=0  TOTAL=78
```

Os quatro avisos sao todos esperados e documentados:

| Aviso | Linhas | Motivo |
|---|---|---|
| `dimension_anomalies_tipo_regiao` | 1 | anomalia de distribuicao real, detectada no primeiro run com a janela corrigida |
| `not_null_int_fraud_credit_cleaned_amount` | 5.641 | sentinela `'none'` da origem |
| `not_null_int_fraud_credit_cleaned_risk_score` | 5.695 | sentinela `'none'` da origem |
| `source_accepted_values_..._location_region` | 1 | placeholder `'0'` na camada raw |

### Pendencias conhecidas

- **`int_fraud_credit_cleaned` continua `view`.** Com 1M de linhas, cada teste
  novo re-escaneia a base inteira atraves dela. O `all_columns_anomalies` e o
  mais sensivel a isso. Materializar como `table` segue sendo a otimizacao de
  maior retorno.

- **Artefatos de build rastreados no git** (~11MB em `edr_target/`), herdado da
  0.2.0.

- **Deriva do `detection_delay`.** Coberta por `fail_on_zero: true`, mas a
  solucao definitiva para um dataset fechado seria deslocar os timestamps da
  origem para a janela corrente, ou parametrizar o delay por `var` calculada na
  ingestao.

---

## [0.2.0] — 2026-09-18

Versao que adequa a pipeline ao volume real de dados (1.048.575 linhas, 167MB),
corrige defeitos nas tabelas-resultado e fecha a lacuna de indicadores de
qualidade.

### Corrigido

- **`mart_top_vendas` devolvia tres linhas nulas.** O model terminava em
  `ORDER BY amount DESC` e, no Postgres, `DESC` implica `NULLS FIRST`. Como
  parte das vendas mais recentes tem `amount` nulo (sentinela `'none'` da
  origem), as tres primeiras linhas do resultado eram tres nulos — ou seja, a
  tabela-resultado pedida no desafio estava errada. O proprio pipeline expunha
  o defeito: o `not_null_mart_top_vendas_amount` falhava com
  `Got 3 results, configured to fail if != 0`, derrubando a DAG
  `fraud_credit_run`. Adicionado `AND amount IS NOT NULL` antes do ranking —
  uma venda de valor desconhecido nao pode ser "a de maior amount" — mais
  desempate estavel por `receiving_address`, para o resultado nao alternar
  entre execucoes.

- **`mart_average` listava uma regiao inexistente.** O placeholder `'0'` da
  origem aparecia como sexta linha do ranking de regioes. Passou a ser
  normalizado para `'unknown'` na camada intermediaria e excluido do mart.

- **`elementary_report` falhava sempre, antes de executar o `edr`.** A DAG usava
  `{{ ds }}` no nome do arquivo, mas no Airflow 3 um run disparado por Asset nao
  tem `logical_date` — o Jinja quebrava com `UndefinedError: 'ds' is undefined`
  ao renderizar o `bash_command`. Trocado por `{{ dag_run.run_after | ds }}`,
  que existe em qualquer tipo de run.

- **Limpeza incompleta na camada intermediaria.** O model apenas renomeava a
  coluna `anomaly`, sem tratar o sufixo `;;;` que vinha colado nos valores
  (`low_risk;;;`), nem o placeholder `'0'` de `location_region`.

- **Docstring da DAG de relatorio descrevia a ordem errada** da cadeia.

### Adicionado

- **`mart_dq_indicadores`** — indicadores de qualidade do arquivo importado, um
  dos entregaveis do desafio que nao existia. Uma linha por regra de qualidade
  com `qtd_registros`, `qtd_erros`, `pct_erro` e `pct_conformidade`, mais duas
  linhas consolidadas. Sao 12 regras cobrindo valores faltantes, inconsistentes
  e incorretos, incluindo coerencia entre `risk_score` e o rotulo de risco.
  Medido sobre a camada **raw**, de proposito: medir depois da limpeza daria
  conformidade alta justamente porque a limpeza corrigiu.

- **`dags/fraud_credit_ingest.py`** — ingestao do CSV via `COPY`, substituindo o
  `dbt seed`.

- **Testes de schema** para o novo mart e para `location_region` na camada
  intermediaria.

- **`README.md`** reescrito (era o boilerplate do `astro dev init`): pre-requisitos,
  arquivos locais obrigatorios, ordem de execucao das DAGs, verificacao dos
  resultados e armadilhas conhecidas.

### Alterado

- **Ingestao: `dbt seed` -> `COPY`.** Medido neste projeto:

  | Metodo | Tempo (1.048.575 linhas) | Throughput |
  |---|---|---|
  | `dbt seed` | ~54 min (projetado de benchmark 20k/60k) | 326 linhas/s |
  | `COPY` | **7 s** | ~150.000 linhas/s |

  Alem da lentidao, o `dbt seed` carrega o CSV inteiro em memoria antes de
  emitir os INSERTs — com 167MB de texto o modo de falha deixa de ser lentidao e
  passa a ser OOM. O `dbt seed` foi feito para tabela de dominio, nao para carga.

- **`build_elementery` saiu da cadeia** e virou DAG de setup (`schedule=None`).
  Os models do pacote Elementary sao `incremental` com corpo vazio
  (`get_dbt_models_empty_table_query`): eles apenas **criam** as estruturas.
  Quem popula sao os hooks `on-run-start`/`on-run-end`, que gravam direto nas
  tabelas finais — `upload_run_results` insere em `dbt_run_results` sem area
  intermediaria nem consolidacao posterior. Com as tabelas ja criadas, roda-la a
  cada ciclo nao acrescentava dado algum, apenas ~2 min por execucao. Continua
  necessaria na primeira instalacao e apos cada upgrade do pacote.

- **Cadeia de DAGs** passou de quatro para tres elos:

  ```
  antes:  fraud_credit_seed -> build_elementery -> fraud_credit_run -> elementary_report
  agora:  fraud_credit_ingest -> fraud_credit_run -> elementary_report
  ```

- **CSV de origem movido** de `fraud_credit/seeds/` para `include/data/`.
  `fraud_credit/` entra na imagem Docker; `include/` e bind-mount. Com 167MB, o
  arquivo seria recopiado a cada build — e o GitHub rejeita arquivo acima de
  100MB. Adicionado `include/data/*.csv` ao `.gitignore`.

- **Coluna `anomaly;;;` renomeada para `anomaly`.** O arquivo completo nao tem o
  sufixo; ele era artefato do CSV de amostra. O `replace()` foi mantido nos
  models como defesa, caso o artefato reapareca numa carga futura.

- **`accepted_values` de `anomaly_status` subiu de `warn` para `error`.** Com a
  limpeza no lugar, um valor fora do dominio deixa de ser sujeira esperada e
  passa a significar mudanca na origem ou limpeza quebrada.

### Descontinuado

- **`fraud_credit_seed`** — substituida pela `fraud_credit_ingest` e **pausada**.
  Nao despause: ela recria a tabela a partir do CSV e apaga a carga.

### Resultado

Execucao completa sobre 1.048.575 linhas:

| Etapa | Duracao |
|---|---|
| `fraud_credit_ingest` | 7 s |
| `fraud_credit_run` (models + 65 testes) | 265 s |
| `elementary_report` | 47 s |

`PASS=69, WARN=3, ERROR=0` na validacao com a amostra; conformidade de
**98,38%** no arquivo completo (17.031 linhas com defeito), distribuida em
placeholder `'0'` (5.795), `risk_score` ausente (5.695) e `amount` ausente
(5.641).

### Pendencias conhecidas

- **Deteccao de anomalia do Elementary opera sobre janela vazia.** Os testes
  temporais bucketizam por `transaction_at`, que termina em 2024-01-02 — 990
  dias atras. A janela do Elementary e `[detection_end - days_back,
  detection_end]` com `detection_end = agora`, entao ela varre um intervalo sem
  linhas: as metricas coletadas para `int_fraud_credit_cleaned` somam zero. Os
  testes passam verdes sem comparar nada. Resolver com `detection_delay`,
  bucketizando por horario de execucao, ou deslocando os timestamps da origem.

- **`int_fraud_credit_cleaned` e `view`.** Com 1M de linhas, cada mart e cada
  teste re-escaneiam a base inteira atraves dela. Materializar como `table`
  deve reduzir os ~240 s de transformacao.

- **Artefatos de build rastreados no git** (~11MB em `edr_target/` e
  `fraud_credit/edr_target/`). Regerados a cada execucao, sujam todo diff.
  Remover com `git rm -r --cached`.

- **Recursos do Elementary ainda nao explorados:** `collect_metrics` (que expoe
  `not_null_percent` e `not_missing_percent` nativamente, com historico),
  `all_columns_anomalies`, e os testes `*_with_context`, que levam as linhas com
  defeito e suas colunas de contexto para dentro do relatorio.

---

## [0.1.0] — anterior

Versao inicial: projeto dbt com staging, intermediate e marts; DAGs de seed,
build e relatorio orquestradas por Cosmos; Elementary configurado com testes de
anomalia; warehouse Postgres dedicado em `docker-compose.override.yml`.
