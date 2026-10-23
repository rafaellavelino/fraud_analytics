-- Indicadores de qualidade do arquivo importado.
--
-- Complementa, e nao substitui, os testes dbt e o relatorio do Elementary:
--   - os testes respondem "passou ou quebrou" (booleano, por regra);
--   - o Elementary responde "mudou em relacao ao historico" (anomalia);
--   - este model responde "qual o TAMANHO do problema agora" — quantos
--     registros, quantos erros e qual o percentual de conformidade.
--
-- A medicao e feita sobre a camada RAW de proposito: e o dado como foi
-- importado do CSV, antes da limpeza. Medir depois da limpeza mostraria
-- conformidade artificialmente alta, justamente porque a limpeza corrigiu.

with raw_data as (

    select * from {{ ref('stg_raw_df_fraud_credit') }}

),

-- Cast defensivo: a coluna e texto e pode conter qualquer coisa. O regex
-- garante que um valor nao numerico vire NULL em vez de derrubar o model,
-- e o proprio "nao numerico" vira uma regra medida abaixo.
tipado as (

    select
        *,
        case when amount     ~ '^-?[0-9]+(\.[0-9]+)?$' then amount::numeric          end as amount_num,
        case when risk_score ~ '^-?[0-9]+(\.[0-9]+)?$' then risk_score::double precision end as risk_score_num,
        replace(anomaly, ';;;', '') as anomaly_limpo
    from raw_data

),

-- Uma coluna booleana por regra de qualidade, avaliada linha a linha.
-- Marcar por linha (em vez de contar por regra) permite derivar tanto o
-- indicador de cada regra quanto o total de linhas integras numa varredura so.
marcado as (

    select
        (amount is null or amount = 'none')                       as err_amount_ausente,
        (risk_score is null or risk_score = 'none')               as err_risk_score_ausente,
        (amount <> 'none' and amount_num is null)                 as err_amount_nao_numerico,
        (risk_score <> 'none' and risk_score_num is null)         as err_risk_score_nao_numerico,
        -- NOT IN devolve NULL quando a coluna e NULL, e uma linha assim
        -- escaparia de TODA regra categorica — um indicador de qualidade que
        -- perde justamente o valor faltante. Por isso o "is null or".
        (location_region is null or location_region not in (
            'Europe', 'South America', 'Asia', 'Africa', 'North America'
         ))                                                       as err_regiao_fora_dominio,
        (transaction_type is null or transaction_type not in (
            'sale', 'purchase', 'transfer', 'phishing', 'scam'
         ))                                                       as err_tipo_fora_dominio,
        (anomaly_limpo is null or anomaly_limpo not in (
            'low_risk', 'moderate_risk', 'high_risk'
         ))                                                       as err_rotulo_fora_dominio,
        (anomaly like '%;;;')                                     as err_sufixo_nao_removido,
        (amount_num < 0)                                          as err_amount_negativo,
        (risk_score_num < 0 or risk_score_num > 100)              as err_risk_score_fora_faixa,
        (sending_address is null or receiving_address is null
            or timestamp is null)                                 as err_chave_nula,
        -- Coerencia entre score e rotulo: o tipo de defeito que nenhum teste
        -- de coluna isolada pega, porque cada coluna esta dentro da faixa dela.
        (risk_score_num is not null and anomaly_limpo <> case
            when risk_score_num <  60 then 'low_risk'
            when risk_score_num <  90 then 'moderate_risk'
            else                           'high_risk'
         end)                                                     as err_rotulo_incoerente
    from tipado

),

agregado as (

    select
        count(*)                                               as qtd_registros,
        count(*) filter (where err_amount_ausente)             as amount_ausente,
        count(*) filter (where err_risk_score_ausente)         as risk_score_ausente,
        count(*) filter (where err_amount_nao_numerico)        as amount_nao_numerico,
        count(*) filter (where err_risk_score_nao_numerico)    as risk_score_nao_numerico,
        count(*) filter (where err_regiao_fora_dominio)        as regiao_fora_dominio,
        count(*) filter (where err_tipo_fora_dominio)          as tipo_fora_dominio,
        count(*) filter (where err_rotulo_fora_dominio)        as rotulo_fora_dominio,
        count(*) filter (where err_sufixo_nao_removido)        as sufixo_nao_removido,
        count(*) filter (where err_amount_negativo)            as amount_negativo,
        count(*) filter (where err_risk_score_fora_faixa)      as risk_score_fora_faixa,
        count(*) filter (where err_chave_nula)                 as chave_nula,
        count(*) filter (where err_rotulo_incoerente)          as rotulo_incoerente,
        count(*) filter (where
            err_amount_ausente or err_risk_score_ausente
            or err_amount_nao_numerico or err_risk_score_nao_numerico
            or err_regiao_fora_dominio or err_tipo_fora_dominio
            or err_rotulo_fora_dominio or err_sufixo_nao_removido
            or err_amount_negativo or err_risk_score_fora_faixa
            or err_chave_nula or err_rotulo_incoerente
        )                                                      as linhas_com_algum_defeito,
        -- O sufixo ';;;' atinge 100% das linhas: e um defeito de FORMATACAO
        -- do arquivo inteiro, nao um problema de conteudo de registros
        -- especificos. Somado aos demais, ele zera o indicador consolidado e
        -- esconde a evolucao dos outros defeitos — por isso os dois totais.
        count(*) filter (where
            err_amount_ausente or err_risk_score_ausente
            or err_amount_nao_numerico or err_risk_score_nao_numerico
            or err_regiao_fora_dominio or err_tipo_fora_dominio
            or err_rotulo_fora_dominio
            or err_amount_negativo or err_risk_score_fora_faixa
            or err_chave_nula or err_rotulo_incoerente
        )                                                      as linhas_com_defeito_de_conteudo
    from marcado

),

indicadores as (

    select 'TOTAL — linhas com defeito de conteudo' as regra,
           '(todas)'            as coluna,
           'consolidado'        as tipo_defeito,
           qtd_registros, linhas_com_defeito_de_conteudo as qtd_erros from agregado
    union all
    select 'TOTAL — linhas com qualquer defeito (inclui formatacao)',
           '(todas)', 'consolidado', qtd_registros, linhas_com_algum_defeito from agregado
    union all
    select 'amount ausente (sentinela ''none'')',
           'amount', 'valor faltante', qtd_registros, amount_ausente from agregado
    union all
    select 'risk_score ausente (sentinela ''none'')',
           'risk_score', 'valor faltante', qtd_registros, risk_score_ausente from agregado
    union all
    select 'amount nao numerico',
           'amount', 'valor incorreto', qtd_registros, amount_nao_numerico from agregado
    union all
    select 'risk_score nao numerico',
           'risk_score', 'valor incorreto', qtd_registros, risk_score_nao_numerico from agregado
    union all
    select 'location_region fora do dominio (placeholder ''0'')',
           'location_region', 'valor inconsistente', qtd_registros, regiao_fora_dominio from agregado
    union all
    select 'transaction_type fora do dominio',
           'transaction_type', 'valor inconsistente', qtd_registros, tipo_fora_dominio from agregado
    union all
    select 'rotulo de risco fora do dominio',
           'anomaly', 'valor inconsistente', qtd_registros, rotulo_fora_dominio from agregado
    union all
    select 'sufixo '';;;'' presente no valor',
           'anomaly', 'valor incorreto', qtd_registros, sufixo_nao_removido from agregado
    union all
    select 'amount negativo',
           'amount', 'valor incorreto', qtd_registros, amount_negativo from agregado
    union all
    select 'risk_score fora da faixa 0-100',
           'risk_score', 'valor incorreto', qtd_registros, risk_score_fora_faixa from agregado
    union all
    select 'chave de transacao nula',
           'timestamp / addresses', 'valor faltante', qtd_registros, chave_nula from agregado
    union all
    select 'rotulo incoerente com o risk_score',
           'anomaly x risk_score', 'valor inconsistente', qtd_registros, rotulo_incoerente from agregado

)

select
    regra,
    coluna,
    tipo_defeito,
    qtd_registros,
    qtd_erros,
    round(100.0 * qtd_erros / nullif(qtd_registros, 0), 2)                   as pct_erro,
    round(100.0 * (qtd_registros - qtd_erros) / nullif(qtd_registros, 0), 2) as pct_conformidade
from indicadores
order by
    case when regra like 'TOTAL%' then 0 else 1 end,
    qtd_erros desc,
    regra
