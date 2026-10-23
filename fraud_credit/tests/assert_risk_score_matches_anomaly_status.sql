-- Teste singular: o rotulo de risco tem que ser coerente com o score.
--
-- Regra derivada do proprio dado (faixas disjuntas, sem sobreposicao):
--   risk_score <  60  -> low_risk       (observado: 15,00 a 59,06)
--   risk_score >= 60 e < 90 -> moderate_risk (observado: 62,50 a 84,38)
--   risk_score >= 90  -> high_risk      (observado: 90,00 a 100,00)
--
-- Uma quebra aqui significa que score e rotulo sairam de sincronia — o tipo de
-- defeito que nenhum teste de coluna isolada pega, porque cada coluna esta
-- dentro da faixa dela.
--
-- O replace() neutraliza o sufixo ';;;' que vem da origem, entao o teste
-- continua valido depois que int_fraud_credit_cleaned for corrigido.

with rotulado as (

    select
        transaction_at,
        sending_address,
        receiving_address,
        risk_score,
        replace(anomaly_status, ';;;', '') as status,
        case
            when risk_score <  60 then 'low_risk'
            when risk_score <  90 then 'moderate_risk'
            else                       'high_risk'
        end as status_esperado
    from {{ ref('int_fraud_credit_cleaned') }}
    where risk_score is not null   -- nulos sao cobertos pelo teste not_null

)

select *
from rotulado
where status <> status_esperado
