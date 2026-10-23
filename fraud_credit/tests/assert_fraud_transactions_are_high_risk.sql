-- Teste singular de regra de negocio: transacao de fraude confirmada
-- (phishing ou scam) tem que estar classificada como high_risk.
--
-- Na base atual isso vale para 100% dos casos (106 phishing + 149 scam, todos
-- high_risk). Se uma fraude entrar rotulada como low/moderate, ou o rotulo
-- esta errado ou a classificacao de tipo esta errada — os dois sao incidente.

select
    transaction_at,
    sending_address,
    receiving_address,
    transaction_type,
    risk_score,
    replace(anomaly_status, ';;;', '') as status
from {{ ref('int_fraud_credit_cleaned') }}
where transaction_type in ('phishing', 'scam')
  and replace(anomaly_status, ';;;', '') <> 'high_risk'
