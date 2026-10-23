with source_data as (

    select * from {{ ref('stg_raw_df_fraud_credit')}}

),

transformed as (

    select
        -- Conversão do Unix Timestamp (Integer) para Timestamp com fuso horário
        to_timestamp(timestamp) as transaction_at,
        -- Textos diretos da fonte (Mantidos como text)
        sending_address,
        receiving_address,
        -- Limpeza e conversão de Text para Numeric (Garante precisão financeira)
        nullif(amount, 'none')::numeric(18, 4) as amount,
        -- Textos diretos da fonte
        transaction_type,
        -- A origem entrega 5.795 linhas com '0', que e placeholder de regiao
        -- desconhecida e nao uma regiao. Vira 'unknown' em vez de NULL para
        -- que a linha continue contavel nos indicadores de qualidade: o
        -- defeito fica explicito, nao escondido. Os marts de resultado
        -- excluem 'unknown', porque '0' nao e uma regiao para ranquear.
        case
            when location_region = '0' then 'unknown'
            else location_region
        end as location_region,
        -- IP prefix veio como Double Precision, convertendo para texto para manter padrão de rede
        ip_prefix::text as ip_prefix,
        -- Inteiros nativos da fonte
        login_frequency,
        session_duration,
        -- Textos diretos da fonte
        purchase_pattern,
        age_group,
        -- Conversão de Text para Double Precision (Float)
        nullif(risk_score, 'none')::double precision as risk_score,
        -- O arquivo de AMOSTRA entregava esta coluna como "anomaly;;;", com o
        -- sufixo colado tambem nos VALORES ('low_risk;;;'). O arquivo real
        -- (1.048.575 linhas) vem limpo e a coluna se chama "anomaly".
        -- O replace fica como defesa: se o artefato reaparecer numa carga
        -- futura, o valor continua normalizado em vez de furar o
        -- accepted_values, que agora e severity: error.
        replace(anomaly, ';;;', '') as anomaly_status

    from source_data
)

select * from transformed
