SELECT 
    location_region, 
    ROUND(AVG(risk_score)::numeric, 2) AS media_risk_score
FROM 
    {{ ref('int_fraud_credit_cleaned') }}
WHERE 
    risk_score IS NOT NULL
    -- 'unknown' e o placeholder '0' da origem, normalizado na camada
    -- intermediaria. Nao e uma regiao geografica, entao nao entra num
    -- ranking de regioes — sem este filtro aparece como 6a linha, com
    -- media 44,53 sobre 5.761 transacoes de risk_score conhecido.
    AND location_region <> 'unknown'
GROUP BY 
    location_region
ORDER BY 
    media_risk_score DESC
