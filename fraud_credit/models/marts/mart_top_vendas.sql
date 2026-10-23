WITH transacoes_recentes AS (
    SELECT 
        receiving_address,
        amount,
        transaction_at,
        ROW_NUMBER() OVER(
            PARTITION BY receiving_address 
            ORDER BY transaction_at DESC
        ) AS rank_recente
    FROM 
        {{ref('int_fraud_credit_cleaned')}}
    WHERE 
        transaction_type = 'sale'
)
SELECT 
    receiving_address,
    amount,
    transaction_at
FROM 
    transacoes_recentes
WHERE 
    rank_recente = 1
    -- Das 1.166 carteiras com venda, 8 tem amount NULL na venda mais recente
    -- (sentinela 'none' da origem). Sem este filtro elas ocupavam o topo: no
    -- Postgres, ORDER BY ... DESC implica NULLS FIRST, entao o resultado deste
    -- mart eram 3 linhas nulas. Venda de valor desconhecido nao pode ser
    -- "a de maior amount".
    --
    -- Nao ha fallback para a penultima venda, de proposito: o enunciado pede
    -- "somente a transacao mais recente", entao a carteira sai do ranking em
    -- vez de entrar com um valor que nao e o da venda mais recente.
    AND amount IS NOT NULL
ORDER BY 
    amount DESC,
    -- Desempate estavel: sem ele, amounts iguais trocam de posicao entre runs
    -- e o Elementary acusa mudanca onde o dado nao mudou.
    receiving_address
LIMIT 3
