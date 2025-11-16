SELECT
    id,
    nome,
    email,
    telefone,
    cidade,
    estado,
    regiao,
    email_valido,
    telefone_valido,
    CURRENT_TIMESTAMP() AS processed_at
FROM {{ ref('customers_filters') }}
ORDER BY estado, cidade, nome;

