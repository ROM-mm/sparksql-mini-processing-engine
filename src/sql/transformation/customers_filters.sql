-- Filtra dados de clientes: remove registros inválidos
-- Mantém apenas clientes com dados válidos
-- Usa {{ ref() }} para referenciar a view de enriquecimento
SELECT
    *
FROM {{ ref('customers_validations') }}
WHERE
    -- Filtrar apenas clientes com email válido
    email_valido = true
    -- Filtrar apenas clientes com telefone válido
    AND telefone_valido = true
    -- Filtrar registros com nome não nulo e não vazio
    AND nome IS NOT NULL
    AND TRIM(nome) != ''
    -- Filtrar registros com cidade não nula
    AND cidade IS NOT NULL
    AND TRIM(cidade) != ''
    -- Filtrar registros com estado não nulo
    AND estado IS NOT NULL
    AND TRIM(estado) != '';

