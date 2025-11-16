SELECT
    id,
    nome,
    email,
    telefone_clean AS telefone,
    telefone_original,
    cidade,
    estado,
    -- Validação de email (verifica se tem @ e .)
    CASE 
        WHEN email LIKE '%@%.%' THEN true 
        ELSE false 
    END AS email_valido,
    -- Validação de telefone (deve ter pelo menos 10 dígitos)
    CASE 
        WHEN LENGTH(telefone_clean) >= 10 THEN true 
        ELSE false 
    END AS telefone_valido,
    -- Região do Brasil baseada no estado
    CASE 
        WHEN estado IN ('AC', 'AP', 'AM', 'PA', 'RO', 'RR', 'TO') THEN 'Norte'
        WHEN estado IN ('AL', 'BA', 'CE', 'MA', 'PB', 'PE', 'PI', 'RN', 'SE') THEN 'Nordeste'
        WHEN estado IN ('GO', 'MT', 'MS', 'DF') THEN 'Centro-Oeste'
        WHEN estado IN ('ES', 'MG', 'RJ', 'SP') THEN 'Sudeste'
        WHEN estado IN ('PR', 'RS', 'SC') THEN 'Sul'
        ELSE 'Não identificado'
    END AS regiao
FROM {{ ref('customers_padroniza') }};

