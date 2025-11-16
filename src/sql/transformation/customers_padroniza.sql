SELECT
    id,
    TRIM(UPPER(nome)) AS nome_normalized,
    INITCAP(TRIM(nome)) AS nome,
    LOWER(TRIM(email)) AS email,
    REGEXP_REPLACE(telefone, '[^0-9]', '') AS telefone_clean,
    telefone AS telefone_original,
    INITCAP(TRIM(cidade)) AS cidade,
    UPPER(TRIM(estado)) AS estado
FROM {{ ref('customers') }};

