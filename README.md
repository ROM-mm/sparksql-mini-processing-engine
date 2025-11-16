# SparkSQL Declarative Pipelines - Processamento de Dados

Este projeto processa dados usando **apenas arquivos SQL** com **Spark Declarative Pipelines (SDP)**. O engenheiro escreve a lógica em arquivos `.sql` e o SDP gerencia automaticamente a ordem de execução, dependências e persistência dos dados.

## 🎯 Conceito

A ideia do projeto é processar dados usando apenas queries SQL, onde o engenheiro escreve a lógica nos arquivos `.sql` e o **Spark Declarative Pipelines** determina automaticamente a ordem de execução dos arquivos que serão executados como Materialized Views no SparkSQL.

### Características Principais

- ✅ **Processamento declarativo**: Apenas SQL, sem código de orquestração
- ✅ **Detecção automática de dependências**: Usa templates estilo dbt `{{ ref() }}`
- ✅ **Materialized Views**: Persistência automática como tabelas Parquet
- ✅ **Configuração unificada**: Tudo em `pipeline.yml`
- ✅ **Ordem de execução automática**: Baseada em dependências detectadas

## 📁 Estrutura do Projeto

```
declarative-pipelines/
├── pipeline.yml                    # Configuração principal unificada
├── src/
│   ├── sql/                        # Arquivos SQL com transformações
│   │   ├── source/                 # Arquivos de origem
│   │   │   └── mv_source.sql
│   │   ├── transformation/         # Transformações intermediárias
│   │   │   ├── table_customers_select_structure.sql
│   │   │   ├── table_customers_enrichment.sql
│   │   │   └── table_customers_filter.sql
│   │   └── export/                 # Exportações finais
│   │       └── table_customers_final.sql
│   └── modules/                    # Módulos Python auxiliares
│       ├── config_loader.py        # Carregamento de configurações
│       ├── source.py               # Configuração de origem
│       ├── sql_file_processor.py   # Processamento de arquivos SQL
│       ├── sql_template_processor.py  # Processamento de templates {{ ref() }}
│       ├── materialized_view_utils.py # Utilitários para Materialized Views
│       └── view_loader.py          # Carregamento de views
└── lakehouse/                      # Zonas do lakehouse
    ├── stage/                      # Dados brutos de entrada
    ├── silver/                     # Dados confiáveis e validados
    ├── gold/                       # Dados agregados e prontos para consumo
    └── .meta/                      # Metadados e persistência do Spark (warehouse)
```

## ⚙️ Configuração

### `pipeline.yml` - Configuração Principal

O arquivo `pipeline.yml` contém toda a configuração do pipeline:

```yaml
name: spark_declarative_pipelines

definitions:
  - glob:
      include: src/sql/**/*.sql
  - glob:
      include: src/modules/**/*.py

catalog: spark_catalog
database: default

configuration:
  spark.sql.shuffle.partitions: "200"
  spark.sql.adaptive.enabled: "true"
  spark.sql.warehouse.dir: "lakehouse/.meta"
  # ... outras configurações do Spark

transformation_settings:
  validate_schema: true
  log_transformations: true
  execution_mode: batch
  use_dependency_detection: true  # Detecta dependências via {{ ref() }}

source:
  format: "csv"
  path: "lakehouse/stage/*.csv"
  options:
    header: "true"
    inferSchema: "true"
  view_name: "source_table"

export:
  parquet:
    path: "lakehouse/silver/table_customers_final"
    mode: "overwrite"
    compression: "snappy"
```

## 📝 Escrevendo Transformações SQL

### Sintaxe Básica

Os arquivos SQL contêm apenas a lógica de transformação (SELECT statements). O sistema automaticamente:
- Adiciona `CREATE MATERIALIZED VIEW` usando o nome do arquivo
- Processa templates `{{ ref() }}` para referenciar outras views
- Gerencia a ordem de execução baseada em dependências

### Exemplo: Arquivo de Origem

**`src/sql/source/mv_source.sql`**
```sql
SELECT * FROM {{ ref('source_table') }}
```

### Exemplo: Transformação Intermediária

**`src/sql/transformation/table_customers_select_structure.sql`**
```sql
SELECT
    id,
    TRIM(UPPER(nome)) AS nome_normalized,
    INITCAP(TRIM(nome)) AS nome,
    LOWER(TRIM(email)) AS email,
    REGEXP_REPLACE(telefone, '[^0-9]', '') AS telefone_clean,
    INITCAP(TRIM(cidade)) AS cidade,
    UPPER(TRIM(estado)) AS estado
FROM {{ ref('mv_source') }};
```

### Exemplo: Transformação com Enriquecimento

**`src/sql/transformation/table_customers_enrichment.sql`**
```sql
SELECT
    id,
    nome,
    email,
    telefone_clean AS telefone,
    cidade,
    estado,
    CASE 
        WHEN email LIKE '%@%.%' THEN true 
        ELSE false 
    END AS email_valido,
    CASE 
        WHEN LENGTH(telefone_clean) >= 10 THEN true 
        ELSE false 
    END AS telefone_valido,
    CASE 
        WHEN estado IN ('SP', 'RJ', 'MG', 'ES') THEN 'Sudeste'
        WHEN estado IN ('PR', 'SC', 'RS') THEN 'Sul'
        -- ... outras regiões
        ELSE 'Não identificado'
    END AS regiao
FROM {{ ref('table_customers_select_structure') }};
```

### Templates `{{ ref() }}`

O projeto usa templates estilo dbt para referenciar outras views:

- `{{ ref('view_name') }}` - Referencia uma Materialized View criada por outro arquivo SQL
- O nome da view é o nome do arquivo SQL (sem extensão)
- Exemplo: `src/sql/transformation/table_customers.sql` cria a view `table_customers`

## 🔄 Ordem de Execução Automática

A ordem de execução é determinada **automaticamente** pelas dependências detectadas via `{{ ref() }}`:

1. O sistema analisa todos os arquivos SQL
2. Detecta dependências através de `{{ ref('view_name') }}`
3. Constrói um grafo de dependências
4. Executa na ordem correta automaticamente

**Exemplo de dependências:**
```
mv_source
  └── table_customers_select_structure
      └── table_customers_enrichment
          └── table_customers_filter
              └── table_customers_final
```

## 💾 Materialized Views

Todas as views são criadas como **Materialized Views**, que são:
- **Persistidas automaticamente** como tabelas Parquet no warehouse
- **Armazenadas em**: `lakehouse/.meta/` (configurado via `spark.sql.warehouse.dir`)
- **Gerenciadas pelo Spark**: Criadas, atualizadas e removidas automaticamente

### Benefícios

- ✅ **Performance**: Dados pré-computados e persistidos
- ✅ **Reutilização**: Views podem ser referenciadas em múltiplas transformações
- ✅ **Confiabilidade**: Dados persistidos sobrevivem a reinicializações
- ✅ **Rastreabilidade**: Histórico de transformações mantido no warehouse

## 📊 Zonas do Lakehouse

O projeto segue a arquitetura de lakehouse com as seguintes zonas:

- **stage/** - Dados brutos de entrada (CSV, JSON, etc.)
- **silver/** - Dados confiáveis e validados (camada intermediária)
- **gold/** - Dados agregados e prontos para consumo (camada final)
- **.meta/** - Metadados e persistência interna do Spark (warehouse)

## 🔧 Processamento de Arquivos SQL

O sistema processa automaticamente os arquivos SQL:

1. **Detecção**: Encontra todos os arquivos `.sql` em `src/sql/**/*.sql`
2. **Processamento**: 
   - Remove qualquer `CREATE VIEW` existente
   - Adiciona `CREATE MATERIALIZED VIEW` usando o nome do arquivo
   - Processa templates `{{ ref() }}` substituindo por nomes de views reais
3. **Execução**: Executa na ordem correta baseada em dependências

## 📚 Referências

- [Spark Declarative Pipelines Documentation](https://spark.apache.org/docs/4.1.0-preview1/declarative-pipelines-programming-guide.html)
- [Spark SQL Data Sources](https://spark.apache.org/docs/latest/sql-data-sources.html)

## 👤 Autor

**Romero Morais**  
LinkedIn: [romerito.morais](https://www.linkedin.com/in/r0m-m0r/)
