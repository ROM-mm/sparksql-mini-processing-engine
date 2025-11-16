#!/bin/bash

# Script de setup para instalar todas as dependências do projeto
# SparkSQL Mini Processing Engine com Airflow
#
# IMPORTANTE: Este script sempre faz uma instalação limpa, removendo:
#   - Ambiente virtual anterior (venv)
#   - Arquivos de configuração (.env, activate_env.sh)
#   - Configurações do Airflow (airflow_home)
#
# Execute este script sempre que quiser resetar o ambiente completamente.

set -e

echo "🚀 Configurando ambiente SparkSQL Mini Processing Engine..."
echo "⚠️  ATENÇÃO: Este script irá remover todas as configurações anteriores!"
echo ""

# Cores para output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Verificar Python
echo "📦 Verificando Python..."
if ! command -v python3 &> /dev/null; then
    echo "❌ Python 3 não encontrado. Por favor, instale Python 3.10 ou superior."
    exit 1
fi

PYTHON_VERSION=$(python3 --version | cut -d' ' -f2 | cut -d'.' -f1,2)
echo "✅ Python ${PYTHON_VERSION} encontrado"
echo ""

# Verificar Java
echo "📦 Verificando Java..."
JAVA_FOUND=false
JAVA_HOME_PATH=""

if [[ "$OSTYPE" == "darwin"* ]]; then
    # macOS: usar /usr/libexec/java_home
    if command -v /usr/libexec/java_home &> /dev/null; then
        JAVA_HOME_PATH=$(/usr/libexec/java_home 2>/dev/null || echo "")
        if [ -n "$JAVA_HOME_PATH" ] && [ -d "$JAVA_HOME_PATH" ]; then
            JAVA_FOUND=true
        fi
    fi
    
    # Verificar Homebrew
    if [ "$JAVA_FOUND" = false ]; then
        if [ -d "/opt/homebrew/opt/openjdk@21" ]; then
            JAVA_HOME_PATH="/opt/homebrew/opt/openjdk@21"
            JAVA_FOUND=true
        elif [ -d "/opt/homebrew/opt/openjdk" ]; then
            JAVA_HOME_PATH=$(ls -d /opt/homebrew/opt/openjdk@* 2>/dev/null | sort -V -r | head -n 1)
            if [ -z "$JAVA_HOME_PATH" ]; then
                JAVA_HOME_PATH="/opt/homebrew/opt/openjdk"
            fi
            JAVA_FOUND=true
        fi
    fi
elif [[ "$OSTYPE" == "linux-gnu"* ]]; then
    # Linux: verificar JAVA_HOME ou locais comuns
    if [ -n "$JAVA_HOME" ] && [ -d "$JAVA_HOME" ]; then
        JAVA_HOME_PATH="$JAVA_HOME"
        JAVA_FOUND=true
    elif [ -d "/usr/lib/jvm" ]; then
        JAVA_HOME_PATH=$(ls -d /usr/lib/jvm/java-*-openjdk* 2>/dev/null | head -n 1)
        if [ -n "$JAVA_HOME_PATH" ]; then
            JAVA_FOUND=true
        fi
    fi
fi

if [ "$JAVA_FOUND" = true ] && [ -n "$JAVA_HOME_PATH" ]; then
    export JAVA_HOME="$JAVA_HOME_PATH"
    export PATH="${JAVA_HOME}/bin:${PATH}"
    JAVA_VERSION=$("$JAVA_HOME/bin/java" -version 2>&1 | head -n 1)
    echo "✅ Java encontrado: $JAVA_HOME_PATH"
    echo "   Versão: $JAVA_VERSION"
else
    echo -e "${YELLOW}⚠️  Java não encontrado${NC}"
    echo ""
    echo "Java é necessário para o PySpark funcionar."
    echo ""
    if [[ "$OSTYPE" == "darwin"* ]]; then
        echo "Para instalar Java no macOS:"
        echo "  brew install openjdk@21"
        echo ""
        echo "Ou baixe de: https://adoptium.net/"
    else
        echo "Para instalar Java no Linux:"
        echo "  sudo apt-get install openjdk-21-jdk  # Ubuntu/Debian"
        echo "  sudo yum install java-21-openjdk     # CentOS/RHEL"
    fi
    echo ""
    read -p "Deseja continuar sem Java? (PySpark pode não funcionar) (s/N): " continue_without_java
    if [[ ! "$continue_without_java" =~ ^[Ss]$ ]]; then
        echo "❌ Setup cancelado. Por favor, instale Java primeiro."
        exit 1
    fi
    echo -e "${YELLOW}⚠️  Continuando sem Java - PySpark pode não funcionar corretamente${NC}"
fi
echo ""

# Limpar instalações anteriores
echo "🧹 Limpando instalações anteriores..."
echo ""

# Remover ambiente virtual se existir
if [ -d "venv" ]; then
    echo "   Removendo ambiente virtual anterior..."
    rm -rf venv
    echo "✅ Ambiente virtual removido"
fi

# Remover arquivos de configuração
if [ -f ".env" ]; then
    echo "   Removendo arquivo .env anterior..."
    rm -f .env
    echo "✅ Arquivo .env removido"
fi

if [ -f "activate_env.sh" ]; then
    echo "   Removendo script activate_env.sh anterior..."
    rm -f activate_env.sh
    echo "✅ Script activate_env.sh removido"
fi

# Remover configurações do Airflow
if [ -d "airflow_home" ]; then
    echo "   Removendo configurações do Airflow..."
    rm -rf airflow_home
    echo "✅ Configurações do Airflow removidas"
fi

echo "✅ Limpeza concluída"
echo ""

# Criar ambiente virtual (sempre novo)
echo "🔧 Criando ambiente virtual..."
python3 -m venv venv
echo "✅ Ambiente virtual criado"
echo ""

# Ativar ambiente virtual
echo "🔧 Ativando ambiente virtual..."
source venv/bin/activate
echo "✅ Ambiente virtual ativado"
echo ""

# Atualizar pip
echo "📦 Atualizando pip..."
pip install --upgrade pip setuptools wheel
echo "✅ pip atualizado"
echo ""

# Instalar dependências Python
echo "📦 Instalando dependências Python..."
pip install -r requirements.txt
echo "✅ Dependências Python instaladas"
echo ""

# Verificar se PySpark foi instalado corretamente
echo "🔍 Verificando PySpark..."
PYSPARK_VERSION=$(python3 -c "import pyspark; print(pyspark.__version__)" 2>/dev/null || echo "não encontrado")
if [ "$PYSPARK_VERSION" != "não encontrado" ]; then
    echo "✅ PySpark instalado: $PYSPARK_VERSION"
    if echo "$PYSPARK_VERSION" | grep -q "4.1"; then
        echo "✅ Versão 4.1.x detectada (suporte a SDP disponível)"
    else
        echo -e "${YELLOW}⚠️  Versão diferente de 4.1.x - SDP pode não estar disponível${NC}"
        echo "   Versão esperada: 4.1.0.dev3"
    fi
else
    echo -e "${YELLOW}⚠️  PySpark não encontrado após instalação${NC}"
    echo "   Tentando instalar PySpark 4.1.0.dev3..."
    pip install "pyspark==4.1.0.dev3"
    echo "✅ PySpark 4.1.0.dev3 instalado"
fi
echo ""

# Configurar Airflow
echo "🔧 Configurando Airflow..."
export AIRFLOW_HOME="${PWD}/airflow_home"
export AIRFLOW__WEBSERVER__WEB_SERVER_PORT=8081
export AIRFLOW__API__PORT=8081

# Criar diretório Airflow (sempre novo após limpeza)
echo "   Criando diretório Airflow..."
mkdir -p "${AIRFLOW_HOME}"
echo "✅ Diretório Airflow criado"

# Inicializar banco de dados do Airflow (sempre novo)
echo "   Inicializando banco de dados do Airflow..."
# Airflow 2.0+ usa 'db migrate' ao invés de 'db init'
airflow db migrate
echo "✅ Banco de dados do Airflow inicializado"
echo ""

# Configurar porta no airflow.cfg se o arquivo existir
if [ -f "${AIRFLOW_HOME}/airflow.cfg" ]; then
    echo "🔧 Configurando porta 8081 no airflow.cfg..."
    # Detectar se é macOS (usa BSD sed) ou Linux (usa GNU sed)
    if [[ "$OSTYPE" == "darwin"* ]]; then
        SED_EXT="''"
    else
        SED_EXT=""
    fi
    
    # Atualizar porta do webserver
    if grep -q "^web_server_port" "${AIRFLOW_HOME}/airflow.cfg"; then
        sed -i $SED_EXT 's/^web_server_port.*/web_server_port = 8081/' "${AIRFLOW_HOME}/airflow.cfg"
    else
        # Adicionar seção [webserver] se não existir
        if ! grep -q "^\[webserver\]" "${AIRFLOW_HOME}/airflow.cfg"; then
            echo "" >> "${AIRFLOW_HOME}/airflow.cfg"
            echo "[webserver]" >> "${AIRFLOW_HOME}/airflow.cfg"
        fi
        # Adicionar porta após [webserver]
        if ! grep -A5 "^\[webserver\]" "${AIRFLOW_HOME}/airflow.cfg" | grep -q "^web_server_port"; then
            # Encontrar linha de [webserver] e adicionar após ela
            awk '/^\[webserver\]/ {print; print "web_server_port = 8081"; next}1' "${AIRFLOW_HOME}/airflow.cfg" > "${AIRFLOW_HOME}/airflow.cfg.tmp" && mv "${AIRFLOW_HOME}/airflow.cfg.tmp" "${AIRFLOW_HOME}/airflow.cfg"
        fi
    fi
    
    # Atualizar porta da API
    # Primeiro, remover todas as entradas 'port' na seção [api] para evitar duplicatas
    if grep -q "^\[api\]" "${AIRFLOW_HOME}/airflow.cfg"; then
        # Remover todas as linhas 'port = ...' dentro da seção [api]
        awk '
            /^\[api\]/ { in_api = 1; print; next }
            /^\[/ { in_api = 0 }
            in_api && /^port\s*=/ { next }  # Pula linhas port dentro de [api]
            { print }
        ' "${AIRFLOW_HOME}/airflow.cfg" > "${AIRFLOW_HOME}/airflow.cfg.tmp" && mv "${AIRFLOW_HOME}/airflow.cfg.tmp" "${AIRFLOW_HOME}/airflow.cfg"
        
        # Remover base_url existente se houver
        awk '
            /^\[api\]/ { in_api = 1; print; next }
            /^\[/ { in_api = 0 }
            in_api && /^base_url\s*=/ { next }  # Pula linhas base_url dentro de [api]
            { print }
        ' "${AIRFLOW_HOME}/airflow.cfg" > "${AIRFLOW_HOME}/airflow.cfg.tmp" && mv "${AIRFLOW_HOME}/airflow.cfg.tmp" "${AIRFLOW_HOME}/airflow.cfg"
        
        # Adicionar porta e base_url após [api]
        awk '/^\[api\]/ {print; print "base_url = http://localhost:8081"; print "port = 8081"; next}1' "${AIRFLOW_HOME}/airflow.cfg" > "${AIRFLOW_HOME}/airflow.cfg.tmp" && mv "${AIRFLOW_HOME}/airflow.cfg.tmp" "${AIRFLOW_HOME}/airflow.cfg"
    fi
    echo "✅ Porta 8081 e base_url configurados no airflow.cfg"
fi
echo ""

# Criar usuário admin do Airflow (opcional - pode ser criado manualmente também)
echo "👤 Configurando usuário admin do Airflow..."
echo "   Opção 1: Usar senha gerada automaticamente pelo standalone (recomendado)"
echo "   Opção 2: Criar usuário com senha fixa"
echo ""
read -p "Deseja criar usuário admin com senha fixa? (s/N): " create_admin

if [[ "$create_admin" =~ ^[Ss]$ ]]; then
    echo "   Criando usuário admin (username: admin, password: admin)..."
    airflow users create \
        --username admin \
        --password admin \
        --firstname Admin \
        --lastname User \
        --role Admin \
        --email admin@example.com \
        2>/dev/null || echo "   ⚠️  Usuário admin já existe ou erro ao criar"
else
    echo "ℹ️  Usando modo standalone: senha será gerada automaticamente e exibida no console"
    echo "   Para ver a senha, procure no console quando o Airflow iniciar"
fi
echo ""

# Criar diretórios necessários
echo "📁 Criando diretórios necessários..."
mkdir -p datasets/stage
mkdir -p datasets/landing
mkdir -p datasets/raw
mkdir -p datasets/trusted
mkdir -p airflow_home/dags
mkdir -p airflow_home/logs
mkdir -p airflow_home/plugins
echo "✅ Diretórios criados"
echo ""

# Copiar DAGs para o diretório do Airflow
echo "📋 Copiando DAGs para o Airflow..."
if [ -d "dags" ]; then
    cp -r dags/* "${AIRFLOW_HOME}/dags/" 2>/dev/null || true
    echo "✅ DAGs copiados"
else
    echo "⚠️  Diretório dags não encontrado"
fi
echo ""

# Configurar variáveis de ambiente
echo "🔧 Configurando variáveis de ambiente..."
cat > .env << EOF
# Airflow
export AIRFLOW_HOME="${PWD}/airflow_home"
export AIRFLOW__CORE__DAGS_FOLDER="${PWD}/dags"
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__WEBSERVER__WEB_SERVER_PORT=8081
export AIRFLOW__API__PORT=8081
export AIRFLOW__API__BASE_URL=http://localhost:8081

# Spark
export SPARK_HOME=\$(python -c "import findspark; findspark.init(); import os; print(os.environ.get('SPARK_HOME', ''))" 2>/dev/null || echo "")

# Python
export PYTHONPATH="${PWD}:${PYTHONPATH}"
EOF

echo "✅ Arquivo .env criado"
echo ""

# Criar script de ativação
echo "📝 Criando script de ativação..."
cat > activate_env.sh << EOF
#!/bin/bash
# Script para ativar o ambiente

source venv/bin/activate
source .env

# Configurar porta 8081 e base_url
export AIRFLOW__WEBSERVER__WEB_SERVER_PORT=8081
export AIRFLOW__API__PORT=8081
export AIRFLOW__API__BASE_URL=http://localhost:8081

# Configurar PYTHONPATH para o projeto (importante para DAGs encontrarem módulos)
export PYTHONPATH="${PWD}:\${PYTHONPATH}"

echo "✅ Ambiente ativado!"
echo ""
echo "Para iniciar o Airflow Standalone (webserver + scheduler juntos):"
echo "  airflow standalone"
echo ""
echo "O Airflow será iniciado na porta 8081 e exibirá a senha temporária."
echo ""
EOF

chmod +x activate_env.sh
echo "✅ Script de ativação criado"
echo ""

# Corrigir duplicatas no airflow.cfg (se houver)
echo "🔍 Verificando e corrigindo duplicatas no airflow.cfg..."
if [ -f "scripts/fix_airflow_cfg_duplicates.py" ]; then
    python3 scripts/fix_airflow_cfg_duplicates.py
    echo ""
fi

# Verificar compatibilidade PySpark + Airflow
echo "🔍 Verificando compatibilidade PySpark + Airflow..."
if [ -f "scripts/verify_pyspark_airflow.py" ]; then
    python3 scripts/verify_pyspark_airflow.py
    echo ""
fi

# Resumo
echo -e "${GREEN}✅ Setup concluído com sucesso!${NC}"
echo ""
echo "📋 Iniciando Airflow em modo Standalone..."
echo ""
echo "ℹ️  O Airflow standalone executa webserver e scheduler juntos."
echo "   Uma senha temporária será gerada e exibida abaixo."
echo ""
echo "⚠️  IMPORTANTE: Certifique-se de que o Airflow está rodando no mesmo"
echo "   ambiente Python onde o PySpark está instalado (venv ativado)."
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "🚀 Iniciando Airflow Standalone na porta 8081..."
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

# Executar Airflow Standalone
# O comando standalone já cria o usuário admin e exibe a senha
airflow standalone

