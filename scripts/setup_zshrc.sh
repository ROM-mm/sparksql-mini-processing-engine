#!/bin/bash

# Script para configurar variáveis de ambiente no .zshrc
# Este script detecta e configura JAVA_HOME automaticamente

set -e

# Cores
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

ZSHRC_FILE="$HOME/.zshrc"
BACKUP_FILE="$HOME/.zshrc.backup.$(date +%Y%m%d_%H%M%S)"

echo "🔧 Configurando variáveis de ambiente no .zshrc..."
echo ""

# Fazer backup do .zshrc
if [ -f "$ZSHRC_FILE" ]; then
    echo "📋 Fazendo backup do .zshrc..."
    cp "$ZSHRC_FILE" "$BACKUP_FILE"
    echo "✅ Backup criado: $BACKUP_FILE"
    echo ""
fi

# Detectar Java e configurar JAVA_HOME
echo "🔍 Detectando Java..."
JAVA_HOME_PATH=""

# Tentar encontrar Java no macOS
if [[ "$OSTYPE" == "darwin"* ]]; then
    # Método 1: Usar /usr/libexec/java_home (mais confiável no macOS)
    if command -v /usr/libexec/java_home &> /dev/null; then
        echo "   Verificando versões de Java instaladas..."
        JAVA_VERSIONS=$(/usr/libexec/java_home -V 2>&1 | grep -E "^\s+[0-9]+\.[0-9]+" | awk '{print $1}' | sort -V -r)
        
        if [ -n "$JAVA_VERSIONS" ]; then
            # Tentar Java 21 primeiro (se o usuário mencionou)
            if echo "$JAVA_VERSIONS" | grep -q "^21"; then
                JAVA_HOME_PATH=$(/usr/libexec/java_home -v 21 2>/dev/null || echo "")
                if [ -n "$JAVA_HOME_PATH" ] && [ -d "$JAVA_HOME_PATH" ]; then
                    echo "   ✅ Java 21 encontrado!"
                fi
            fi
            
            # Se não encontrou Java 21, usar a versão mais recente disponível
            if [ -z "$JAVA_HOME_PATH" ] || [ ! -d "$JAVA_HOME_PATH" ]; then
                LATEST_VERSION=$(echo "$JAVA_VERSIONS" | head -n 1)
                if [ -n "$LATEST_VERSION" ]; then
                    JAVA_HOME_PATH=$(/usr/libexec/java_home -v "$LATEST_VERSION" 2>/dev/null || echo "")
                    if [ -n "$JAVA_HOME_PATH" ] && [ -d "$JAVA_HOME_PATH" ]; then
                        echo "   ✅ Java $LATEST_VERSION encontrado (versão mais recente)"
                    fi
                fi
            fi
            
            # Se ainda não encontrou, usar java_home sem versão (padrão do sistema)
            if [ -z "$JAVA_HOME_PATH" ] || [ ! -d "$JAVA_HOME_PATH" ]; then
                JAVA_HOME_PATH=$(/usr/libexec/java_home 2>/dev/null || echo "")
            fi
        fi
    fi
    
    # Método 2: Verificar se Java está instalado via Homebrew (fallback)
    if [ -z "$JAVA_HOME_PATH" ] || [ ! -d "$JAVA_HOME_PATH" ]; then
        if [ -d "/opt/homebrew/opt/openjdk@21" ]; then
            JAVA_HOME_PATH="/opt/homebrew/opt/openjdk@21"
            echo "   ✅ Java 21 encontrado via Homebrew"
        elif [ -d "/opt/homebrew/opt/openjdk" ]; then
            # Encontrar a versão mais recente
            JAVA_HOME_PATH=$(ls -d /opt/homebrew/opt/openjdk@* 2>/dev/null | sort -V -r | head -n 1)
            if [ -z "$JAVA_HOME_PATH" ]; then
                JAVA_HOME_PATH="/opt/homebrew/opt/openjdk"
            fi
        elif [ -d "/usr/local/opt/openjdk@21" ]; then
            JAVA_HOME_PATH="/usr/local/opt/openjdk@21"
            echo "   ✅ Java 21 encontrado via Homebrew (Intel)"
        elif [ -d "/usr/local/opt/openjdk" ]; then
            JAVA_HOME_PATH=$(ls -d /usr/local/opt/openjdk@* 2>/dev/null | sort -V -r | head -n 1)
            if [ -z "$JAVA_HOME_PATH" ]; then
                JAVA_HOME_PATH="/usr/local/opt/openjdk"
            fi
        fi
    fi
fi

# Tentar encontrar Java no Linux
if [[ "$OSTYPE" == "linux-gnu"* ]]; then
    # Verificar JAVA_HOME se já estiver configurado
    if [ -n "$JAVA_HOME" ] && [ -d "$JAVA_HOME" ]; then
        JAVA_HOME_PATH="$JAVA_HOME"
    # Tentar encontrar em locais comuns
    elif [ -d "/usr/lib/jvm" ]; then
        JAVA_HOME_PATH=$(ls -d /usr/lib/jvm/java-*-openjdk* 2>/dev/null | head -n 1)
    elif [ -d "/opt/java" ]; then
        JAVA_HOME_PATH=$(ls -d /opt/java/* 2>/dev/null | head -n 1)
    fi
fi

# Verificar se encontrou Java
if [ -z "$JAVA_HOME_PATH" ] || [ ! -d "$JAVA_HOME_PATH" ]; then
    echo -e "${YELLOW}⚠️  Java não encontrado automaticamente${NC}"
    echo ""
    echo "Versões de Java disponíveis no sistema:"
    if command -v /usr/libexec/java_home &> /dev/null; then
        /usr/libexec/java_home -V 2>&1 | grep -E "^\s+[0-9]+\.[0-9]+" || echo "  Nenhuma versão encontrada"
    else
        echo "  /usr/libexec/java_home não disponível"
    fi
    echo ""
    echo "Para instalar Java 21:"
    echo "  macOS: brew install openjdk@21"
    echo "  Ou baixe de: https://adoptium.net/"
    echo ""
    echo "Ou configure JAVA_HOME manualmente no .zshrc:"
    echo "  export JAVA_HOME=\$(/usr/libexec/java_home -v 21)"
    echo ""
    read -p "Deseja continuar sem configurar JAVA_HOME? (s/N): " continue_without_java
    if [[ ! "$continue_without_java" =~ ^[Ss]$ ]]; then
        echo "❌ Configuração cancelada"
        exit 1
    fi
else
    # Verificar versão do Java encontrado
    JAVA_VERSION_OUTPUT=$("$JAVA_HOME_PATH/bin/java" -version 2>&1 | head -n 1)
    echo -e "${GREEN}✅ Java encontrado: $JAVA_HOME_PATH${NC}"
    echo "   Versão: $JAVA_VERSION_OUTPUT"
fi

# Preparar configurações para adicionar ao .zshrc
CONFIG_BLOCK="# ============================================================================
# SparkSQL Mini Processing Engine - Configurações
# Adicionado automaticamente em $(date)
# ============================================================================

"

if [ -n "$JAVA_HOME_PATH" ] && [ -d "$JAVA_HOME_PATH" ]; then
    CONFIG_BLOCK+="export JAVA_HOME=\"$JAVA_HOME_PATH\"
export PATH=\"\${JAVA_HOME}/bin:\${PATH}\"

"
fi

CONFIG_BLOCK+="# PySpark (instalado via pip não precisa de SPARK_HOME)
# O PySpark gerencia SPARK_HOME automaticamente quando instalado via pip
# Não configure SPARK_HOME a menos que tenha Spark instalado separadamente

# Projeto específico (ajuste o caminho conforme necessário)
# export PYTHONPATH=\"\${HOME}/Documents/projects/develop/declarative-pipelines:\${PYTHONPATH}\"

# ============================================================================
"

# Verificar se já existe configuração do projeto
if grep -q "# SparkSQL Mini Processing Engine" "$ZSHRC_FILE" 2>/dev/null; then
    echo -e "${YELLOW}⚠️  Configuração do projeto já existe no .zshrc${NC}"
    echo ""
    read -p "Deseja atualizar a configuração? (s/N): " update_config
    if [[ "$update_config" =~ ^[Ss]$ ]]; then
        # Remover configuração antiga
        sed -i.bak '/# ============================================================================/,/# ============================================================================/d' "$ZSHRC_FILE" 2>/dev/null || \
        sed -i '' '/# ============================================================================/,/# ============================================================================/d' "$ZSHRC_FILE" 2>/dev/null || true
        echo "✅ Configuração antiga removida"
    else
        echo "ℹ️  Mantendo configuração existente"
        exit 0
    fi
fi

# Adicionar configuração ao .zshrc
echo "📝 Adicionando configurações ao .zshrc..."
echo "" >> "$ZSHRC_FILE"
echo "$CONFIG_BLOCK" >> "$ZSHRC_FILE"
echo -e "${GREEN}✅ Configurações adicionadas ao .zshrc${NC}"
echo ""

# Mostrar resumo
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "📋 Resumo das configurações:"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
if [ -n "$JAVA_HOME_PATH" ]; then
    echo "✅ JAVA_HOME: $JAVA_HOME_PATH"
else
    echo "⚠️  JAVA_HOME: Não configurado"
fi
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo -e "${GREEN}✅ Configuração concluída!${NC}"
echo ""
echo "📝 Próximos passos:"
echo "   1. Recarregue o .zshrc:"
echo "      ${YELLOW}source ~/.zshrc${NC}"
echo ""
echo "   2. Ou abra um novo terminal"
echo ""
echo "   3. Verifique as variáveis:"
echo "      ${YELLOW}echo \$JAVA_HOME${NC}"
echo "      ${YELLOW}java -version${NC}"
echo ""
echo "   4. Execute o setup do projeto:"
echo "      ${YELLOW}make setup${NC}"
echo ""

