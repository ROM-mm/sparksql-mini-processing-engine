.PHONY: help setup install activate start-airflow stop-airflow restart-airflow \
	verify check clean clean-all fix-duplicates setup-zshrc copy-dags status test

# Cores para output
GREEN := \033[0;32m
YELLOW := \033[1;33m
RED := \033[0;31m
NC := \033[0m # No Color

# Variáveis
VENV := venv
ACTIVATE := $(VENV)/bin/activate
AIRFLOW_HOME := $(PWD)/airflow_home

# Help padrão
help: ## Mostra esta mensagem de ajuda
	@echo "$(GREEN)SparkSQL Mini Processing Engine - Comandos Disponíveis$(NC)"
	@echo ""
	@echo "$(YELLOW)Setup e Instalação:$(NC)"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  $(GREEN)%-20s$(NC) %s\n", $$1, $$2}' | grep -E "(setup|install|activate)"
	@echo ""
	@echo "$(YELLOW)Execução:$(NC)"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  $(GREEN)%-20s$(NC) %s\n", $$1, $$2}' | grep -E "(run|start|stop|restart)"
	@echo ""
	@echo "$(YELLOW)Verificação e Troubleshooting:$(NC)"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  $(GREEN)%-20s$(NC) %s\n", $$1, $$2}' | grep -E "(verify|check|fix)"
	@echo ""
	@echo "$(YELLOW)Limpeza:$(NC)"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  $(GREEN)%-20s$(NC) %s\n", $$1, $$2}' | grep -E "(clean)"
	@echo ""
	@echo "$(YELLOW)Exemplos:$(NC)"
	@echo "  $(GREEN)make setup$(NC)              # Instala tudo do zero"
	@echo "  $(GREEN)make start-airflow$(NC)       # Inicia o Airflow"
	@echo "  $(GREEN)make verify$(NC)              # Verifica o ambiente"

# ============================================================================
# Setup e Instalação
# ============================================================================

setup: ## Executa setup completo (remove tudo e instala do zero)
	@echo "$(GREEN)🚀 Executando setup completo...$(NC)"
	@bash setup.sh

install: setup ## Alias para setup (instala tudo do zero)

activate: ## Ativa o ambiente virtual e carrega variáveis
	@echo "$(GREEN)🔧 Ativando ambiente...$(NC)"
	@if [ -f activate_env.sh ]; then \
		bash -c "source activate_env.sh"; \
	else \
		echo "$(YELLOW)⚠️  activate_env.sh não encontrado. Execute 'make setup' primeiro.$(NC)"; \
		exit 1; \
	fi

setup-zshrc: ## Configura variáveis de ambiente no .zshrc (JAVA_HOME, etc)
	@echo "$(GREEN)🔧 Configurando .zshrc...$(NC)"
	@bash scripts/setup_zshrc.sh

copy-dags: ## Copia DAGs atualizados para airflow_home/dags
	@echo "$(GREEN)📋 Copiando DAGs...$(NC)"
	@if [ -d "dags" ]; then \
		cp -r dags/* "$(AIRFLOW_HOME)/dags/" 2>/dev/null || true; \
		echo "$(GREEN)✅ DAGs copiados$(NC)"; \
	else \
		echo "$(YELLOW)⚠️  Diretório dags não encontrado$(NC)"; \
	fi

# ============================================================================
# Execução
# ============================================================================

start-airflow: ## Inicia o Airflow em modo standalone
	@echo "$(GREEN)🚀 Iniciando Airflow...$(NC)"
	@if [ -f activate_env.sh ]; then \
		bash -c "source activate_env.sh && airflow standalone"; \
	else \
		echo "$(YELLOW)⚠️  Ambiente não configurado. Execute 'make setup' primeiro.$(NC)"; \
		exit 1; \
	fi

stop-airflow: ## Para todos os processos do Airflow
	@echo "$(YELLOW)🛑 Parando Airflow...$(NC)"
	@pkill -f airflow || echo "$(YELLOW)⚠️  Nenhum processo do Airflow encontrado$(NC)"
	@echo "$(GREEN)✅ Airflow parado$(NC)"

restart-airflow: stop-airflow start-airflow ## Reinicia o Airflow (para e inicia)

# ============================================================================
# Verificação e Troubleshooting
# ============================================================================

verify: ## Verifica compatibilidade PySpark + Airflow
	@echo "$(GREEN)🔍 Verificando ambiente...$(NC)"
	@if [ -f "$(ACTIVATE)" ]; then \
		bash -c "source $(ACTIVATE) && python3 scripts/verify_pyspark_airflow.py"; \
	else \
		echo "$(YELLOW)⚠️  Ambiente virtual não encontrado. Execute 'make setup' primeiro.$(NC)"; \
		exit 1; \
	fi

check: verify ## Alias para verify

fix-duplicates: ## Corrige duplicatas no airflow.cfg
	@echo "$(GREEN)🔧 Corrigindo duplicatas...$(NC)"
	@if [ -f "$(ACTIVATE)" ]; then \
		bash -c "source $(ACTIVATE) && python3 scripts/fix_airflow_cfg_duplicates.py"; \
	else \
		echo "$(YELLOW)⚠️  Ambiente virtual não encontrado. Execute 'make setup' primeiro.$(NC)"; \
		exit 1; \
	fi

# ============================================================================
# Limpeza
# ============================================================================

clean: ## Remove arquivos temporários e caches
	@echo "$(YELLOW)🧹 Limpando arquivos temporários...$(NC)"
	@find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	@find . -type f -name "*.pyc" -delete 2>/dev/null || true
	@find . -type f -name "*.pyo" -delete 2>/dev/null || true
	@find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true
	@echo "$(GREEN)✅ Limpeza concluída$(NC)"

clean-all: clean ## Remove tudo (venv, airflow_home, .env, etc) - CUIDADO!
	@echo "$(RED)⚠️  ATENÇÃO: Isso irá remover TODAS as configurações!$(NC)"
	@read -p "Tem certeza? (s/N): " confirm; \
	if [ "$$confirm" = "s" ] || [ "$$confirm" = "S" ]; then \
		echo "$(YELLOW)🧹 Removendo tudo...$(NC)"; \
		rm -rf $(VENV) 2>/dev/null || true; \
		rm -rf $(AIRFLOW_HOME) 2>/dev/null || true; \
		rm -f .env 2>/dev/null || true; \
		rm -f activate_env.sh 2>/dev/null || true; \
		echo "$(GREEN)✅ Tudo removido$(NC)"; \
	else \
		echo "$(YELLOW)Operação cancelada$(NC)"; \
	fi

# ============================================================================
# Comandos Úteis
# ============================================================================

status: ## Mostra status do ambiente
	@echo "$(GREEN)📊 Status do Ambiente$(NC)"
	@echo ""
	@echo "$(YELLOW)Ambiente Virtual:$(NC)"
	@if [ -d "$(VENV)" ]; then \
		echo "  ✅ Existe"; \
		if [ -f "$(ACTIVATE)" ]; then \
			echo "  ✅ Ativável"; \
		fi; \
	else \
		echo "  ❌ Não existe"; \
	fi
	@echo ""
	@echo "$(YELLOW)Airflow:$(NC)"
	@if [ -d "$(AIRFLOW_HOME)" ]; then \
		echo "  ✅ Diretório existe"; \
		if [ -f "$(AIRFLOW_HOME)/airflow.db" ]; then \
			echo "  ✅ Banco de dados existe"; \
		else \
			echo "  ❌ Banco de dados não existe"; \
		fi; \
	else \
		echo "  ❌ Diretório não existe"; \
	fi
	@if pgrep -f "airflow" > /dev/null; then \
		echo "  ✅ Airflow está rodando"; \
	else \
		echo "  ❌ Airflow não está rodando"; \
	fi
	@echo ""
	@echo "$(YELLOW)Arquivos de Configuração:$(NC)"
	@if [ -f ".env" ]; then echo "  ✅ .env existe"; else echo "  ❌ .env não existe"; fi
	@if [ -f "activate_env.sh" ]; then echo "  ✅ activate_env.sh existe"; else echo "  ❌ activate_env.sh não existe"; fi
	@echo ""
	@echo "$(YELLOW)Python:$(NC)"
	@python3 --version 2>/dev/null || echo "  ❌ Python não encontrado"
	@echo ""
	@echo "$(YELLOW)Java:$(NC)"
	@java -version 2>&1 | head -n 1 || echo "  ❌ Java não encontrado"

test: ## Executa testes (se existirem)
	@echo "$(GREEN)🧪 Executando testes...$(NC)"
	@if [ -f "$(ACTIVATE)" ]; then \
		bash -c "source $(ACTIVATE) && python3 -m pytest tests/ -v 2>/dev/null || echo '$(YELLOW)⚠️  Nenhum teste encontrado$(NC)'"; \
	else \
		echo "$(YELLOW)⚠️  Ambiente virtual não encontrado. Execute 'make setup' primeiro.$(NC)"; \
	fi

# Comando padrão
.DEFAULT_GOAL := help

