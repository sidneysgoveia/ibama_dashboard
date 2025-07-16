import os
import streamlit as st
from decouple import config as decouple_config, UndefinedValueError

# --- Lógica de Detecção de Ambiente ---
# ALTERAÇÃO: A forma mais robusta de detectar o ambiente é verificar se um segredo específico do Streamlit Cloud foi carregado.
# Isso requer que você adicione um segredo chamado `IS_STREAMLIT_CLOUD` com o valor `True`
# nas configurações do seu aplicativo no painel do Streamlit Cloud.
IS_RUNNING_ON_STREAMLIT_CLOUD = st.secrets.get("IS_STREAMLIT_CLOUD", False)

def get_secret(key: str, default: any = None):
    """
    Função auxiliar para buscar segredos.
    Tenta primeiro no st.secrets (para deploy na nuvem) e depois no .env (para desenvolvimento local).
    """
    if IS_RUNNING_ON_STREAMLIT_CLOUD:
        # No Streamlit Cloud, busca em st.secrets
        return st.secrets.get(key, default)
    else:
        # Localmente, usa a biblioteca decouple para ler do .env
        try:
            return decouple_config(key, default=default)
        except UndefinedValueError:
            # Se a variável não for encontrada nem no .env nem como default
            return default

# --- Configurações do Projeto ---

# API Keys
# A função auxiliar `get_secret` busca as chaves do ambiente correto (nuvem ou local)
GROQ_API_KEY = get_secret('GROQ_API_KEY')
GOOGLE_API_KEY = get_secret('GOOGLE_API_KEY')
SERPER_API_KEY = get_secret('SERPER_API_KEY')

# Database
# O caminho do banco de dados local é usado apenas se não estiver na nuvem.
DB_PATH = get_secret('DB_PATH', default='data/ibama_infracao.db')

# Supabase Credentials
# Estas credenciais serão lidas do st.secrets quando em produção.
SUPABASE_URL = get_secret('SUPABASE_URL')
SUPABASE_KEY = get_secret('SUPABASE_KEY')

# Data Sources
# A URL dos dados pode ser fixa, mas é bom mantê-la configurável.
IBAMA_ZIP_URL = get_secret(
    'IBAMA_ZIP_URL',
    default='https://dadosabertos.ibama.gov.br/dados/SIFISC/auto_infracao/auto_infracao/auto_infracao_csv.zip'
)

# App Settings
APP_TITLE = "IBAMA - Análise de Autos de Infração"
APP_ICON = "🌳"

# Cache Settings
CACHE_DIR = "data/cache"
CACHE_MAX_AGE_HOURS = 24

# Data Update Schedule (Informativo, já que a lógica está no GitHub Actions)
UPDATE_HOUR = 10  # 10:00 AM Brasília time
UPDATE_TIMEZONE = 'America/Sao_Paulo'
