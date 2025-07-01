import os
import streamlit as st
from decouple import config as decouple_config, UndefinedValueError

# --- Lógica de Detecção de Ambiente ---
# Verifica se o app está rodando no Streamlit Community Cloud
IS_RUNNING_ON_STREAMLIT_CLOUD = "STREAMLIT_SERVER_RUNNING_IN_CLOUD" in os.environ

def get_secret(key: str, default: any = None):
    """
    Função auxiliar para buscar segredos.
    Tenta primeiro no st.secrets (para deploy) e depois no .env (para desenvolvimento local).
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
# Agora usamos nossa função auxiliar para buscar as chaves
GROQ_API_KEY = get_secret('GROQ_API_KEY')
GOOGLE_API_KEY = get_secret('GOOGLE_API_KEY')
SERPER_API_KEY = get_secret('SERPER_API_KEY') # Adicionando a chave da Serper

# Database
# O caminho do banco de dados pode ser fixo, pois ele será criado no ambiente de execução
DB_PATH = get_secret('DB_PATH', default='data/ibama_infracao.db')

# Supabase Credentials
SUPABASE_URL = get_secret('SUPABASE_URL')
SUPABASE_KEY = get_secret('SUPABASE_KEY')

# Data Sources
# A URL dos dados pode ser fixa, mas é bom mantê-la configurável
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

# Data Update Schedule
UPDATE_HOUR = 10  # 10:00 AM Brasília time
UPDATE_TIMEZONE = 'America/Sao_Paulo'
