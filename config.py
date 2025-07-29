import os
import streamlit as st
from decouple import config as decouple_config, UndefinedValueError

# --- Lógica de Detecção de Ambiente CORRIGIDA ---
def _is_streamlit_cloud():
    """
    Detecta se está rodando no Streamlit Cloud de forma segura.
    Retorna False se não conseguir acessar st.secrets (como no GitHub Actions).
    """
    try:
        # Tenta acessar st.secrets de forma segura
        return st.secrets.get("IS_STREAMLIT_CLOUD", False) if hasattr(st, 'secrets') else False
    except Exception:
        # Se der qualquer erro (como no GitHub Actions), assume que não é Streamlit Cloud
        return False

IS_RUNNING_ON_STREAMLIT_CLOUD = _is_streamlit_cloud()

def get_secret(key: str, default: any = None):
    """
    Função auxiliar para buscar segredos.
    Tenta primeiro no st.secrets (para deploy na nuvem) e depois no .env (para desenvolvimento local).
    Se não conseguir acessar st.secrets (como no GitHub Actions), usa apenas variáveis de ambiente.
    """
    if IS_RUNNING_ON_STREAMLIT_CLOUD:
        # No Streamlit Cloud, busca em st.secrets
        try:
            return st.secrets.get(key, default)
        except Exception:
            # Fallback para variáveis de ambiente se st.secrets falhar
            return os.getenv(key, default)
    else:
        # Primeiro tenta variáveis de ambiente do sistema (GitHub Actions, Docker, etc.)
        env_value = os.getenv(key)
        if env_value is not None:
            return env_value
        
        # Se não encontrar, tenta o arquivo .env local
        try:
            return decouple_config(key, default=default)
        except UndefinedValueError:
            # Se a variável não for encontrada nem no .env nem como default
            return default

# --- Configurações do Projeto ---

# API Keys
GROQ_API_KEY = get_secret('GROQ_API_KEY')
GOOGLE_API_KEY = get_secret('GOOGLE_API_KEY')
SERPER_API_KEY = get_secret('SERPER_API_KEY')

# Database
DB_PATH = get_secret('DB_PATH', default='data/ibama_infracao.db')

# Supabase Credentials
SUPABASE_URL = get_secret('SUPABASE_URL')
SUPABASE_KEY = get_secret('SUPABASE_KEY')

# Data Sources
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
