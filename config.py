from decouple import config

# API Keys
GROQ_API_KEY = config('GROQ_API_KEY', default=None)
GOOGLE_API_KEY = config('GOOGLE_API_KEY', default=None)

# Database
DB_PATH = config('DB_PATH', default='data/ibama_infracao.db')

# Data Sources
IBAMA_ZIP_URL = config('IBAMA_ZIP_URL')

# App Settings
APP_TITLE = "IBAMA - Análise de Autos de Infração"
APP_ICON = "🌳"

# Cache Settings
CACHE_DIR = "data/cache"
CACHE_MAX_AGE_HOURS = 24

# Data Update Schedule
UPDATE_HOUR = 10  # 10:00 AM Brasília time
UPDATE_TIMEZONE = 'America/Sao_Paulo'