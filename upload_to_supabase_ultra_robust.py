import pandas as pd
from supabase import create_client, Client
import time
import os
import sys
import zipfile
import requests
from io import BytesIO
import urllib3
import ssl
from urllib.request import urlopen, Request
from urllib.error import URLError
import subprocess

print("🌳 Iniciando processo de upload para o Supabase (Versão Ultra Robusta)...")

# --- 1. Configuração de variáveis de ambiente ---
def get_env_var(key: str, default: str = None) -> str:
    """Obtém variável de ambiente com fallback."""
    value = os.getenv(key, default)
    if not value:
        raise ValueError(f"Variável de ambiente {key} não encontrada!")
    return value

# Configurações
SUPABASE_URL = get_env_var("SUPABASE_URL")
SUPABASE_KEY = get_env_var("SUPABASE_KEY")
IBAMA_ZIP_URL = get_env_var(
    "IBAMA_ZIP_URL", 
    "https://dadosabertos.ibama.gov.br/dados/SIFISC/auto_infracao/auto_infracao/auto_infracao_csv.zip"
)

print(f"Configurações carregadas:")
print(f"  - Supabase URL: {SUPABASE_URL[:50]}...")
print(f"  - IBAMA ZIP URL: {IBAMA_ZIP_URL}")

# --- 2. Download ultra robusto ---
def download_with_multiple_methods(url):
    """Tenta múltiplos métodos para baixar o arquivo."""
    
    # Suprimir avisos de SSL
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    
    methods = [
        ("requests_no_ssl", lambda: download_with_requests_no_ssl(url)),
        ("urllib_no_ssl", lambda: download_with_urllib_no_ssl(url)),
        ("wget", lambda: download_with_wget(url)),
        ("curl", lambda: download_with_curl(url)),
        ("requests_http", lambda: download_with_requests_http(url)),
    ]
    
    for method_name, method_func in methods:
        print(f"🔄 Tentando método: {method_name}")
        try:
            content = method_func()
            if content and len(content) > 1000:  # Verifica se baixou algo substancial
                print(f"✅ Sucesso com {method_name}! Tamanho: {len(content):,} bytes")
                return content
            else:
                print(f"⚠️ {method_name}: Conteúdo muito pequeno ou vazio")
        except Exception as e:
            print(f"❌ {method_name} falhou: {str(e)[:100]}...")
    
    raise Exception("❌ Todos os métodos de download falharam!")

def download_with_requests_no_ssl(url):
    """Download usando requests sem verificação SSL."""
    session = requests.Session()
    session.verify = False
    response = session.get(url, timeout=300, 
                          headers={'User-Agent': 'Mozilla/5.0 (compatible; IBAMA-Bot/1.0)'})
    response.raise_for_status()
    return response.content

def download_with_urllib_no_ssl(url):
    """Download usando urllib sem verificação SSL."""
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE
    
    request = Request(url, headers={'User-Agent': 'Mozilla/5.0 (compatible; IBAMA-Bot/1.0)'})
    with urlopen(request, timeout=300, context=ssl_context) as response:
        return response.read()

def download_with_wget(url):
    """Download usando wget (se disponível)."""
    try:
        result = subprocess.run([
            'wget', '--no-check-certificate', '--timeout=300', 
            '--user-agent=Mozilla/5.0 (compatible; IBAMA-Bot/1.0)',
            '-O', '-', url
        ], capture_output=True, check=True, timeout=320)
        return result.stdout
    except (subprocess.CalledProcessError, FileNotFoundError, subprocess.TimeoutExpired):
        raise Exception("wget não disponível ou falhou")

def download_with_curl(url):
    """Download usando curl (se disponível)."""
    try:
        result = subprocess.run([
            'curl', '-k', '--max-time', '300',
            '--user-agent', 'Mozilla/5.0 (compatible; IBAMA-Bot/1.0)',
            '-L', url
        ], capture_output=True, check=True, timeout=320)
        return result.stdout
    except (subprocess.CalledProcessError, FileNotFoundError, subprocess.TimeoutExpired):
        raise Exception("curl não disponível ou falhou")

def download_with_requests_http(url):
    """Tenta a versão HTTP da URL."""
    http_url = url.replace('https://', 'http://')
    if http_url == url:
        raise Exception("URL já é HTTP")
    
    session = requests.Session()
    response = session.get(http_url, timeout=300,
                          headers={'User-Agent': 'Mozilla/5.0 (compatible; IBAMA-Bot/1.0)'})
    response.raise_for_status()
    return response.content

# --- 3. Processamento dos dados ---
def download_and_process_data():
    """Download e processa os dados do IBAMA."""
    print("📥 Baixando dados do IBAMA...")
    
    try:
        # Download ultra robusto
        content = download_with_multiple_methods(IBAMA_ZIP_URL)
        
        print("📦 Processando arquivo ZIP...")
        
        # Extrai o ZIP em memória
        with zipfile.ZipFile(BytesIO(content)) as zip_file:
            # Lista arquivos no ZIP
            file_list = zip_file.namelist()
            csv_files = [f for f in file_list if f.endswith('.csv')]
            
            if not csv_files:
                raise ValueError("Nenhum arquivo CSV encontrado no ZIP")
            
            print(f"📄 Arquivos CSV encontrados: {csv_files}")
            
            # Processa o primeiro arquivo CSV
            csv_file = csv_files[0]
            print(f"⚙️ Processando arquivo: {csv_file}")
            
            # Lê o CSV com múltiplas tentativas de encoding
            df = None
            encodings = ['utf-8', 'latin1', 'cp1252', 'iso-8859-1']
            separators = [';', ',', '\t']
            
            for encoding in encodings:
                for sep in separators:
                    try:
                        with zip_file.open(csv_file) as csv_data:
                            df = pd.read_csv(csv_data, encoding=encoding, sep=sep, low_memory=False)
                            if len(df.columns) > 5:  # Verifica se os dados fazem sentido
                                print(f"✅ CSV lido com sucesso (encoding: {encoding}, sep: '{sep}')")
                                break
                    except Exception as e:
                        continue
                if df is not None and len(df.columns) > 5:
                    break
            
            if df is None or len(df.columns) <= 5:
                raise ValueError("Não foi possível ler o arquivo CSV com nenhuma configuração")
                
        print(f"📊 Dados carregados. Shape: {df.shape}")
        print(f"📋 Colunas: {list(df.columns)[:10]}...")  # Mostra apenas as primeiras 10
        
        # Filtra dados dos últimos 2 anos (2024-2025) se a coluna existir
        original_size = len(df)
        if 'DAT_HORA_AUTO_INFRACAO' in df.columns:
            try:
                # Converte a coluna de data
                df['DAT_HORA_AUTO_INFRACAO'] = pd.to_datetime(df['DAT_HORA_AUTO_INFRACAO'], errors='coerce')
                
                # Filtra pelos anos 2024 e 2025
                df = df[df['DAT_HORA_AUTO_INFRACAO'].dt.year.isin([2024, 2025])]
                print(f"📅 Dados filtrados (2024-2025): {original_size:,} → {len(df):,} registros")
            except Exception as e:
                print(f"⚠️ Erro ao filtrar por data, usando todos os dados: {e}")
        else:
            print("⚠️ Coluna de data não encontrada, usando todos os dados")
        
        # Limpeza final
        df = df.fillna('')  # Remove NaN
        
        # Remove colunas completamente vazias
        df = df.dropna(axis=1, how='all')
        
        print(f"🎯 Dados finais prontos: {len(df):,} registros, {len(df.columns)} colunas")
        
        return df
        
    except Exception as e:
        print(f"❌ Erro ao baixar/processar dados: {e}")
        raise

# --- 4. Execução principal ---
try:
    df = download_and_process_data()
    
    if df.empty:
        print("❌ Nenhum dado foi processado. Encerrando.")
        sys.exit(1)
    
    print(f"✅ Dados processados com sucesso. Total de {len(df):,} registros.")
    
    # --- 5. Configurar o cliente do Supabase ---
    print("🔗 Conectando ao Supabase...")
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    table_name = "ibama_infracao"
    
    # --- 6. Limpar a tabela existente ---
    print(f"🧹 Limpando a tabela '{table_name}' no Supabase...")
    try:
        delete_response = supabase.table(table_name).delete().neq('id', -1).execute()
        print("  ✅ Tabela limpa com sucesso.")
    except Exception as e:
        print(f"❌ Erro ao limpar a tabela: {e}")
        raise
    
    # --- 7. Upload dos dados em lotes ---
    chunk_size = 500
    total_chunks = (len(df) // chunk_size) + 1
    print(f"🚀 Iniciando upload de {len(df):,} registros em {total_chunks} lotes de {chunk_size}...")
    
    successful_uploads = 0
    failed_uploads = 0
    
    for i in range(0, len(df), chunk_size):
        chunk_index = i // chunk_size + 1
        print(f"  📤 Processando lote {chunk_index}/{total_chunks}...")
        
        chunk = df[i:i + chunk_size]
        data_to_insert = chunk.to_dict(orient='records')
        
        try:
            response = supabase.table(table_name).insert(data_to_insert).execute()
            
            if hasattr(response, 'error') and response.error:
                raise Exception(f"Erro da API do Supabase: {response.error.message}")
            
            successful_uploads += len(data_to_insert)
            print(f"    ✅ Lote {chunk_index} enviado ({len(data_to_insert)} registros)")
            
            time.sleep(0.5)  # Pausa menor para não sobrecarregar
            
        except Exception as e:
            failed_uploads += len(data_to_insert)
            print(f"    ❌ Falha no lote {chunk_index}: {str(e)[:100]}...")
            continue
    
    # --- 8. Relatório final ---
    print(f"\n{'='*60}")
    print(f"📊 RELATÓRIO FINAL:")
    print(f"  📥 Total de registros processados: {len(df):,}")
    print(f"  ✅ Uploads bem-sucedidos: {successful_uploads:,}")
    print(f"  ❌ Uploads falharam: {failed_uploads:,}")
    print(f"  📈 Taxa de sucesso: {(successful_uploads/len(df))*100:.1f}%")
    print(f"{'='*60}")
    
    if failed_uploads == 0:
        print("🎉 Upload para o Supabase concluído com sucesso!")
        sys.exit(0)
    else:
        print("⚠️  Upload concluído com algumas falhas.")
        sys.exit(1 if failed_uploads > successful_uploads else 0)

except Exception as e:
    print(f"💥 Erro crítico: {e}")
    sys.exit(1)
