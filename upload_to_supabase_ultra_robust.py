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
import json
import numpy as np
from datetime import datetime

print("🌳 Iniciando processo de upload para o Supabase (JSON Safe)...")

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

# --- 2. Download robusto (mesmo código anterior) ---
def download_with_multiple_methods(url):
    """Tenta múltiplos métodos para baixar o arquivo."""
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
            if content and len(content) > 1000:
                print(f"✅ Sucesso com {method_name}! Tamanho: {len(content):,} bytes")
                return content
            else:
                print(f"⚠️ {method_name}: Conteúdo muito pequeno")
        except Exception as e:
            print(f"❌ {method_name} falhou: {str(e)[:100]}...")
    
    raise Exception("❌ Todos os métodos de download falharam!")

def download_with_requests_no_ssl(url):
    session = requests.Session()
    session.verify = False
    response = session.get(url, timeout=300, 
                          headers={'User-Agent': 'Mozilla/5.0 (compatible; IBAMA-Bot/1.0)'})
    response.raise_for_status()
    return response.content

def download_with_urllib_no_ssl(url):
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE
    
    request = Request(url, headers={'User-Agent': 'Mozilla/5.0 (compatible; IBAMA-Bot/1.0)'})
    with urlopen(request, timeout=300, context=ssl_context) as response:
        return response.read()

def download_with_wget(url):
    try:
        result = subprocess.run([
            'wget', '--no-check-certificate', '--timeout=300', 
            '--user-agent=Mozilla/5.0 (compatible; IBAMA-Bot/1.0)',
            '-O', '-', url
        ], capture_output=True, check=True, timeout=320)
        return result.stdout
    except:
        raise Exception("wget não disponível ou falhou")

def download_with_curl(url):
    try:
        result = subprocess.run([
            'curl', '-k', '--max-time', '300',
            '--user-agent', 'Mozilla/5.0 (compatible; IBAMA-Bot/1.0)',
            '-L', url
        ], capture_output=True, check=True, timeout=320)
        return result.stdout
    except:
        raise Exception("curl não disponível ou falhou")

def download_with_requests_http(url):
    http_url = url.replace('https://', 'http://')
    if http_url == url:
        raise Exception("URL já é HTTP")
    
    session = requests.Session()
    response = session.get(http_url, timeout=300,
                          headers={'User-Agent': 'Mozilla/5.0 (compatible; IBAMA-Bot/1.0)'})
    response.raise_for_status()
    return response.content

# --- 3. Funções de processamento ---
def make_json_serializable(obj):
    """Converte objetos para tipos serializáveis em JSON."""
    if pd.isna(obj):
        return ""
    elif isinstance(obj, (pd.Timestamp, datetime)):
        return obj.strftime('%Y-%m-%d %H:%M:%S') if pd.notna(obj) else ""
    elif isinstance(obj, (np.integer, np.int64)):
        return int(obj)
    elif isinstance(obj, (np.floating, np.float64)):
        return float(obj) if not np.isnan(obj) else ""
    elif isinstance(obj, np.bool_):
        return bool(obj)
    elif isinstance(obj, bytes):
        return obj.decode('utf-8', errors='ignore')
    else:
        return str(obj) if obj is not None else ""

def clean_dataframe_for_json(df):
    """Limpa o DataFrame e garante que todos os dados são serializáveis em JSON."""
    print("🧹 Preparando dados para serialização JSON...")
    
    # Cria uma cópia para não modificar o original
    df_clean = df.copy()
    
    # Remove colunas e linhas completamente vazias
    df_clean = df_clean.dropna(axis=1, how='all')
    df_clean = df_clean.dropna(axis=0, how='all')
    
    # Processa cada coluna
    for col in df_clean.columns:
        print(f"  🔄 Processando coluna: {col} ({df_clean[col].dtype})")
        
        # Aplica a função de conversão para cada valor da coluna
        df_clean[col] = df_clean[col].apply(make_json_serializable)
    
    print(f"  ✅ Limpeza concluída: {len(df_clean)} registros, {len(df_clean.columns)} colunas")
    
    # Teste final de serialização
    print("🔍 Testando serialização JSON...")
    try:
        test_record = df_clean.iloc[0].to_dict()
        json.dumps(test_record)
        print("  ✅ Teste de serialização passou!")
    except Exception as e:
        print(f"  ❌ Ainda há problemas de serialização: {e}")
        raise
    
    return df_clean

def read_csv_robust(zip_file, csv_file):
    """Lê um arquivo CSV de forma robusta."""
    encodings = ['utf-8', 'latin1', 'cp1252', 'iso-8859-1']
    separators = [';', ',', '\t']
    
    for encoding in encodings:
        for sep in separators:
            try:
                with zip_file.open(csv_file) as csv_data:
                    df = pd.read_csv(csv_data, encoding=encoding, sep=sep, low_memory=False)
                    if len(df.columns) > 5 and len(df) > 0:
                        return df
            except Exception:
                continue
    return None

# --- 4. Processamento principal ---
def download_and_process_data():
    """Download e processa os dados do IBAMA."""
    print("📥 Baixando dados do IBAMA...")
    
    try:
        # Download
        content = download_with_multiple_methods(IBAMA_ZIP_URL)
        
        print("📦 Processando arquivo ZIP...")
        
        # Processa ZIP
        with zipfile.ZipFile(BytesIO(content)) as zip_file:
            file_list = zip_file.namelist()
            csv_files = [f for f in file_list if f.endswith('.csv')]
            
            if not csv_files:
                raise ValueError("Nenhum arquivo CSV encontrado no ZIP")
            
            print(f"📄 Total de arquivos CSV: {len(csv_files)}")
            
            # Busca arquivos 2024-2025
            target_files = [f for f in csv_files if any(year in f for year in ['2024', '2025'])]
            
            if target_files:
                print(f"🎯 Arquivos encontrados (2024-2025): {target_files}")
                files_to_process = target_files
            else:
                print("⚠️ Arquivos 2024-2025 não encontrados. Usando os mais recentes...")
                files_to_process = sorted(csv_files, reverse=True)[:5]
            
            # Processa arquivos
            all_dataframes = []
            
            for csv_file in files_to_process:
                print(f"⚙️ Processando: {csv_file}")
                
                try:
                    df_temp = read_csv_robust(zip_file, csv_file)
                    
                    if df_temp is not None and len(df_temp) > 0:
                        print(f"    ✅ {len(df_temp):,} registros, {len(df_temp.columns)} colunas")
                        all_dataframes.append(df_temp)
                    else:
                        print(f"    ⚠️ Arquivo vazio: {csv_file}")
                        
                except Exception as e:
                    print(f"    ❌ Erro: {str(e)[:100]}...")
                    continue
            
            if not all_dataframes:
                raise ValueError("Nenhum arquivo válido processado")
            
            # Combina DataFrames
            print(f"🔄 Combinando {len(all_dataframes)} arquivos...")
            df = pd.concat(all_dataframes, ignore_index=True, sort=False)
            print(f"📊 Dados combinados: {len(df):,} registros")
        
        # Limpa para JSON
        df_clean = clean_dataframe_for_json(df)
        
        return df_clean
        
    except Exception as e:
        print(f"❌ Erro no processamento: {e}")
        raise

# --- 5. Execução principal ---
try:
    df = download_and_process_data()
    
    if df.empty:
        print("❌ Nenhum dado processado.")
        sys.exit(1)
    
    print(f"✅ Dados prontos: {len(df):,} registros, {len(df.columns)} colunas")
    
    # Conecta ao Supabase
    print("🔗 Conectando ao Supabase...")
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    table_name = "ibama_infracao"
    
    # Limpa tabela
    print(f"🧹 Limpando tabela '{table_name}'...")
    try:
        supabase.table(table_name).delete().neq('id', -1).execute()
        print("  ✅ Tabela limpa")
    except Exception as e:
        print(f"❌ Erro ao limpar tabela: {e}")
        raise
    
    # Upload em lotes
    chunk_size = 500  # Reduzido para ser mais seguro
    total_chunks = (len(df) // chunk_size) + 1
    print(f"🚀 Upload: {len(df):,} registros em {total_chunks} lotes de {chunk_size}")
    
    successful_uploads = 0
    failed_uploads = 0
    
    for i in range(0, len(df), chunk_size):
        chunk_index = i // chunk_size + 1
        print(f"  📤 Lote {chunk_index}/{total_chunks}...", end=" ")
        
        chunk = df[i:i + chunk_size]
        
        try:
            # Converte para dict e testa serialização
            data_to_insert = chunk.to_dict(orient='records')
            
            # Teste adicional de serialização no primeiro lote
            if chunk_index == 1:
                json.dumps(data_to_insert[0])  # Testa o primeiro registro
            
            # Upload
            response = supabase.table(table_name).insert(data_to_insert).execute()
            
            if hasattr(response, 'error') and response.error:
                raise Exception(f"API Error: {response.error.message}")
            
            successful_uploads += len(data_to_insert)
            print(f"✅ {len(data_to_insert)} registros")
            
            time.sleep(0.3)  # Pausa de segurança
            
        except Exception as e:
            failed_uploads += len(data_to_insert)
            print(f"❌ {str(e)[:50]}...")
            
            # Para no primeiro erro para debug
            if chunk_index <= 3:
                print(f"🔍 Debug do erro no lote {chunk_index}:")
                print(f"    Tipos de dados: {chunk.dtypes.to_dict()}")
                print(f"    Primeiro registro: {chunk.iloc[0].to_dict()}")
                break
            continue
    
    # Relatório final
    print(f"\n{'='*60}")
    print(f"📊 RELATÓRIO FINAL:")
    print(f"  📥 Total: {len(df):,} registros")
    print(f"  ✅ Sucesso: {successful_uploads:,}")
    print(f"  ❌ Falha: {failed_uploads:,}")
    print(f"  📈 Taxa: {(successful_uploads/len(df))*100:.1f}%")
    print(f"{'='*60}")
    
    if successful_uploads > 0:
        print("🎉 Upload concluído com sucesso!")
        sys.exit(0)
    else:
        print("❌ Upload falhou completamente.")
        sys.exit(1)

except Exception as e:
    print(f"💥 Erro crítico: {e}")
    sys.exit(1)
