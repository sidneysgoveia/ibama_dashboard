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
import re

print("🌳 Iniciando processo de upload CORRIGIDO para o Supabase...")

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

# --- 2. Download robusto (mantido do script original) ---
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

# --- 3. Funções de processamento CORRIGIDAS ---
def make_json_serializable(obj):
    """Converte objetos para tipos serializáveis em JSON."""
    if pd.isna(obj):
        return None  # CORREÇÃO: Retorna None em vez de string vazia
    elif isinstance(obj, (pd.Timestamp, datetime)):
        return obj.strftime('%Y-%m-%d %H:%M:%S') if pd.notna(obj) else None
    elif isinstance(obj, (np.integer, np.int64)):
        return int(obj)
    elif isinstance(obj, (np.floating, np.float64)):
        if np.isnan(obj):
            return None  # CORREÇÃO: Retorna None para NaN
        return float(obj)
    elif isinstance(obj, np.bool_):
        return bool(obj)
    elif isinstance(obj, bytes):
        return obj.decode('utf-8', errors='ignore')
    elif isinstance(obj, str):
        # CORREÇÃO: Limpa strings problemáticas
        cleaned = str(obj).strip()
        return cleaned if cleaned else None
    else:
        return str(obj) if obj is not None else None

def clean_column_name(col_name: str) -> str:
    """Limpa nomes de colunas para compatibilidade com PostgreSQL."""
    # Remove caracteres especiais e espaços
    clean_name = re.sub(r'[^\w]', '_', str(col_name))
    # Remove underscores duplos/múltiplos
    clean_name = re.sub(r'_+', '_', clean_name)
    # Remove underscore no início/fim
    clean_name = clean_name.strip('_')
    # Força lowercase para PostgreSQL
    return clean_name.lower()

def clean_dataframe_for_supabase(df):
    """Limpa o DataFrame para compatibilidade com Supabase/PostgreSQL."""
    print("🧹 Preparando dados para Supabase/PostgreSQL...")
    
    df_clean = df.copy()
    
    # 1. Limpa nomes das colunas
    print("  🔄 Limpando nomes das colunas...")
    original_columns = df_clean.columns.tolist()
    df_clean.columns = [clean_column_name(col) for col in df_clean.columns]
    
    # Log das mudanças de colunas
    for old, new in zip(original_columns, df_clean.columns):
        if old != new:
            print(f"    📝 {old} → {new}")
    
    # 2. Remove colunas e linhas completamente vazias
    df_clean = df_clean.dropna(axis=1, how='all')
    df_clean = df_clean.dropna(axis=0, how='all')
    
    # 3. Processa cada coluna
    for col in df_clean.columns:
        original_type = df_clean[col].dtype
        print(f"  🔄 Processando coluna: {col} ({original_type})")
        
        # Aplica a função de conversão
        df_clean[col] = df_clean[col].apply(make_json_serializable)
        
        # CORREÇÃO ESPECIAL: Trata colunas numéricas problemáticas
        if col in ['cd_receita_auto_infracao', 'seq_auto_infracao', 'cod_municipio', 'cod_infracao']:
            try:
                # Converte para numeric, colocando None para valores inválidos
                df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce')
                # Substitui NaN por None
                df_clean[col] = df_clean[col].where(pd.notna(df_clean[col]), None)
            except:
                print(f"    ⚠️ Erro na conversão numérica de {col}")
    
    print(f"  ✅ Limpeza concluída: {len(df_clean)} registros, {len(df_clean.columns)} colunas")
    
    # 4. Teste de serialização
    print("🔍 Testando serialização JSON...")
    try:
        test_record = df_clean.iloc[0].to_dict()
        json.dumps(test_record, default=str)  # CORREÇÃO: Adiciona default=str
        print("  ✅ Teste de serialização passou!")
    except Exception as e:
        print(f"  ❌ Problema na serialização: {e}")
        
        # Debug: identifica campos problemáticos
        for key, value in test_record.items():
            try:
                json.dumps({key: value}, default=str)
            except:
                print(f"    🚨 Campo problemático: {key} = {type(value)} {value}")
        
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
        
        # Limpa para Supabase
        df_clean = clean_dataframe_for_supabase(df)
        
        return df_clean
        
    except Exception as e:
        print(f"❌ Erro no processamento: {e}")
        raise

# --- 5. Função para testar conectividade e schema ---
def test_supabase_connection(supabase: Client):
    """Testa a conexão e verifica o schema da tabela."""
    try:
        print("🔍 Testando conexão com Supabase...")
        
        # Testa conexão básica
        result = supabase.table('ibama_infracao').select('*').limit(1).execute()
        print("  ✅ Conexão estabelecida")
        
        return True
        
    except Exception as e:
        print(f"  ❌ Erro na conexão: {e}")
        return False

def safe_upload_batch(supabase: Client, table_name: str, data_batch: list, batch_index: int):
    """Upload seguro de um lote com tratamento de erros aprimorado."""
    try:
        # CORREÇÃO: Trata valores None e tipos problemáticos
        cleaned_batch = []
        for record in data_batch:
            cleaned_record = {}
            for key, value in record.items():
                # Remove chaves vazias ou None
                if key and key.strip():
                    # Converte valores problemáticos
                    if pd.isna(value):
                        cleaned_record[key] = None
                    elif isinstance(value, (list, dict)):
                        cleaned_record[key] = json.dumps(value) if value else None
                    else:
                        cleaned_record[key] = value
            cleaned_batch.append(cleaned_record)
        
        # Upload do lote limpo
        response = supabase.table(table_name).insert(cleaned_batch).execute()
        
        # Verifica se houve erro
        if hasattr(response, 'error') and response.error:
            raise Exception(f"Erro da API: {response.error}")
        
        return True, len(cleaned_batch)
        
    except Exception as e:
        error_msg = str(e)
        
        # Log detalhado para debug
        if batch_index <= 3:  # Só para os primeiros lotes
            print(f"🔍 DEBUG - Lote {batch_index}:")
            print(f"  Tamanho do lote: {len(data_batch)}")
            print(f"  Primeiro registro: {list(data_batch[0].keys())[:10]}...")
            print(f"  Erro: {error_msg[:200]}...")
        
        return False, error_msg

# --- 6. Execução principal CORRIGIDA ---
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
    
    # Testa conectividade
    if not test_supabase_connection(supabase):
        print("❌ Falha na conexão com Supabase")
        sys.exit(1)
    
    # Limpa tabela
    print(f"🧹 Limpando tabela '{table_name}'...")
    try:
        # CORREÇÃO: Usa método mais eficiente para limpeza
        delete_result = supabase.table(table_name).delete().neq('id', -1).execute()
        print("  ✅ Tabela limpa")
    except Exception as e:
        print(f"❌ Erro ao limpar tabela: {e}")
        print("⚠️ Continuando sem limpeza...")
    
    # Upload em lotes com tratamento de erro aprimorado
    chunk_size = 100  # CORREÇÃO: Reduzido para lotes menores
    total_chunks = (len(df) // chunk_size) + 1
    print(f"🚀 Upload: {len(df):,} registros em {total_chunks} lotes de {chunk_size}")
    
    successful_uploads = 0
    failed_uploads = 0
    errors_log = []
    
    for i in range(0, len(df), chunk_size):
        chunk_index = i // chunk_size + 1
        print(f"  📤 Lote {chunk_index}/{total_chunks}...", end=" ")
        
        chunk = df[i:i + chunk_size]
        
        # Converte para dict
        data_to_insert = chunk.to_dict(orient='records')
        
        # Upload seguro
        success, result = safe_upload_batch(supabase, table_name, data_to_insert, chunk_index)
        
        if success:
            successful_uploads += result
            print(f"✅ {result} registros")
        else:
            failed_uploads += len(data_to_insert)
            errors_log.append(f"Lote {chunk_index}: {result}")
            print(f"❌ Erro")
            
            # Para nos primeiros erros para análise
            if chunk_index <= 3:
                print(f"  📋 Erro detalhado: {result}")
                
                # Tenta upload unitário para identificar o registro problemático
                print("  🔍 Tentando upload unitário...")
                for j, record in enumerate(data_to_insert[:5]):  # Só os primeiros 5
                    try:
                        single_result = supabase.table(table_name).insert([record]).execute()
                        print(f"    ✅ Registro {j+1}: OK")
                    except Exception as single_error:
                        print(f"    ❌ Registro {j+1}: {str(single_error)[:100]}...")
                        print(f"      📄 Dados: {list(record.keys())[:10]}...")
                break  # Para após 3 lotes com erro
        
        # Pausa de segurança
        time.sleep(0.1)
    
    # Relatório final
    print(f"\n{'='*60}")
    print(f"📊 RELATÓRIO FINAL:")
    print(f"  📥 Total: {len(df):,} registros")
    print(f"  ✅ Sucesso: {successful_uploads:,}")
    print(f"  ❌ Falha: {failed_uploads:,}")
    
    if len(df) > 0:
        success_rate = (successful_uploads / len(df)) * 100
        print(f"  📈 Taxa de sucesso: {success_rate:.1f}%")
    
    # Log de erros
    if errors_log:
        print(f"\n🚨 ERROS ENCONTRADOS:")
        for error in errors_log[:10]:  # Máximo 10 erros
            print(f"  • {error}")
    
    print(f"{'='*60}")
    
    if successful_uploads > len(df) * 0.8:  # 80% ou mais
        print("🎉 Upload concluído com sucesso (≥80%)!")
        sys.exit(0)
    elif successful_uploads > 0:
        print("⚠️ Upload parcialmente concluído.")
        sys.exit(0)
    else:
        print("❌ Upload falhou completamente.")
        sys.exit(1)

except Exception as e:
    print(f"💥 Erro crítico: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
