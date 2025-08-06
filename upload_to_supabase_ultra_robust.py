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
import subprocess
import json
import numpy as np
from datetime import datetime

print("🌳 IBAMA Upload AUTO-FIX - Correção Automática de Schema v3.0...")

# --- 1. Configuração ---
def get_env_var(key: str, default: str = None) -> str:
    value = os.getenv(key, default)
    if not value:
        raise ValueError(f"Variável de ambiente {key} não encontrada!")
    return value

SUPABASE_URL = get_env_var("SUPABASE_URL")
SUPABASE_KEY = get_env_var("SUPABASE_KEY")
IBAMA_ZIP_URL = get_env_var(
    "IBAMA_ZIP_URL", 
    "https://dadosabertos.ibama.gov.br/dados/SIFISC/auto_infracao/auto_infracao/auto_infracao_csv.zip"
)

print(f"📋 Configurações:")
print(f"  - Supabase URL: {SUPABASE_URL[:50]}...")

# --- 2. DETECÇÃO AUTOMÁTICA DE SCHEMA ---
def get_existing_supabase_columns(supabase_client):
    """Obtém colunas existentes no Supabase através de uma query SQL."""
    try:
        print("🔍 Verificando estrutura atual da tabela...")
        
        # Usa query SQL para obter informações das colunas
        query = """
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns 
        WHERE table_name = 'ibama_infracao' 
        AND table_schema = 'public'
        ORDER BY ordinal_position;
        """
        
        result = supabase_client.rpc('exec_sql', {'query': query}).execute()
        
        if result.data:
            columns = [row['column_name'] for row in result.data]
            print(f"✅ Encontradas {len(columns)} colunas na tabela")
            return columns
        else:
            print("⚠️ Nenhuma coluna encontrada - tabela pode não existir")
            return []
            
    except Exception as e:
        print(f"❌ Erro ao consultar schema: {e}")
        
        # Fallback: tenta buscar um registro
        try:
            result = supabase_client.table('ibama_infracao').select('*').limit(1).execute()
            if result.data:
                columns = list(result.data[0].keys())
                print(f"✅ Fallback: {len(columns)} colunas encontradas")
                return columns
            else:
                print("⚠️ Tabela existe mas está vazia")
                return []
        except Exception as e2:
            print(f"❌ Fallback também falhou: {e2}")
            return []

def create_missing_columns_sql(supabase_client, missing_columns, sample_data):
    """Cria colunas faltantes usando SQL."""
    print(f"🔧 Criando {len(missing_columns)} colunas faltantes...")
    
    created_columns = []
    failed_columns = []
    
    for col_name in missing_columns:
        try:
            # Detecta tipo baseado nos dados de amostra
            sample_value = sample_data.get(col_name)
            
            if pd.isna(sample_value) or sample_value is None:
                sql_type = "TEXT"
            elif isinstance(sample_value, (int, np.integer)):
                sql_type = "BIGINT"
            elif isinstance(sample_value, (float, np.floating)):
                sql_type = "DOUBLE PRECISION"  
            elif isinstance(sample_value, bool):
                sql_type = "BOOLEAN"
            else:
                sql_type = "TEXT"
            
            # SQL para criar coluna
            alter_sql = f'ALTER TABLE public.ibama_infracao ADD COLUMN IF NOT EXISTS "{col_name}" {sql_type};'
            
            print(f"  📝 Criando: {col_name} ({sql_type})")
            
            # Executa SQL
            result = supabase_client.rpc('exec_sql', {'query': alter_sql}).execute()
            created_columns.append(col_name)
            
        except Exception as e:
            print(f"  ❌ Falha ao criar {col_name}: {e}")
            failed_columns.append((col_name, str(e)))
    
    print(f"✅ Colunas criadas: {len(created_columns)}")
    print(f"❌ Colunas falharam: {len(failed_columns)}")
    
    return created_columns, failed_columns

def auto_fix_schema(supabase_client, df_sample):
    """Corrige automaticamente o schema da tabela."""
    print("\n🔧 AUTO-FIX: Corrigindo schema automaticamente...")
    
    # 1. Obtém colunas existentes
    existing_columns = get_existing_supabase_columns(supabase_client)
    csv_columns = list(df_sample.columns)
    
    print(f"📊 CSV: {len(csv_columns)} colunas")
    print(f"📊 Supabase: {len(existing_columns)} colunas")
    
    # 2. Identifica colunas faltantes
    existing_set = set(existing_columns)
    csv_set = set(csv_columns)
    
    missing_columns = csv_set - existing_set
    
    if missing_columns:
        print(f"🆕 Colunas faltantes: {len(missing_columns)}")
        for i, col in enumerate(list(missing_columns)[:10], 1):
            print(f"   {i:2d}. {col}")
        if len(missing_columns) > 10:
            print(f"   ... e mais {len(missing_columns) - 10} colunas")
        
        # 3. Cria colunas faltantes
        sample_record = df_sample.iloc[0].to_dict()
        created, failed = create_missing_columns_sql(supabase_client, missing_columns, sample_record)
        
        if failed:
            print("⚠️ Algumas colunas não puderam ser criadas - continuando com as disponíveis")
        
        # 4. Atualiza lista de colunas existentes
        updated_columns = get_existing_supabase_columns(supabase_client)
        return updated_columns
    
    else:
        print("✅ Schema já está compatível")
        return existing_columns

def create_supabase_function_if_needed(supabase_client):
    """Cria função SQL personalizada se não existir."""
    try:
        # SQL para criar função de execução se não existir
        function_sql = """
        CREATE OR REPLACE FUNCTION public.exec_sql(query text)
        RETURNS json
        LANGUAGE plpgsql
        SECURITY DEFINER
        AS $$
        DECLARE
            result json;
        BEGIN
            EXECUTE query;
            GET DIAGNOSTICS result = ROW_COUNT;
            RETURN json_build_object('success', true, 'rows_affected', result);
        EXCEPTION
            WHEN OTHERS THEN
                RETURN json_build_object('success', false, 'error', SQLERRM);
        END;
        $$;
        """
        
        print("🔧 Configurando função SQL auxiliar...")
        supabase_client.rpc('exec_sql', {'query': function_sql}).execute()
        print("✅ Função SQL configurada")
        
    except Exception as e:
        print(f"⚠️ Não foi possível criar função SQL: {e}")

# --- 3. Download robusto (reutilizado) ---
def download_with_multiple_methods(url):
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    
    methods = [
        ("requests_no_ssl", lambda: download_with_requests_no_ssl(url)),
        ("urllib_no_ssl", lambda: download_with_urllib_no_ssl(url)),
        ("wget", lambda: download_with_wget(url)),
        ("curl", lambda: download_with_curl(url)),
    ]
    
    for method_name, method_func in methods:
        print(f"🔄 Tentando método: {method_name}")
        try:
            content = method_func()
            if content and len(content) > 1000:
                print(f"✅ Sucesso com {method_name}! Tamanho: {len(content):,} bytes")
                return content
        except Exception as e:
            print(f"❌ {method_name} falhou: {str(e)[:50]}...")
    
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

# --- 4. Processamento de dados ---
def make_json_serializable(obj):
    if pd.isna(obj):
        return None
    elif isinstance(obj, (pd.Timestamp, datetime)):
        return obj.strftime('%Y-%m-%d %H:%M:%S') if pd.notna(obj) else None
    elif isinstance(obj, (np.integer, np.int64)):
        return int(obj)
    elif isinstance(obj, (np.floating, np.float64)):
        return float(obj) if not np.isnan(obj) else None
    elif isinstance(obj, np.bool_):
        return bool(obj)
    elif isinstance(obj, bytes):
        return obj.decode('utf-8', errors='ignore')
    else:
        return str(obj) if obj is not None else None

def clean_dataframe_for_json(df):
    print("🧹 Preparando dados para JSON...")
    df_clean = df.copy()
    
    for col in df_clean.columns:
        df_clean[col] = df_clean[col].apply(make_json_serializable)
    
    print(f"✅ Dados limpos: {len(df_clean)} registros, {len(df_clean.columns)} colunas")
    return df_clean

def read_csv_robust(zip_file, csv_file):
    encodings = ['utf-8', 'latin1', 'cp1252']
    separators = [';', ',', '\t']
    
    for encoding in encodings:
        for sep in separators:
            try:
                with zip_file.open(csv_file) as csv_data:
                    df = pd.read_csv(csv_data, encoding=encoding, sep=sep, low_memory=False)
                    if len(df.columns) > 5 and len(df) > 0:
                        return df
            except:
                continue
    return None

def download_and_process_data():
    print("📥 Baixando dados do IBAMA...")
    
    content = download_with_multiple_methods(IBAMA_ZIP_URL)
    
    print("📦 Processando arquivo ZIP...")
    with zipfile.ZipFile(BytesIO(content)) as zip_file:
        csv_files = [f for f in zip_file.namelist() if f.endswith('.csv')]
        
        # Busca arquivos 2024-2025
        target_files = [f for f in csv_files if any(year in f for year in ['2024', '2025'])]
        
        if target_files:
            print(f"🎯 Arquivos encontrados: {target_files}")
        else:
            print("⚠️ Usando arquivos mais recentes...")
            target_files = sorted(csv_files, reverse=True)[:2]
        
        # Processa arquivos
        all_dataframes = []
        for csv_file in target_files:
            print(f"⚙️ Processando: {csv_file}")
            df_temp = read_csv_robust(zip_file, csv_file)
            
            if df_temp is not None and len(df_temp) > 0:
                print(f"    ✅ {len(df_temp):,} registros")
                all_dataframes.append(df_temp)
        
        if not all_dataframes:
            raise ValueError("Nenhum arquivo válido processado")
        
        # Combina DataFrames
        df = pd.concat(all_dataframes, ignore_index=True, sort=False)
        print(f"📊 Dados combinados: {len(df):,} registros")
        
    return df

# --- 5. Upload AUTO-FIX ---
def upload_with_auto_fix(df):
    """Upload com correção automática de schema."""
    print("🔗 Conectando ao Supabase...")
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    table_name = "ibama_infracao"
    
    # 1. Configurar função SQL se necessário
    create_supabase_function_if_needed(supabase)
    
    # 2. Auto-corrigir schema
    compatible_columns = auto_fix_schema(supabase, df.head(1))
    
    # 3. Filtrar DataFrame para colunas compatíveis
    available_columns = [col for col in df.columns if col in compatible_columns]
    df_filtered = df[available_columns].copy()
    
    print(f"✅ DataFrame filtrado: {len(df_filtered)} registros, {len(available_columns)} colunas")
    
    if len(available_columns) < len(df.columns):
        skipped = len(df.columns) - len(available_columns)
        print(f"⚠️ {skipped} colunas foram puladas (não compatíveis)")
    
    # 4. Limpeza de dados
    df_clean = clean_dataframe_for_json(df_filtered)
    
    # 5. Limpar tabela
    print(f"🧹 Limpando tabela...")
    try:
        supabase.table(table_name).delete().neq('id', -1).execute()
        print("✅ Tabela limpa")
    except Exception as e:
        print(f"⚠️ Aviso ao limpar: {e}")
    
    # 6. Upload em lotes pequenos
    chunk_size = 200
    total_chunks = (len(df_clean) // chunk_size) + 1
    print(f"🚀 Upload: {len(df_clean):,} registros em {total_chunks} lotes de {chunk_size}")
    
    successful_uploads = 0
    failed_uploads = 0
    
    for i in range(0, len(df_clean), chunk_size):
        chunk_index = i // chunk_size + 1
        print(f"  📤 Lote {chunk_index}/{total_chunks}...", end=" ")
        
        chunk = df_clean[i:i + chunk_size]
        
        try:
            data_to_insert = chunk.to_dict(orient='records')
            
            # Upload com retry
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    response = supabase.table(table_name).insert(data_to_insert).execute()
                    
                    if hasattr(response, 'error') and response.error:
                        raise Exception(f"API Error: {response.error}")
                    
                    successful_uploads += len(data_to_insert)
                    print(f"✅ {len(data_to_insert)} registros")
                    break
                    
                except Exception as retry_error:
                    if attempt == max_retries - 1:
                        raise retry_error
                    time.sleep(1)
            
            time.sleep(0.3)  # Pausa entre lotes
            
        except Exception as e:
            failed_uploads += len(chunk)
            error_msg = str(e)[:100]
            print(f"❌ {error_msg}...")
            
            # Se falhar nos primeiros lotes, para e reporta
            if chunk_index <= 3:
                print(f"🔍 Erro crítico no lote {chunk_index}:")
                print(f"    Colunas: {len(chunk.columns)}")
                print(f"    Erro: {error_msg}")
                if chunk_index == 1:
                    print("⚠️ Parando no primeiro erro para análise")
                    break
    
    # Relatório final
    print(f"\n{'='*60}")
    print(f"📊 RELATÓRIO FINAL:")
    print(f"  📥 Total processado: {len(df_clean):,}")
    print(f"  ✅ Sucesso: {successful_uploads:,}")
    print(f"  ❌ Falhas: {failed_uploads:,}")
    print(f"  📈 Taxa de sucesso: {(successful_uploads/len(df_clean))*100:.1f}%")
    print(f"  📋 Colunas utilizadas: {len(available_columns)}")
    print(f"{'='*60}")
    
    return successful_uploads, failed_uploads, len(available_columns)

# --- 6. Execução principal ---
try:
    # Download e processamento
    df = download_and_process_data()
    
    if df.empty:
        print("❌ Nenhum dado processado.")
        sys.exit(1)
    
    print(f"✅ Dados processados: {len(df):,} registros, {len(df.columns)} colunas")
    
    # Upload com auto-fix
    successful, failed, columns_used = upload_with_auto_fix(df)
    
    # Avaliação final
    success_rate = (successful / len(df)) * 100 if len(df) > 0 else 0
    
    if success_rate >= 95:
        print("🎉 UPLOAD REALIZADO COM SUCESSO!")
        sys.exit(0)
    elif success_rate >= 80:
        print("⚠️ Upload parcialmente bem-sucedido")
        sys.exit(0)
    else:
        print("❌ Upload falhou - taxa de sucesso muito baixa")
        sys.exit(1)

except Exception as e:
    print(f"💥 Erro crítico: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
