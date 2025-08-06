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

print("🌳 IBAMA Upload CORRIGIDO - Mapeamento de Colunas v2.0...")

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

# --- 2. MAPEAMENTO DE COLUNAS CORRIGIDO ---
def get_column_mapping():
    """
    Mapeamento entre colunas do CSV e colunas do Supabase.
    Baseado no arquivo 'Supabase Snippet Column Names Verification.csv'
    """
    return {
        # Colunas principais
        'SEQ_AUTO_INFRACAO': 'SEQ_AUTO_INFRACAO',
        'DES_STATUS_FORMULARIO': 'DES_STATUS_FORMULARIO', 
        'DS_SIT_AUTO_AIE': 'DS_SIT_AUTO_AIE',
        'SIT_CANCELADO': 'SIT_CANCELADO',
        'NUM_AUTO_INFRACAO': 'NUM_AUTO_INFRACAO',
        'SER_AUTO_INFRACAO': 'SER_AUTO_INFRACAO',
        'CD_ORIGINAL_AUTO_INFRACAO': 'CD_ORIGINAL_AUTO_INFRACAO',
        'TIPO_AUTO': 'TIPO_AUTO',
        'TIPO_MULTA': 'TIPO_MULTA',
        'VAL_AUTO_INFRACAO': 'VAL_AUTO_INFRACAO',
        'FUNDAMENTACAO_MULTA': 'FUNDAMENTACAO_MULTA',
        'PATRIMONIO_APURACAO': 'PATRIMONIO_APURACAO',
        'GRAVIDADE_INFRACAO': 'GRAVIDADE_INFRACAO',
        'CD_NIVEL_GRAVIDADE': 'CD_NIVEL_GRAVIDADE',
        'MOTIVACAO_CONDUTA': 'MOTIVACAO_CONDUTA',
        'EFEITO_MEIO_AMBIENTE': 'EFEITO_MEIO_AMBIENTE',
        'EFEITO_SAUDE_PUBLICA': 'EFEITO_SAUDE_PUBLICA',
        'PASSIVEL_RECUPERACAO': 'PASSIVEL_RECUPERACAO',
        'UNID_ARRECADACAO': 'UNID_ARRECADACAO',
        'DES_AUTO_INFRACAO': 'DES_AUTO_INFRACAO',
        'DAT_HORA_AUTO_INFRACAO': 'DAT_HORA_AUTO_INFRACAO',
        'FORMA_ENTREGA': 'FORMA_ENTREGA',
        'DAT_CIENCIA_AUTUACAO': 'DAT_CIENCIA_AUTUACAO',
        'DT_FATO_INFRACIONAL': 'DT_FATO_INFRACIONAL',
        'DT_INICIO_ATO_INEQUIVOCO': 'DT_INICIO_ATO_INEQUIVOCO',
        'DT_FIM_ATO_INEQUIVOCO': 'DT_FIM_ATO_INEQUIVOCO',
        'COD_MUNICIPIO': 'COD_MUNICIPIO',
        'MUNICIPIO': 'MUNICIPIO',
        'UF': 'UF',
        'NUM_PROCESSO': 'NUM_PROCESSO',
        'NU_PROCESSO_FORMATADO': 'NU_PROCESSO_FORMATADO',
        'COD_INFRACAO': 'COD_INFRACAO',
        'DES_INFRACAO': 'DES_INFRACAO',
        'TIPO_INFRACAO': 'TIPO_INFRACAO',
        
        # CORREÇÃO: Coluna problemática mapeada corretamente
        'CD_RECEITA_AUTO_INFRACAO': 'CD_RECEITA_AUTO_INFRACAO',  # Verificar se truncada
        'DES_RECEITA': 'DES_RECEITA',
        'TP_PESSOA_INFRATOR': 'TP_PESSOA_INFRATOR',
        'NUM_PESSOA_INFRATOR': 'NUM_PESSOA_INFRATOR',
        'NOME_INFRATOR': 'NOME_INFRATOR',
        'CPF_CNPJ_INFRATOR': 'CPF_CNPJ_INFRATOR',
        'QT_AREA': 'QT_AREA',
        'INFRACAO_AREA': 'INFRACAO_AREA',
        'DES_OUTROS_TIPO_AREA': 'DES_OUTROS_TIPO_AREA',
        'CLASSIFICACAO_AREA': 'CLASSIFICACAO_AREA',
        'DS_FATOR_AJUSTE': 'DS_FATOR_AJUSTE',
        'NUM_LONGITUDE_AUTO': 'NUM_LONGITUDE_AUTO',
        'NUM_LATITUDE_AUTO': 'NUM_LATITUDE_AUTO',
        'DS_WKT': 'DS_WKT',
        'DES_LOCAL_INFRACAO': 'DES_LOCAL_INFRACAO',
        'DS_REFERENCIA_ACAO_FISCALIZATORIA': 'DS_REFERENCIA_ACAO_FISCALIZATORIA',
        'UNIDADE_CONSERVACAO': 'UNIDADE_CONSERVACAO',
        'ID_SICAFI_BIOMAS_ATINGIDOS_INFRACAO': 'ID_SICAFI_BIOMAS_ATINGIDOS_INFRACAO',
        'DS_BIOMAS_ATINGIDOS': 'DS_BIOMAS_ATINGIDOS',
        'SEQ_NOTIFICACAO': 'SEQ_NOTIFICACAO',
        'SEQ_ACAO_FISCALIZATORIA': 'SEQ_ACAO_FISCALIZATORIA',
        'CD_ACAO_FISCALIZATORIA': 'CD_ACAO_FISCALIZATORIA',
        'UNID_CONTROLE': 'UNID_CONTROLE',
        'TIPO_ACAO': 'TIPO_ACAO',
        'OPERACAO': 'OPERACAO',
        'SEQ_ORDEM_FISCALIZACAO': 'SEQ_ORDEM_FISCALIZACAO',
        'ORDEM_FISCALIZACAO': 'ORDEM_FISCALIZACAO',
        'UNID_ORDENADORA': 'UNID_ORDENADORA',
        'SEQ_SOLICITACAO_RECURSO': 'SEQ_SOLICITACAO_RECURSO',
        'SOLICITACAO_RECURSO': 'SOLICITACAO_RECURSO',
        'OPERACAO_SOL_RECURSO': 'OPERACAO_SOL_RECURSO',
        'DT_LANCAMENTO': 'DT_LANCAMENTO',
        'TP_ULT_ALTERACAO': 'TP_ULT_ALTERACAO',
        'DT_ULT_ALTERACAO': 'DT_ULT_ALTERACAO',
        'JUSTIFICATIVA_ALTERACAO': 'JUSTIFICATIVA_ALTERACAO',
        'WKT_GE_AREA_AUTUADA': 'WKT_GE_AREA_AUTUADA',
        'DT_ULT_ALTER_GEOM': 'DT_ULT_ALTER_GEOM',
        'TP_ORIGEM_GE_AREA_AUTUADA': 'TP_ORIGEM_GE_AREA_AUTUADA',
        'DS_ERRO_GE_AREA_AUTUADA': 'DS_ERRO_GE_AREA_AUTUADA',
        'ST_AUTO_MIGRADO_AIE': 'ST_AUTO_MIGRADO_AIE',
        'DS_ENQUADRAMENTO_ADMINISTRATIVO': 'DS_ENQUADRAMENTO_ADMINISTRATIVO',
        'DS_ENQUADRAMENTO_NAO_ADMINISTRATIVO': 'DS_ENQUADRAMENTO_NAO_ADMINISTRATIVO',
        'DS_ENQUADRAMENTO_COMPLEMENTAR': 'DS_ENQUADRAMENTO_COMPLEMENTAR',
        'CD_TERMOS_APREENSAO': 'CD_TERMOS_APREENSAO',
        'CD_TERMOS_EMBARGOS': 'CD_TERMOS_EMBARGOS',
        'TP_ORIGEM_REGISTRO_AUTO': 'TP_ORIGEM_REGISTRO_AUTO',
        'ULTIMA_ATUALIZACAO_RELATORIO': 'ULTIMA_ATUALIZACAO_RELATORIO'
    }

def verify_supabase_schema(supabase_client):
    """Verifica o schema da tabela no Supabase."""
    try:
        # Tenta obter um registro para ver as colunas
        result = supabase_client.table('ibama_infracao').select('*').limit(1).execute()
        
        if result.data:
            supabase_columns = set(result.data[0].keys())
            print(f"📊 Colunas encontradas no Supabase: {len(supabase_columns)}")
            print(f"   Primeiras 10: {list(supabase_columns)[:10]}")
            return supabase_columns
        else:
            print("⚠️ Tabela vazia - não foi possível verificar schema")
            return set()
    except Exception as e:
        print(f"❌ Erro ao verificar schema: {e}")
        return set()

def map_dataframe_columns(df, supabase_columns):
    """Mapeia e filtra colunas do DataFrame para o Supabase."""
    column_mapping = get_column_mapping()
    
    print(f"🔄 Iniciando mapeamento de colunas...")
    print(f"   DataFrame original: {len(df.columns)} colunas")
    print(f"   Supabase disponíveis: {len(supabase_columns)} colunas")
    
    # Cria DataFrame mapeado
    mapped_df = pd.DataFrame()
    successful_mappings = 0
    failed_mappings = []
    
    for csv_col, supabase_col in column_mapping.items():
        if csv_col in df.columns:
            if supabase_col in supabase_columns or len(supabase_columns) == 0:
                # Mapeia a coluna
                mapped_df[supabase_col] = df[csv_col]
                successful_mappings += 1
            else:
                failed_mappings.append(f"{csv_col} -> {supabase_col} (não existe no Supabase)")
        else:
            failed_mappings.append(f"{csv_col} (não existe no CSV)")
    
    print(f"✅ Mapeamento concluído:")
    print(f"   Sucessos: {successful_mappings} colunas")
    print(f"   Falhas: {len(failed_mappings)} colunas")
    
    if failed_mappings:
        print(f"⚠️ Colunas não mapeadas:")
        for failure in failed_mappings[:10]:  # Mostra apenas as primeiras 10
            print(f"     • {failure}")
        if len(failed_mappings) > 10:
            print(f"     ... e mais {len(failed_mappings) - 10} colunas")
    
    return mapped_df

# --- 3. Download robusto (mesmo código anterior) ---
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

# --- 4. Funções de processamento CORRIGIDAS ---
def make_json_serializable(obj):
    """Converte objetos para tipos serializáveis em JSON."""
    if pd.isna(obj):
        return None  # CORREÇÃO: usa None ao invés de string vazia
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

# --- 5. Processamento principal CORRIGIDO ---
def download_and_process_data():
    """Download e processa os dados do IBAMA com mapeamento correto."""
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
        
        return df
        
    except Exception as e:
        print(f"❌ Erro no processamento: {e}")
        raise

# --- 6. Upload CORRIGIDO com mapeamento ---
def upload_to_supabase_corrected(df):
    """Upload corrigido com mapeamento de colunas."""
    print("🔗 Conectando ao Supabase...")
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    table_name = "ibama_infracao"
    
    # PASSO 1: Verificar schema do Supabase
    print("🔍 Verificando schema do Supabase...")
    supabase_columns = verify_supabase_schema(supabase)
    
    # PASSO 2: Mapear colunas do DataFrame
    print("🔄 Mapeando colunas...")
    df_mapped = map_dataframe_columns(df, supabase_columns)
    
    if df_mapped.empty:
        raise Exception("❌ Nenhuma coluna foi mapeada com sucesso!")
    
    print(f"✅ DataFrame mapeado: {len(df_mapped)} registros, {len(df_mapped.columns)} colunas")
    
    # PASSO 3: Limpeza para JSON
    df_clean = clean_dataframe_for_json(df_mapped)
    
    # PASSO 4: Limpar tabela
    print(f"🧹 Limpando tabela '{table_name}'...")
    try:
        supabase.table(table_name).delete().neq('id', -1).execute()
        print("  ✅ Tabela limpa")
    except Exception as e:
        print(f"❌ Erro ao limpar tabela: {e}")
        raise
    
    # PASSO 5: Upload em lotes menores
    chunk_size = 250  # Reduzido ainda mais para maior confiabilidade
    total_chunks = (len(df_clean) // chunk_size) + 1
    print(f"🚀 Upload: {len(df_clean):,} registros em {total_chunks} lotes de {chunk_size}")
    
    successful_uploads = 0
    failed_uploads = 0
    
    for i in range(0, len(df_clean), chunk_size):
        chunk_index = i // chunk_size + 1
        print(f"  📤 Lote {chunk_index}/{total_chunks}...", end=" ")
        
        chunk = df_clean[i:i + chunk_size]
        
        try:
            # Converte para dict
            data_to_insert = chunk.to_dict(orient='records')
            
            # Upload com timeout maior
            response = supabase.table(table_name).insert(data_to_insert).execute()
            
            if hasattr(response, 'error') and response.error:
                raise Exception(f"API Error: {response.error.message}")
            
            successful_uploads += len(data_to_insert)
            print(f"✅ {len(data_to_insert)} registros")
            
            time.sleep(0.5)  # Pausa maior de segurança
            
        except Exception as e:
            failed_uploads += len(chunk)
            error_msg = str(e)[:100]
            print(f"❌ {error_msg}...")
            
            # Mostra detalhes do erro nos primeiros lotes
            if chunk_index <= 2:
                print(f"🔍 Debug do erro no lote {chunk_index}:")
                print(f"    Colunas do lote: {list(chunk.columns)}")
                print(f"    Exemplo de registro: {chunk.iloc[0].to_dict()}")
                
                # Para nos primeiros erros para permitir correção
                if chunk_index == 1:
                    raise Exception(f"Parando no primeiro erro para debug: {error_msg}")
            continue
    
    # Relatório final
    print(f"\n{'='*60}")
    print(f"📊 RELATÓRIO FINAL:")
    print(f"  📥 Total: {len(df_clean):,} registros")
    print(f"  ✅ Sucesso: {successful_uploads:,}")
    print(f"  ❌ Falha: {failed_uploads:,}")
    print(f"  📈 Taxa: {(successful_uploads/len(df_clean))*100:.1f}%")
    print(f"{'='*60}")
    
    return successful_uploads, failed_uploads

# --- 7. Execução principal ---
try:
    # Download e processamento
    df = download_and_process_data()
    
    if df.empty:
        print("❌ Nenhum dado processado.")
        sys.exit(1)
    
    print(f"✅ Dados processados: {len(df):,} registros, {len(df.columns)} colunas")
    
    # Upload corrigido
    successful, failed = upload_to_supabase_corrected(df)
    
    if successful > 0:
        print("🎉 Upload concluído com sucesso!")
        sys.exit(0)
    else:
        print("❌ Upload falhou completamente.")
        sys.exit(1)

except Exception as e:
    print(f"💥 Erro crítico: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
