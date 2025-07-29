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

print("🌳 Iniciando processo de upload para o Supabase (Versão Final)...")

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

# --- 3. Processamento inteligente dos dados ---
def download_and_process_data():
    """Download e processa os dados do IBAMA de forma inteligente."""
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
            
            print(f"📄 Total de arquivos CSV encontrados: {len(csv_files)}")
            print(f"    Arquivos: {', '.join(csv_files[:5])}{'...' if len(csv_files) > 5 else ''}")
            
            # Estratégia inteligente: priorizar arquivos 2024-2025, mas ter fallback
            target_years = ['2024', '2025']
            priority_files = [f for f in csv_files if any(year in f for year in target_years)]
            
            if priority_files:
                print(f"🎯 Arquivos prioritários encontrados (2024-2025): {priority_files}")
                files_to_process = priority_files
            else:
                # Se não encontrar arquivos específicos, pega os mais recentes
                print("⚠️ Arquivos 2024-2025 não encontrados. Processando arquivos mais recentes...")
                # Ordena os arquivos pelo nome (que geralmente tem o ano) em ordem decrescente
                sorted_files = sorted(csv_files, reverse=True)
                files_to_process = sorted_files[:5]  # Pega os 5 mais recentes
                print(f"📅 Processando arquivos mais recentes: {files_to_process}")
            
            # Processa os arquivos selecionados
            all_dataframes = []
            total_records = 0
            
            for csv_file in files_to_process:
                print(f"⚙️ Processando: {csv_file}")
                
                try:
                    df_temp = read_csv_robust(zip_file, csv_file)
                    
                    if df_temp is not None and len(df_temp) > 0:
                        print(f"    ✅ Sucesso: {len(df_temp):,} registros, {len(df_temp.columns)} colunas")
                        all_dataframes.append(df_temp)
                        total_records += len(df_temp)
                    else:
                        print(f"    ⚠️ Arquivo vazio ou inválido: {csv_file}")
                        
                except Exception as e:
                    print(f"    ❌ Erro ao processar {csv_file}: {str(e)[:100]}...")
                    continue
            
            if not all_dataframes:
                raise ValueError("Não foi possível processar nenhum arquivo CSV com dados válidos")
            
            # Combina todos os DataFrames
            print(f"🔄 Combinando {len(all_dataframes)} arquivos válidos...")
            df = pd.concat(all_dataframes, ignore_index=True, sort=False)
            print(f"📊 Dados combinados: {len(df):,} registros, {len(df.columns)} colunas")
            
        # Filtro adicional por data se necessário
        df = apply_date_filter(df)
        
        # Limpeza final
        df = clean_dataframe(df)
        
        if len(df) == 0:
            raise ValueError("Nenhum registro restou após filtros e limpeza")
        
        print(f"🎯 Dados finais prontos: {len(df):,} registros, {len(df.columns)} colunas")
        return df
        
    except Exception as e:
        print(f"❌ Erro ao baixar/processar dados: {e}")
        raise

def read_csv_robust(zip_file, csv_file):
    """Lê um arquivo CSV de forma robusta com múltiplas tentativas."""
    encodings = ['utf-8', 'latin1', 'cp1252', 'iso-8859-1']
    separators = [';', ',', '\t']
    
    for encoding in encodings:
        for sep in separators:
            try:
                with zip_file.open(csv_file) as csv_data:
                    df = pd.read_csv(csv_data, encoding=encoding, sep=sep, low_memory=False)
                    
                    # Validações básicas
                    if len(df.columns) > 5 and len(df) > 0:
                        return df
                        
            except Exception:
                continue
    
    return None

def apply_date_filter(df):
    """Aplica filtro de data se possível."""
    if 'DAT_HORA_AUTO_INFRACAO' not in df.columns:
        print("📅 Coluna de data não encontrada, mantendo todos os registros")
        return df
    
    try:
        original_size = len(df)
        
        # Converte a coluna de data
        df['DAT_HORA_AUTO_INFRACAO'] = pd.to_datetime(df['DAT_HORA_AUTO_INFRACAO'], errors='coerce')
        
        # Verifica se conseguimos converter alguma data
        valid_dates = df['DAT_HORA_AUTO_INFRACAO'].notna().sum()
        if valid_dates == 0:
            print("⚠️ Nenhuma data válida encontrada, mantendo todos os registros")
            return df
        
        # Filtra pelos anos 2024 e 2025
        df_filtered = df[df['DAT_HORA_AUTO_INFRACAO'].dt.year.isin([2024, 2025])]
        
        if len(df_filtered) > 0:
            print(f"📅 Filtro de data aplicado (2024-2025): {original_size:,} → {len(df_filtered):,} registros")
            return df_filtered
        else:
            print(f"⚠️ Nenhum registro de 2024-2025 encontrado, mantendo todos os {original_size:,} registros")
            return df
            
    except Exception as e:
        print(f"⚠️ Erro ao aplicar filtro de data: {e}")
        return df

def clean_dataframe(df):
    """Limpa o DataFrame para upload."""
    # Remove valores NaN
    df = df.fillna('')
    
    # Remove colunas completamente vazias
    df = df.dropna(axis=1, how='all')
    
    # Remove linhas completamente vazias
    df = df.dropna(axis=0, how='all')
    
    return df

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
    chunk_size = 1000  # Aumentado para ser mais eficiente
    total_chunks = (len(df) // chunk_size) + 1
    print(f"🚀 Iniciando upload de {len(df):,} registros em {total_chunks} lotes de {chunk_size}...")
    
    successful_uploads = 0
    failed_uploads = 0
    
    for i in range(0, len(df), chunk_size):
        chunk_index = i // chunk_size + 1
        print(f"  📤 Lote {chunk_index}/{total_chunks}...", end=" ")
        
        chunk = df[i:i + chunk_size]
        data_to_insert = chunk.to_dict(orient='records')
        
        try:
            response = supabase.table(table_name).insert(data_to_insert).execute()
            
            if hasattr(response, 'error') and response.error:
                raise Exception(f"Erro da API: {response.error.message}")
            
            successful_uploads += len(data_to_insert)
            print(f"✅ {len(data_to_insert)} registros")
            
            time.sleep(0.2)  # Pausa otimizada
            
        except Exception as e:
            failed_uploads += len(data_to_insert)
            print(f"❌ Falha: {str(e)[:50]}...")
            continue
    
    # --- 8. Relatório final ---
    print(f"\n{'='*60}")
    print(f"📊 RELATÓRIO FINAL:")
    print(f"  📥 Total processado: {len(df):,} registros")
    print(f"  ✅ Uploads bem-sucedidos: {successful_uploads:,}")
    print(f"  ❌ Uploads falharam: {failed_uploads:,}")
    print(f"  📈 Taxa de sucesso: {(successful_uploads/len(df))*100:.1f}%")
    print(f"{'='*60}")
    
    if failed_uploads == 0:
        print("🎉 Upload para o Supabase concluído com sucesso!")
        sys.exit(0)
    elif successful_uploads > failed_uploads:
        print("⚠️  Upload concluído com êxito parcial.")
        sys.exit(0)
    else:
        print("❌ Upload falhou - muitos erros.")
        sys.exit(1)

except Exception as e:
    print(f"💥 Erro crítico: {e}")
    sys.exit(1)
