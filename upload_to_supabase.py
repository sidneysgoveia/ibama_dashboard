import pandas as pd
from supabase import create_client, Client
import time
import os

# Importa os componentes necessários do seu projeto
from src.utils.data_loader import DataLoader
from src.utils.database import Database # Usado para o processamento local inicial

print("Iniciando processo de upload para o Supabase...")

# 1. Baixar e processar os dados localmente primeiro
# ----------------------------------------------------
# Lê a URL do IBAMA a partir das variáveis de ambiente, com um valor padrão
ibama_zip_url = os.getenv(
    "IBAMA_ZIP_URL", 
    'https://dadosabertos.ibama.gov.br/dados/SIFISC/auto_infracao/auto_infracao/auto_infracao_csv.zip'
)

# Usa um DuckDB em memória para processar os dados antes do upload
temp_db = Database() 
data_loader = DataLoader(database=temp_db)
# Passa a URL para o DataLoader
data_loader.zip_url = ibama_zip_url

print("Baixando e processando dados do IBAMA (anos 2024-2025)...")
success = data_loader.download_and_process()

if not success:
    raise RuntimeError("Falha ao baixar e processar os dados do IBAMA.")

df = temp_db.execute_query("SELECT * FROM ibama_infracao")
# Garante que não há valores NaN, que podem causar problemas no JSON
df = df.fillna('')
print(f"Dados processados. Total de {len(df)} registros prontos para upload.")
print("Colunas encontradas no DataFrame:", df.columns.tolist())


# 2. Configurar o cliente do Supabase
# ------------------------------------
supabase_url = os.getenv("SUPABASE_URL")
supabase_key = os.getenv("SUPABASE_KEY")

if not supabase_url or not supabase_key:
    raise ValueError("As variáveis de ambiente SUPABASE_URL e SUPABASE_SERVICE_KEY precisam ser definidas.")

supabase: Client = create_client(supabase_url, supabase_key)
table_name = "ibama_infracao"
print(f"Conectado ao Supabase. Alvo: tabela '{table_name}'.")


# 3. Limpar a tabela existente (Opcional, mas recomendado para atualizações)
# -------------------------------------------------------------------------
print(f"Limpando a tabela '{table_name}' no Supabase antes do upload...")
try:
    # Deleta todas as linhas da tabela
    delete_response = supabase.table(table_name).delete().neq('id', -1).execute()
    
    # Verifica se a API retornou um erro explícito
    if hasattr(delete_response, 'error') and delete_response.error:
        raise Exception(f"Erro ao limpar a tabela: {delete_response.error.message}")
    
    # A API pode não retornar dados em um delete bem-sucedido, então o log reflete isso
    print("  Tabela limpa ou já estava vazia.")

except Exception as e:
    print(f"❌ Erro ao tentar limpar a tabela: {e}")
    # Para o script para que o erro possa ser investigado
    raise


# 4. Fazer o upload dos dados em lotes (chunks)
# ----------------------------------------------
chunk_size = 500
total_chunks = (len(df) // chunk_size) + 1
print(f"Iniciando upload de {len(df)} registros em {total_chunks} lotes de {chunk_size}...")

for i in range(0, len(df), chunk_size):
    chunk_index = i // chunk_size + 1
    print(f"  Preparando lote {chunk_index}/{total_chunks}...")
    
    chunk = df[i:i + chunk_size]
    data_to_insert = chunk.to_dict(orient='records')
    
    try:
        # Executa a inserção
        response = supabase.table(table_name).insert(data_to_insert).execute()
        
        # Verifica explicitamente se a API retornou um erro
        if hasattr(response, 'error') and response.error:
            raise Exception(f"Erro da API do Supabase: {response.error.message}")
        
        if response.data:
            print(f"  Lote {chunk_index} enviado com sucesso. {len(response.data)} registros inseridos.")
        else:
            print(f"  Lote {chunk_index} enviado. A API não retornou dados, mas não houve erro.")

        time.sleep(1) # Pausa para não sobrecarregar a API
    except Exception as e:
        print(f"❌ Falha crítica ao enviar o lote {chunk_index}: {e}")
        # Para o script se um lote falhar, para que possamos investigar
        raise

print("\n✅ Upload para o Supabase concluído com sucesso!")
