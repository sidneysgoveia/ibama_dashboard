# upload_to_supabase.py

import pandas as pd
from supabase import create_client, Client
from decouple import config
import time

# Carrega os dados do CSV local (ou baixa primeiro)
# Supondo que você já tenha o CSV processado.
# Para este exemplo, vamos simular o download e processamento.
from src.utils.data_loader import DataLoader
from src.utils.database import Database

print("Iniciando processo de upload para o Supabase...")

# 1. Baixar e processar os dados
# Usamos o DataLoader para obter o DataFrame mais recente
temp_db = Database() # DuckDB temporário
data_loader = DataLoader(database=temp_db)
print("Baixando dados do IBAMA...")
data_loader.download_and_process()
df = temp_db.execute_query("SELECT * FROM ibama_infracao")
print(f"Dados processados. Total de {len(df)} registros.")

# 2. Configurar o cliente do Supabase
url: str = config("SUPABASE_URL")
key: str = config("SUPABASE_KEY")
supabase: Client = create_client(url, key)
table_name = "ibama_infracao"

# 3. Limpar a tabela existente (opcional, mas recomendado para atualizações)
print(f"Limpando a tabela '{table_name}' no Supabase antes do upload...")
# O Supabase não tem um 'truncate' fácil via API, então deletamos em lotes
# Uma forma mais simples é deletar tudo com um filtro que corresponda a todos
supabase.table(table_name).delete().neq('id', -1).execute() # Deleta todas as linhas

# 4. Fazer o upload dos dados em lotes (chunks)
# A API do Supabase tem um limite de tamanho por requisição. Lotes de 1000 são seguros.
chunk_size = 1000
total_chunks = (len(df) // chunk_size) + 1
print(f"Iniciando upload de {len(df)} registros em {total_chunks} lotes de {chunk_size}...")

for i in range(0, len(df), chunk_size):
    chunk = df[i:i + chunk_size]
    # Converte o chunk do DataFrame para uma lista de dicionários
    data_to_insert = chunk.to_dict(orient='records')
    
    try:
        response = supabase.table(table_name).insert(data_to_insert).execute()
        print(f"  Lote {i//chunk_size + 1}/{total_chunks} enviado com sucesso.")
        # Pequena pausa para não sobrecarregar a API
        time.sleep(1) 
    except Exception as e:
        print(f"❌ Erro ao enviar o lote {i//chunk_size + 1}: {e}")
        # Opcional: adicionar lógica para tentar novamente ou parar

print("\n✅ Upload para o Supabase concluído com sucesso!")
