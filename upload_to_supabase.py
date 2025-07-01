import pandas as pd
from supabase import create_client, Client
import time
import os # Importa o módulo os

# Importa os componentes necessários
from src.utils.data_loader import DataLoader
from src.utils.database import Database

print("Iniciando processo de upload para o Supabase...")

# 1. Baixar e processar os dados
# O DataLoader agora precisa da URL do IBAMA, que passaremos via variável de ambiente
# Para manter a compatibilidade, o DataLoader ainda usa 'decouple', então vamos garantir que ele funcione
# A melhor abordagem é o script de upload ler as variáveis e passá-las para os componentes.

# Lê as variáveis de ambiente (funciona localmente com .env e no GitHub Actions)
supabase_url = os.getenv("SUPABASE_URL")
supabase_key = os.getenv("SUPABASE_KEY")
ibama_zip_url = os.getenv("IBAMA_ZIP_URL", 'https://dadosabertos.ibama.gov.br/dados/SIFISC/auto_infracao/auto_infracao/auto_infracao_csv.zip')

if not supabase_url or not supabase_key:
    raise ValueError("As variáveis de ambiente SUPABASE_URL e SUPABASE_KEY precisam ser definidas.")

# Usamos um DuckDB temporário em memória para o processamento inicial
temp_db = Database() 
data_loader = DataLoader(database=temp_db)
# Passamos a URL explicitamente para o DataLoader
data_loader.zip_url = ibama_zip_url

print("Baixando dados do IBAMA...")
success = data_loader.download_and_process()

if not success:
    raise RuntimeError("Falha ao baixar e processar os dados do IBAMA.")

df = temp_db.execute_query("SELECT * FROM ibama_infracao")
print(f"Dados processados. Total de {len(df)} registros.")

# 2. Configurar o cliente do Supabase
supabase: Client = create_client(supabase_url, supabase_key)
table_name = "ibama_infracao"

# 3. Limpar a tabela existente
print(f"Limpando a tabela '{table_name}' no Supabase antes do upload...")
# Deleta todas as linhas da tabela
# A API pode ter limites, então fazemos em lotes se necessário, mas delete() é geralmente eficiente
# O método abaixo deleta todas as linhas onde 'id' não é -1 (ou seja, todas)
supabase.table(table_name).delete().neq('id', -1).execute() 

# 4. Fazer o upload dos dados em lotes
chunk_size = 500 # Reduzir o lote pode aumentar a estabilidade
total_chunks = (len(df) // chunk_size) + 1
print(f"Iniciando upload de {len(df)} registros em {total_chunks} lotes de {chunk_size}...")

for i in range(0, len(df), chunk_size):
    chunk = df[i:i + chunk_size]
    data_to_insert = chunk.to_dict(orient='records')
    
    try:
        response = supabase.table(table_name).insert(data_to_insert).execute()
        print(f"  Lote {i//chunk_size + 1}/{total_chunks} enviado com sucesso.")
        time.sleep(1) 
    except Exception as e:
        print(f"❌ Erro ao enviar o lote {i//chunk_size + 1}: {e}")
        # Em um cenário de produção, você poderia adicionar lógica para tentar novamente

print("\n✅ Upload para o Supabase concluído com sucesso!")
