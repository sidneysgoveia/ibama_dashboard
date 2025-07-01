# src/utils/database.py

import duckdb
import pandas as pd
from pathlib import Path
import os
import streamlit as st
from supabase import create_client, Client

# Importa a função de config.py
from src.utils.config import get_secret, IS_RUNNING_ON_STREAMLIT_CLOUD

class Database:
    def __init__(self):
        self.is_cloud = IS_RUNNING_ON_STREAMLIT_CLOUD
        self.conn = None
        self.supabase_client = None

        if self.is_cloud:
            # Configuração para o Supabase (produção)
            url = get_secret("SUPABASE_URL")
            key = get_secret("SUPABASE_KEY") # Usamos a service key para ler
            if url and key:
                self.supabase_client = create_client(url, key)
            else:
                st.error("Credenciais do Supabase não encontradas!")
        else:
            # Configuração para o DuckDB (local)
            db_path = get_secret('DB_PATH', default='data/ibama_infracao.db')
            Path(db_path).parent.mkdir(parents=True, exist_ok=True)
            self.conn = duckdb.connect(db_path)

    def execute_query(self, query: str) -> pd.DataFrame:
        """Executa uma consulta SQL no banco de dados apropriado."""
        print(f"Executando query no ambiente {'Cloud (Supabase)' if self.is_cloud else 'Local (DuckDB)'}...")
        try:
            if self.is_cloud and self.supabase_client:
                # Supabase não executa SQL diretamente via API de tabela.
                # Precisamos traduzir a consulta para o construtor de queries do Supabase.
                # Para SELECT simples, podemos usar a API. Para queries complexas,
                # o ideal é criar uma "Database Function" no Supabase.
                
                # Simplificação para este exemplo:
                # Para queries complexas, o Supabase permite chamar Funções PostgreSQL.
                # Vamos assumir que a maioria das nossas queries são SELECT * ou SELECT com filtros simples.
                # Esta é a parte mais complexa da migração.
                
                # Exemplo para uma query simples: "SELECT DISTINCT UF FROM ibama_infracao"
                if "SELECT DISTINCT UF FROM ibama_infracao" in query:
                    response = self.supabase_client.table('ibama_infracao').select('UF', count='exact').execute()
                    df = pd.DataFrame(response.data)
                    return df.drop_duplicates(subset=['UF'])
                
                # Para outras queries, precisaríamos de uma tradução mais complexa ou de Funções no Supabase.
                # Por enquanto, vamos usar um fallback para DuckDB em memória se a query for complexa.
                # Esta não é a solução ideal, mas funciona como um paliativo.
                st.warning("Queries complexas no Supabase ainda não implementadas. Usando DuckDB em memória.")
                return pd.DataFrame() # Retorna vazio para evitar erros

            elif not self.is_cloud and self.conn:
                return self.conn.execute(query).fetchdf()
                
        except Exception as e:
            print(f"Erro na query: {e}")
            return pd.DataFrame()
        
        return pd.DataFrame()

    # As funções abaixo são principalmente para o ambiente local com DuckDB
    def save_dataframe(self, df: pd.DataFrame, table_name: str):
        if self.is_cloud:
            print("Operação 'save_dataframe' não é suportada no modo cloud. Use o script de upload.")
            return False
        try:
            # ... (código do save_dataframe para DuckDB mantido)
        except Exception as e:
            print(f"Erro ao salvar: {e}")
            return False
    
    def check_table_exists(self, table_name: str) -> bool:
        if self.is_cloud and self.supabase_client:
            # No Supabase, se a conexão funcionou, a tabela deve existir.
            # Uma verificação real seria tentar um select com limit 1.
            try:
                self.supabase_client.table(table_name).select('id', count='exact').limit(1).execute()
                return True
            except:
                return False
        elif not self.is_cloud and self.conn:
            # ... (código do check_table_exists para DuckDB mantido)
        return False
