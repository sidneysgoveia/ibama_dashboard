# src/utils/database.py

import duckdb
import pandas as pd
from pathlib import Path
import os
import streamlit as st
from supabase import create_client, Client
import json

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
            key = get_secret("SUPABASE_KEY")
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
                # --- ALTERAÇÃO AQUI: Chamando a função do banco de dados ---
                # Usamos rpc() para chamar a função 'execute_secure_select' que criamos.
                # Passamos a query SQL como um argumento no dicionário.
                response = self.supabase_client.rpc(
                    'execute_secure_select',
                    {'query_string': query}
                ).execute()

                # A resposta vem em response.data, que é uma lista.
                # O primeiro item da lista contém nosso JSON de resultado.
                if response.data and response.data[0]['result_json']:
                    # Carregamos o JSON em um DataFrame do Pandas.
                    result_data = response.data[0]['result_json']
                    return pd.DataFrame(result_data)
                else:
                    # Se a consulta não retornou linhas, o JSON será nulo.
                    return pd.DataFrame()

            elif not self.is_cloud and self.conn:
                # A lógica para o DuckDB local permanece a mesma.
                return self.conn.execute(query).fetchdf()
                
        except Exception as e:
            print(f"Erro na query: {e}")
            # Se o erro for de permissão, é a nossa validação de segurança funcionando!
            if "Operação não permitida" in str(e):
                st.error("Ação bloqueada: Apenas consultas SELECT são permitidas.")
            return pd.DataFrame()
        
        return pd.DataFrame()

    # As funções abaixo são principalmente para o ambiente local com DuckDB
    def save_dataframe(self, df: pd.DataFrame, table_name: str):
        if self.is_cloud:
            print("Operação 'save_dataframe' não é suportada no modo cloud. Use o script de upload.")
            return False
        try:
            df = df.fillna('')
            for col in df.columns:
                df[col] = df[col].astype(str)
            self.conn.execute(f"DROP TABLE IF EXISTS {table_name}")
            self.conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM df")
            self.conn.commit()
            return True
        except Exception as e:
            print(f"Erro ao salvar: {e}")
            return False
    
    def check_table_exists(self, table_name: str) -> bool:
        if self.is_cloud and self.supabase_client:
            try:
                # Uma verificação simples que não depende de SQL dinâmico
                self.supabase_client.table(table_name).select('id', count='exact').limit(1).execute()
                return True
            except:
                return False
        elif not self.is_cloud and self.conn:
            try:
                self.conn.execute(f"SELECT COUNT(*) FROM {table_name} LIMIT 1")
                return True
            except:
                return False
        return False
    
    def get_table_info(self) -> pd.DataFrame:
        # Esta função ainda precisa de uma forma de obter o esquema do Supabase.
        # Por enquanto, podemos retornar um esquema fixo ou fazer uma chamada RPC específica.
        if self.is_cloud:
            # Para simplificar, podemos retornar um esquema fixo que sabemos ser verdadeiro.
            # A solução ideal seria outra função no Supabase para retornar o esquema.
            schema_data = [
                {'name': 'NOME_INFRATOR', 'type': 'TEXT'},
                {'name': 'CPF_CNPJ_INFRATOR', 'type': 'TEXT'},
                {'name': 'MUNICIPIO', 'type': 'TEXT'},
                {'name': 'UF', 'type': 'TEXT'},
                {'name': 'VAL_AUTO_INFRACAO', 'type': 'TEXT'},
                {'name': 'TIPO_INFRACAO', 'type': 'TEXT'},
                {'name': 'DES_INFRACAO', 'type': 'TEXT'},
                {'name': 'DAT_HORA_AUTO_INFRACAO', 'type': 'TEXT'},
                # Adicione outras colunas importantes aqui
            ]
            return pd.DataFrame(schema_data)
        else:
            try:
                return self.conn.execute("PRAGMA table_info(ibama_infracao)").fetchdf()
            except:
                return pd.DataFrame()
