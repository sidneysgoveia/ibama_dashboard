# src/utils/database.py

import duckdb
import pandas as pd
from pathlib import Path
import os
import streamlit as st
from supabase import create_client, Client
import json

# --- ALTERAÇÃO AQUI: Corrigindo o caminho da importação ---
# Como 'database.py' e 'config.py' estão na mesma pasta 'utils',
# podemos importar diretamente.
# No entanto, para manter a consistência com a forma como o app.py (na raiz) vê os módulos,
# a melhor prática é usar o caminho absoluto a partir da raiz do projeto.
# O problema real é que o Python pode não estar vendo 'src' como um pacote.
# Vamos tentar uma importação relativa primeiro, que é mais robusta.
from config import get_secret, IS_RUNNING_ON_STREAMLIT_CLOUD

class Database:
    # ... (o resto do arquivo permanece o mesmo) ...
    def __init__(self):
        self.is_cloud = IS_RUNNING_ON_STREAMLIT_CLOUD
        self.conn = None
        self.supabase_client = None

        if self.is_cloud:
            url = get_secret("SUPABASE_URL")
            key = get_secret("SUPABASE_SERVICE_KEY")
            if url and key:
                self.supabase_client = create_client(url, key)
            else:
                st.error("Credenciais do Supabase não encontradas!")
        else:
            db_path = get_secret('DB_PATH', default='data/ibama_infracao.db')
            Path(db_path).parent.mkdir(parents=True, exist_ok=True)
            self.conn = duckdb.connect(db_path)

    def execute_query(self, query: str) -> pd.DataFrame:
        print(f"Executando query no ambiente {'Cloud (Supabase)' if self.is_cloud else 'Local (DuckDB)'}...")
        try:
            if self.is_cloud and self.supabase_client:
                response = self.supabase_client.rpc('execute_secure_select', {'query_string': query}).execute()
                if response.data and response.data[0]['result_json']:
                    result_data = response.data[0]['result_json']
                    return pd.DataFrame(result_data)
                else:
                    return pd.DataFrame()
            elif not self.is_cloud and self.conn:
                return self.conn.execute(query).fetchdf()
        except Exception as e:
            print(f"Erro na query: {e}")
            if "Operação não permitida" in str(e):
                st.error("Ação bloqueada: Apenas consultas SELECT são permitidas.")
            return pd.DataFrame()
        return pd.DataFrame()

    def save_dataframe(self, df: pd.DataFrame, table_name: str):
        if self.is_cloud:
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
        if self.is_cloud:
            schema_data = [
                {'name': 'NOME_INFRATOR', 'type': 'TEXT'}, {'name': 'CPF_CNPJ_INFRATOR', 'type': 'TEXT'},
                {'name': 'MUNICIPIO', 'type': 'TEXT'}, {'name': 'UF', 'type': 'TEXT'},
                {'name': 'VAL_AUTO_INFRACAO', 'type': 'TEXT'}, {'name': 'TIPO_INFRACAO', 'type': 'TEXT'},
                {'name': 'DES_INFRACAO', 'type': 'TEXT'}, {'name': 'DAT_HORA_AUTO_INFRACAO', 'type': 'TEXT'},
            ]
            return pd.DataFrame(schema_data)
        else:
            try:
                return self.conn.execute("PRAGMA table_info(ibama_infracao)").fetchdf()
            except:
                return pd.DataFrame()
