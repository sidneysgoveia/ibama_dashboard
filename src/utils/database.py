import duckdb
import pandas as pd
from pathlib import Path
import os
import streamlit as st
from supabase import create_client, Client
import json

# Importa o módulo de configuração que está na raiz do projeto
import config

class Database:
    def __init__(self):
        """
        Inicializa a conexão com o banco de dados apropriado.
        Usa Supabase se estiver no Streamlit Cloud, caso contrário, usa DuckDB localmente.
        """
        self.is_cloud = config.IS_RUNNING_ON_STREAMLIT_CLOUD
        self.conn = None
        self.supabase_client = None

        if self.is_cloud:
            # Configuração para o Supabase (produção)
            url = config.get_secret("SUPABASE_URL")
            key = config.get_secret("SUPABASE_SERVICE_KEY")
            if url and key:
                self.supabase_client = create_client(url, key)
            else:
                # Este erro será visível nos logs do Streamlit se as secrets não estiverem configuradas
                print("ERRO: Credenciais do Supabase não encontradas nas secrets do Streamlit.")
                st.error("A configuração do banco de dados na nuvem está incompleta.")
        else:
            # Configuração para o DuckDB (local)
            db_path = config.get_secret('DB_PATH', default='data/ibama_infracao.db')
            Path(db_path).parent.mkdir(parents=True, exist_ok=True)
            self.conn = duckdb.connect(db_path)

    def execute_query(self, query: str) -> pd.DataFrame:
        """
        Executa uma consulta SQL no banco de dados apropriado.
        No modo cloud, chama uma função segura no Supabase.
        Localmente, executa diretamente no DuckDB.
        """
        print(f"Executando query no ambiente {'Cloud (Supabase)' if self.is_cloud else 'Local (DuckDB)'}...")
        try:
            if self.is_cloud and self.supabase_client:
                # Chama a função 'execute_secure_select' que criamos no Supabase
                response = self.supabase_client.rpc(
                    'execute_secure_select',
                    {'query_string': query}
                ).execute()

                # Processa a resposta JSON retornada pela função
                if response.data and response.data[0].get('result_json'):
                    result_data = response.data[0]['result_json']
                    return pd.DataFrame(result_data)
                else:
                    # Se a consulta não retornou linhas, o JSON será nulo ou a chave não existirá
                    return pd.DataFrame()

            elif not self.is_cloud and self.conn:
                # A lógica para o DuckDB local permanece a mesma
                return self.conn.execute(query).fetchdf()
                
        except Exception as e:
            print(f"Erro na query: {e}")
            if "Operação não permitida" in str(e):
                st.error("Ação bloqueada: Apenas consultas SELECT são permitidas.")
            return pd.DataFrame()
        
        return pd.DataFrame()

    def save_dataframe(self, df: pd.DataFrame, table_name: str):
        """Salva um DataFrame. Funciona apenas no modo local com DuckDB."""
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
            print(f"Erro ao salvar no DuckDB: {e}")
            return False
    
    def check_table_exists(self, table_name: str) -> bool:
        """Verifica se uma tabela específica existe no banco de dados."""
        if self.is_cloud and self.supabase_client:
            try:
                # Tenta fazer uma chamada leve para verificar a existência da tabela
                self.supabase_client.table(table_name).select('id', count='exact').limit(1).execute()
                return True
            except:
                return False
        elif not self.is_cloud and self.conn:
            try:
                self.conn.execute(f"SELECT COUNT(*) FROM {table_name} LIMIT 1")
                return True
            except duckdb.CatalogException:
                return False
            except Exception:
                return False
        return False
    
    def get_table_info(self) -> pd.DataFrame:
        """Retorna o esquema da tabela."""
        if self.is_cloud:
            # No modo cloud, retornamos um esquema fixo para evitar chamadas complexas ao DB.
            # Isso é suficiente para o LLM gerar as queries.
            schema_data = [
                {'name': 'NOME_INFRATOR', 'type': 'TEXT'},
                {'name': 'CPF_CNPJ_INFRATOR', 'type': 'TEXT'},
                {'name': 'MUNICIPIO', 'type': 'TEXT'},
                {'name': 'UF', 'type': 'TEXT'},
                {'name': 'VAL_AUTO_INFRACAO', 'type': 'TEXT'},
                {'name': 'TIPO_INFRACAO', 'type': 'TEXT'},
                {'name': 'DES_INFRACAO', 'type': 'TEXT'},
                {'name': 'DAT_HORA_AUTO_INFRACAO', 'type': 'TEXT'},
                {'name': 'NUM_LATITUDE_AUTO', 'type': 'TEXT'},
                {'name': 'NUM_LONGITUDE_AUTO', 'type': 'TEXT'},
                {'name': 'DES_STATUS_FORMULARIO', 'type': 'TEXT'},
                {'name': 'GRAVIDADE_INFRACAO', 'type': 'TEXT'},
            ]
            return pd.DataFrame(schema_data)
        else:
            # Localmente, lemos o esquema diretamente do DuckDB
            try:
                return self.conn.execute("PRAGMA table_info(ibama_infracao)").fetchdf()
            except:
                return pd.DataFrame()
