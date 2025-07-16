import duckdb
import pandas as pd
from pathlib import Path
import streamlit as st
from supabase import create_client, Client

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
            # --- CORREÇÃO APLICADA AQUI ---
            # Configuração para o Supabase (produção)
            # Usamos as variáveis já carregadas pelo módulo de configuração.
            url = config.SUPABASE_URL
            key = config.SUPABASE_KEY

            if url and key:
                self.supabase_client = create_client(url, key)
                print("✅ Conexão com Supabase estabelecida com sucesso.")
            else:
                # Este erro será visível nos logs do Streamlit se as secrets não estiverem configuradas
                print("❌ ERRO: Credenciais do Supabase (URL ou KEY) não encontradas.")
                st.error("A configuração do banco de dados na nuvem está incompleta. Verifique os segredos do Streamlit.")
        else:
            # Configuração para o DuckDB (local)
            db_path = config.DB_PATH
            Path(db_path).parent.mkdir(parents=True, exist_ok=True)
            self.conn = duckdb.connect(db_path)
            print(f"✅ Conexão com DuckDB local estabelecida: {db_path}")

    def execute_query(self, query: str) -> pd.DataFrame:
        """
        Executa uma consulta SQL no banco de dados apropriado.
        No modo cloud, chama uma função segura no Supabase.
        Localmente, executa diretamente no DuckDB.
        """
        env = 'Cloud (Supabase)' if self.is_cloud else 'Local (DuckDB)'
        print(f"Executando query no ambiente {env}...")

        try:
            if self.is_cloud and self.supabase_client:
                # Chama a função 'execute_secure_select' que deve existir no Supabase
                # para executar queries SELECT de forma segura.
                response = self.supabase_client.rpc(
                    'execute_secure_select',
                    {'query_string': query}
                ).execute()

                # Processa a resposta JSON retornada pela função
                if hasattr(response, 'data') and response.data:
                    # A resposta da RPC é uma lista, pegamos o primeiro item
                    result_data = response.data[0].get('result_json')
                    if result_data:
                        return pd.DataFrame(result_data)
                
                # Se a consulta não retornou linhas ou houve um erro não capturado, retorna um DataFrame vazio.
                return pd.DataFrame()

            elif not self.is_cloud and self.conn:
                # A lógica para o DuckDB local permanece a mesma
                return self.conn.execute(query).fetchdf()
                
        except Exception as e:
            print(f"❌ Erro na query: {e}")
            if "permission denied" in str(e).lower() or "operacao nao permitida" in str(e).lower():
                st.error("Ação bloqueada: Apenas consultas SELECT são permitidas por segurança.")
            else:
                st.error(f"Ocorreu um erro ao consultar o banco de dados: {e}")
            return pd.DataFrame()
        
        return pd.DataFrame()

    def save_dataframe(self, df: pd.DataFrame, table_name: str):
        """Salva um DataFrame. Funciona apenas no modo local com DuckDB."""
        if self.is_cloud:
            print("⚠️ Operação 'save_dataframe' não é suportada no modo cloud. Use o script de upload para o Supabase.")
            return False
        
        if not self.conn:
            print("❌ Erro: Conexão com DuckDB não disponível para salvar o DataFrame.")
            return False
            
        try:
            # Prepara o DataFrame para inserção, convertendo tudo para string para evitar erros de tipo
            df_copy = df.copy()
            for col in df_copy.columns:
                df_copy[col] = df_copy[col].astype(str)

            self.conn.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM df_copy")
            print(f"✅ DataFrame salvo com sucesso na tabela '{table_name}' do DuckDB.")
            return True
        except Exception as e:
            print(f"❌ Erro ao salvar no DuckDB: {e}")
            return False
    
    def get_table_info(self) -> pd.DataFrame:
        """Retorna o esquema da tabela para ser usado pelo LLM."""
        if self.is_cloud:
            # No modo cloud, retornamos um esquema fixo para evitar chamadas complexas ao DB.
            # Isso é mais rápido e suficiente para o LLM gerar as queries.
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
            # Localmente, lemos o esquema diretamente do DuckDB se a tabela existir
            try:
                return self.conn.execute("PRAGMA table_info(ibama_infracao)").fetchdf()
            except duckdb.CatalogException:
                print("⚠️ Tabela 'ibama_infracao' não encontrada no DuckDB local ao buscar esquema.")
                return pd.DataFrame()
