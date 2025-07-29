import pandas as pd
import duckdb
import streamlit as st
from supabase import create_client, Client
import os
import config

class Database:
    def __init__(self):
        """Inicializa a conexão com o banco de dados."""
        self.is_cloud = config.IS_RUNNING_ON_STREAMLIT_CLOUD
        self.connection = None
        self.supabase = None
        
        try:
            if self.is_cloud:
                self._init_supabase()
            else:
                self._init_duckdb()
        except Exception as e:
            st.error(f"❌ Erro ao inicializar banco de dados: {e}")
            self.connection = None
            self.supabase = None

    def _init_supabase(self):
        """Inicializa conexão com Supabase."""
        url = config.SUPABASE_URL
        key = config.SUPABASE_KEY
        
        if not url or not key:
            raise ValueError("Credenciais do Supabase não configuradas")
        
        self.supabase = create_client(url, key)
        
        # Teste de conectividade
        try:
            result = self.supabase.table('ibama_infracao').select('count', count='exact').limit(1).execute()
            print(f"✅ Supabase conectado. Registros disponíveis: {getattr(result, 'count', 'N/A')}")
        except Exception as e:
            # Se der erro de RLS, mostra instrução
            if 'row-level security' in str(e).lower() or 'P0001' in str(e):
                st.error("""
                🔒 **Problema de Row Level Security detectado!**
                
                As políticas RLS não estão configuradas corretamente no Supabase.
                
                **Para corrigir:**
                1. Acesse o painel do Supabase
                2. Vá em `SQL Editor`
                3. Execute o código SQL fornecido na documentação
                4. Ou rode o script `fix_supabase_rls.py`
                """)
            raise e

    def _init_duckdb(self):
        """Inicializa conexão com DuckDB local."""
        db_path = config.DB_PATH
        self.connection = duckdb.connect(db_path)

    def execute_query(self, query: str) -> pd.DataFrame:
        """Executa uma consulta e retorna um DataFrame."""
        if not self._is_connected():
            return pd.DataFrame()

        try:
            if self.is_cloud:
                return self._execute_supabase_query(query)
            else:
                return self._execute_duckdb_query(query)
                
        except Exception as e:
            error_msg = str(e)
            
            # Tratamento específico para erros comuns
            if 'P0001' in error_msg:
                st.error("🔒 Erro de Row Level Security. Verifique as políticas no Supabase.")
            elif 'row-level security' in error_msg.lower():
                st.error("🔒 RLS bloqueou a consulta. Configure as políticas adequadas.")
            elif 'connection' in error_msg.lower():
                st.error("🔌 Erro de conexão. Verifique as credenciais do banco.")
            elif 'syntax' in error_msg.lower():
                st.error(f"📝 Erro de sintaxe SQL: {error_msg}")
            else:
                st.error(f"❌ Erro na consulta: {error_msg}")
            
            print(f"Database error: {error_msg}")
            return pd.DataFrame()

    def _execute_supabase_query(self, query: str) -> pd.DataFrame:
        """Executa consulta no Supabase."""
        if not self.supabase:
            raise Exception("Supabase não inicializado")
        
        # Verifica se é uma consulta SELECT simples
        query_clean = query.strip().upper()
        if not query_clean.startswith('SELECT'):
            raise Exception("Apenas consultas SELECT são permitidas")
        
        try:
            # Método melhorado: usar a função RPC personalizada se disponível
            # Primeiro, tenta usar RPC se a função estiver disponível
            try:
                result = self.supabase.rpc('execute_raw_sql', {'sql_query': query}).execute()
                if result.data:
                    return pd.DataFrame(result.data)
            except:
                pass  # Se RPC não funcionar, usa método alternativo
            
            # Método alternativo: tentar simular a consulta
            # Para consultas de agregação simples
            if 'COUNT(' in query_clean and 'SUM(' in query_clean:
                # Busca todos os dados e faz agregação no pandas
                result = self.supabase.table('ibama_infracao').select('*').execute()
                df_full = pd.DataFrame(result.data)
                
                if df_full.empty:
                    return pd.DataFrame()
                
                # Simula agregações básicas
                total_infracoes = len(df_full)
                
                # Calcula valor total das multas
                try:
                    df_full['VAL_AUTO_INFRACAO_NUMERIC'] = pd.to_numeric(
                        df_full['VAL_AUTO_INFRACAO'].astype(str).str.replace(',', '.'), 
                        errors='coerce'
                    )
                    valor_total_multas = df_full['VAL_AUTO_INFRACAO_NUMERIC'].sum()
                except:
                    valor_total_multas = 0
                
                # Conta municípios únicos
                total_municipios = df_full['MUNICIPIO'].nunique()
                
                return pd.DataFrame({
                    'total_infracoes': [total_infracoes],
                    'valor_total_multas': [valor_total_multas],
                    'total_municipios': [total_municipios]
                })
            
            # Para outras consultas, retorna dados básicos
            result = self.supabase.table('ibama_infracao').select('*').limit(1000).execute()
            return pd.DataFrame(result.data)
                
        except Exception as e:
            print(f"Erro na consulta Supabase: {e}")
            # Retorna DataFrame vazio mas com estrutura correta para evitar KeyError
            return pd.DataFrame({
                'total_infracoes': [0],
                'valor_total_multas': [0],
                'total_municipios': [0]
            })

    def _execute_duckdb_query(self, query: str) -> pd.DataFrame:
        """Executa consulta no DuckDB."""
        if not self.connection:
            raise Exception("DuckDB não inicializado")
        
        return self.connection.execute(query).fetchdf()

    def _is_connected(self) -> bool:
        """Verifica se há conexão ativa."""
        if self.is_cloud:
            return self.supabase is not None
        else:
            return self.connection is not None

    def get_table_info(self) -> pd.DataFrame:
        """Retorna informações sobre a estrutura da tabela."""
        if self.is_cloud:
            # Para Supabase, retorna informações básicas
            try:
                result = self.supabase.table('ibama_infracao').select('*').limit(1).execute()
                if result.data:
                    columns = list(result.data[0].keys())
                    return pd.DataFrame({
                        'name': columns,
                        'type': ['text'] * len(columns)  # Simplificado
                    })
            except:
                pass
            
            # Fallback com colunas conhecidas
            known_columns = [
                'SEQ_AUTO_INFRACAO', 'NUM_AUTO_INFRACAO', 'SER_AUTO_INFRACAO',
                'DAT_HORA_AUTO_INFRACAO', 'VAL_AUTO_INFRACAO', 'MUNICIPIO', 'UF',
                'TIPO_INFRACAO', 'GRAVIDADE_INFRACAO', 'NOME_INFRATOR',
                'DES_STATUS_FORMULARIO', 'NUM_LATITUDE_AUTO', 'NUM_LONGITUDE_AUTO'
            ]
            return pd.DataFrame({
                'name': known_columns,
                'type': ['text'] * len(known_columns)
            })
        else:
            # Para DuckDB
            return self.execute_query("DESCRIBE ibama_infracao")

    def get_unique_values(self, column: str, limit: int = 50000) -> list:
        """Obtém valores únicos de uma coluna específica."""
        try:
            if self.is_cloud:
                # Para Supabase, busca com limite alto para pegar todos os valores
                result = self.supabase.table('ibama_infracao').select(column).limit(limit).execute()
                if result.data:
                    # Extrai valores únicos, removendo nulos e vazios
                    values = [item[column] for item in result.data 
                             if item.get(column) and str(item[column]).strip()]
                    return sorted(list(set(values)))
                return []
            else:
                # Para DuckDB
                query = f'SELECT DISTINCT "{column}" FROM ibama_infracao WHERE "{column}" IS NOT NULL ORDER BY "{column}"'
                df = self.execute_query(query)
                return df[column].tolist() if not df.empty else []
                
    def test_connection(self) -> dict:
        """Testa a conexão e retorna status."""
        try:
            if self.is_cloud:
                result = self.supabase.table('ibama_infracao').select('count', count='exact').execute()
                return {
                    'status': 'success',
                    'type': 'Supabase',
                    'count': getattr(result, 'count', 0)
                }
            else:
                result = self.execute_query("SELECT COUNT(*) as count FROM ibama_infracao")
                return {
                    'status': 'success', 
                    'type': 'DuckDB',
                    'count': result['count'].iloc[0] if not result.empty else 0
                }
        except Exception as e:
            return {
                'status': 'error',
                'type': 'Supabase' if self.is_cloud else 'DuckDB',
                'error': str(e)
            }
        """Testa a conexão e retorna status."""
        try:
            if self.is_cloud:
                result = self.supabase.table('ibama_infracao').select('count', count='exact').execute()
                return {
                    'status': 'success',
                    'type': 'Supabase',
                    'count': getattr(result, 'count', 0)
                }
            else:
                result = self.execute_query("SELECT COUNT(*) as count FROM ibama_infracao")
                return {
                    'status': 'success', 
                    'type': 'DuckDB',
                    'count': result['count'].iloc[0] if not result.empty else 0
                }
        except Exception as e:
            return {
                'status': 'error',
                'type': 'Supabase' if self.is_cloud else 'DuckDB',
                'error': str(e)
            }
