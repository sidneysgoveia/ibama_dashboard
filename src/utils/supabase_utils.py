import streamlit as st
import pandas as pd
from typing import List, Dict, Any
import hashlib

class SupabasePaginator:
    """Classe para buscar todos os dados do Supabase com paginação e cache por sessão."""
    
    def __init__(self, supabase_client):
        self.supabase = supabase_client
        self.page_size = 1000
    
    def _get_session_key(self, table_name: str = 'ibama_infracao', filters: str = "") -> str:
        """Gera chave única por sessão para cache."""
        session_id = st.session_state.get('session_id', '')
        if not session_id:
            # Gera ID único para esta sessão
            import time
            import random
            session_id = f"{time.time()}_{random.randint(1000, 9999)}"
            st.session_state.session_id = session_id
        
        # Hash dos filtros para cache específico
        filter_hash = hashlib.md5(f"{table_name}_{filters}_{session_id}".encode()).hexdigest()[:8]
        return f"data_{filter_hash}"
    
    @st.cache_data(ttl=1800, show_spinner=False)
    def get_all_records(_self, table_name: str = 'ibama_infracao', cache_key: str = None) -> pd.DataFrame:
        """
        Busca TODOS os registros únicos da tabela usando paginação.
        Cache isolado por sessão.
        """
        print(f"🔄 Iniciando busca paginada da tabela {table_name} (sessão: {cache_key})")
        
        all_data = []
        page = 0
        seen_infractions = set()  # Para garantir unicidade
        
        while True:
            start = page * _self.page_size
            end = start + _self.page_size - 1
            
            print(f"   📄 Página {page + 1}: registros {start} a {end}")
            
            try:
                # Busca uma página de dados
                result = _self.supabase.table(table_name).select('*').range(start, end).execute()
                
                if not result.data or len(result.data) == 0:
                    print(f"   ✅ Fim da paginação na página {page + 1}")
                    break
                
                # Filtra registros únicos por NUM_AUTO_INFRACAO
                unique_records = []
                for record in result.data:
                    num_auto = record.get('NUM_AUTO_INFRACAO')
                    if num_auto and num_auto not in seen_infractions:
                        seen_infractions.add(num_auto)
                        unique_records.append(record)
                
                all_data.extend(unique_records)
                print(f"   📊 Carregados {len(unique_records)} registros únicos (total: {len(all_data):,})")
                
                # Se retornou menos que o page_size, chegamos ao fim
                if len(result.data) < _self.page_size:
                    print(f"   ✅ Última página alcançada")
                    break
                
                page += 1
                
                # Limite de segurança
                if page > 100:
                    print(f"   ⚠️ Limite de segurança atingido (100 páginas)")
                    break
                
            except Exception as e:
                print(f"   ❌ Erro na página {page + 1}: {e}")
                break
        
        print(f"🎉 Paginação concluída: {len(all_data):,} registros únicos carregados")
        return pd.DataFrame(all_data)
    
    def get_filtered_data(self, selected_ufs: List[str] = None, year_range: tuple = None) -> pd.DataFrame:
        """
        Busca dados filtrados com cache por sessão.
        """
        # Gera chave única para esta sessão e filtros
        filter_str = f"ufs_{selected_ufs}_years_{year_range}"
        cache_key = self._get_session_key('ibama_infracao', filter_str)
        
        print(f"🔍 Buscando dados filtrados - Cache Key: {cache_key}")
        
        # Busca todos os dados únicos
        df = self.get_all_records('ibama_infracao', cache_key)
        
        if df.empty:
            return df
        
        original_count = len(df)
        print(f"📊 Dataset original: {original_count:,} registros únicos")
        
        # Aplica filtros
        if selected_ufs:
            df = df[df['UF'].isin(selected_ufs)]
            print(f"   🗺️ Após filtro UF: {len(df):,} registros")
        
        if year_range and 'DAT_HORA_AUTO_INFRACAO' in df.columns:
            try:
                df['DAT_HORA_AUTO_INFRACAO'] = pd.to_datetime(df['DAT_HORA_AUTO_INFRACAO'], errors='coerce')
                df = df[
                    (df['DAT_HORA_AUTO_INFRACAO'].dt.year >= year_range[0]) &
                    (df['DAT_HORA_AUTO_INFRACAO'].dt.year <= year_range[1])
                ]
                print(f"   📅 Após filtro ano {year_range}: {len(df):,} registros")
            except Exception as e:
                print(f"   ⚠️ Erro no filtro de data: {e}")
        
        print(f"✅ Dados filtrados finais: {len(df):,} registros únicos")
        return df
    
    def clear_cache(self):
        """Limpa o cache específico desta sessão."""
        # Limpa cache do Streamlit
        self.get_all_records.clear()
        
        # Remove dados da sessão
        if 'session_id' in st.session_state:
            del st.session_state.session_id
            
        print("🧹 Cache da sessão limpo")
    
    def get_real_count(self, table_name: str = 'ibama_infracao') -> Dict[str, int]:
        """Obtém contagens reais diretamente do banco."""
        try:
            # Count total de registros
            result_total = self.supabase.table(table_name).select('*', count='exact').limit(1).execute()
            total_records = getattr(result_total, 'count', 0)
            
            # Count de NUM_AUTO_INFRACAO únicos usando SQL
            query = f"""
            SELECT COUNT(DISTINCT "NUM_AUTO_INFRACAO") as unique_count 
            FROM {table_name} 
            WHERE "NUM_AUTO_INFRACAO" IS NOT NULL
            """
            
            # Para Supabase, usamos RPC se disponível
            try:
                unique_result = self.supabase.rpc('count_unique_infractions').execute()
                unique_count = unique_result.data if unique_result.data else 0
            except:
                # Fallback: buscar todos e contar localmente
                df = self.get_all_records(table_name)
                unique_count = df['NUM_AUTO_INFRACAO'].nunique() if 'NUM_AUTO_INFRACAO' in df.columns else 0
            
            return {
                'total_records': total_records,
                'unique_infractions': unique_count,
                'timestamp': pd.Timestamp.now()
            }
        except Exception as e:
            print(f"Erro ao obter contagens reais: {e}")
            return {'total_records': 0, 'unique_infractions': 0, 'timestamp': pd.Timestamp.now()}
