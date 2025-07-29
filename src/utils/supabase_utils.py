import pandas as pd
import streamlit as st
from typing import List, Dict, Any

class SupabasePaginator:
    """Classe para buscar todos os dados do Supabase com paginação."""
    
    def __init__(self, supabase_client):
        self.supabase = supabase_client
        self.page_size = 1000  # Tamanho da página (limite do Supabase)
    
    @st.cache_data(ttl=1800)  # Cache por 30 minutos
    def get_all_records(_self, table_name: str = 'ibama_infracao') -> pd.DataFrame:
        """
        Busca TODOS os registros da tabela usando paginação.
        O underscore no parâmetro _self evita problemas de hash do Streamlit.
        """
        print(f"🔄 Iniciando busca paginada da tabela {table_name}")
        
        all_data = []
        page = 0
        
        while True:
            # Calcula range para esta página
            start = page * _self.page_size
            end = start + _self.page_size - 1
            
            print(f"   📄 Página {page + 1}: registros {start} a {end}")
            
            try:
                # Busca uma página de dados
                result = _self.supabase.table(table_name).select('*').range(start, end).execute()
                
                if not result.data or len(result.data) == 0:
                    print(f"   ✅ Fim da paginação na página {page + 1}")
                    break
                
                all_data.extend(result.data)
                print(f"   📊 Carregados {len(result.data)} registros (total: {len(all_data):,})")
                
                # Se retornou menos que o page_size, chegamos ao fim
                if len(result.data) < _self.page_size:
                    print(f"   ✅ Última página alcançada")
                    break
                
                page += 1
                
                # Limite de segurança para evitar loops infinitos
                if page > 100:  # Máximo 100k registros
                    print(f"   ⚠️ Limite de segurança atingido (100 páginas)")
                    break
                
            except Exception as e:
                print(f"   ❌ Erro na página {page + 1}: {e}")
                break
        
        print(f"🎉 Paginação concluída: {len(all_data):,} registros carregados")
        return pd.DataFrame(all_data)
    
    @st.cache_data(ttl=3600)  # Cache por 1 hora
    def get_filtered_data(_self, selected_ufs: List[str] = None, year_range: tuple = None) -> pd.DataFrame:
        """
        Busca todos os dados e aplica filtros localmente.
        Mais eficiente que múltiplas consultas ao banco.
        """
        print(f"🔍 Buscando dados filtrados - UFs: {selected_ufs}, Anos: {year_range}")
        
        # Busca todos os dados
        df = _self.get_all_records()
        
        if df.empty:
            return df
        
        original_count = len(df)
        print(f"📊 Dataset original: {original_count:,} registros")
        
        # Aplica filtros
        if selected_ufs:
            df = df[df['UF'].isin(selected_ufs)]
            print(f"   🗺️ Após filtro UF: {len(df):,} registros")
        
        if year_range and 'DAT_HORA_AUTO_INFRACAO' in df.columns:
            try:
                # Converte datas uma única vez
                df['DAT_HORA_AUTO_INFRACAO'] = pd.to_datetime(df['DAT_HORA_AUTO_INFRACAO'], errors='coerce')
                
                # Aplica filtro de ano
                df = df[
                    (df['DAT_HORA_AUTO_INFRACAO'].dt.year >= year_range[0]) &
                    (df['DAT_HORA_AUTO_INFRACAO'].dt.year <= year_range[1])
                ]
                print(f"   📅 Após filtro ano {year_range}: {len(df):,} registros")
                
            except Exception as e:
                print(f"   ⚠️ Erro no filtro de data: {e}")
        
        print(f"✅ Dados filtrados finais: {len(df):,} registros")
        return df
    
    def get_count(self, table_name: str = 'ibama_infracao') -> int:
        """Obtém contagem total rápida usando count API."""
        try:
            result = self.supabase.table(table_name).select('*', count='exact').limit(1).execute()
            return getattr(result, 'count', 0)
        except:
            return 0
    
    def clear_cache(self):
        """Limpa o cache para forçar nova busca."""
        self.get_all_records.clear()
        self.get_filtered_data.clear()
        print("🧹 Cache limpo")
