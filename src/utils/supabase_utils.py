import pandas as pd
import streamlit as st
from typing import List, Dict, Any
import hashlib
import time
import random

class SupabasePaginator:
    """Classe para buscar todos os dados do Supabase com paginação e garantia de unicidade."""
    
    def __init__(self, supabase_client):
        self.supabase = supabase_client
        self.page_size = 1000  # Tamanho da página (limite do Supabase)
    
    def _get_session_key(self, table_name: str = 'ibama_infracao', filters: str = "") -> str:
        """Gera chave única por sessão para cache."""
        session_id = st.session_state.get('session_id', '')
        if not session_id:
            # Gera ID único para esta sessão
            session_id = f"{time.time()}_{random.randint(1000, 9999)}"
            st.session_state.session_id = session_id
        
        # Hash dos filtros para cache específico
        filter_hash = hashlib.md5(f"{table_name}_{filters}_{session_id}".encode()).hexdigest()[:8]
        return f"data_{filter_hash}"
    
    @st.cache_data(ttl=1800, show_spinner=False)
    def get_all_records(_self, table_name: str = 'ibama_infracao', cache_key: str = None) -> pd.DataFrame:
        """
        Busca TODOS os registros únicos da tabela usando paginação.
        GARANTIA DE UNICIDADE: Remove duplicatas por NUM_AUTO_INFRACAO.
        """
        print(f"🔄 Iniciando busca paginada da tabela {table_name} (sessão: {cache_key})")
        
        all_data = []
        page = 0
        seen_infractions = set()  # Para garantir unicidade durante a busca
        
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
                
                # CRÍTICO: Filtra registros únicos por NUM_AUTO_INFRACAO durante a busca
                unique_records = []
                for record in result.data:
                    num_auto = record.get('NUM_AUTO_INFRACAO')
                    if num_auto and num_auto not in seen_infractions:
                        seen_infractions.add(num_auto)
                        unique_records.append(record)
                
                all_data.extend(unique_records)
                print(f"   📊 Carregados {len(unique_records)} registros únicos desta página (total: {len(all_data):,})")
                
                # Se retornou menos que o page_size, chegamos ao fim
                if len(result.data) < _self.page_size:
                    print(f"   ✅ Última página alcançada")
                    break
                
                page += 1
                
                # Limite de segurança
                if page > 50:  # Reduzido para 50 páginas (50k registros)
                    print(f"   ⚠️ Limite de segurança atingido (50 páginas)")
                    break
                
            except Exception as e:
                print(f"   ❌ Erro na página {page + 1}: {e}")
                break
        
        print(f"🎉 Paginação concluída: {len(all_data):,} registros únicos carregados")
        print(f"🔍 Infrações únicas encontradas: {len(seen_infractions):,}")
        
        df = pd.DataFrame(all_data)
        
        # VALIDAÇÃO FINAL: Garante que não há duplicatas no DataFrame
        if not df.empty and 'NUM_AUTO_INFRACAO' in df.columns:
            original_count = len(df)
            df_unique = df.drop_duplicates(subset=['NUM_AUTO_INFRACAO'], keep='first')
            final_count = len(df_unique)
            
            if original_count != final_count:
                print(f"🚨 AVISO: {original_count - final_count} duplicatas removidas na validação final")
                df = df_unique
        
        return df
    
    def get_filtered_data(self, selected_ufs: List[str] = None, year_range: tuple = None) -> pd.DataFrame:
        """
        Busca dados filtrados com garantia de unicidade.
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
        if selected_ufs and 'UF' in df.columns:
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
    
    def get_real_count(self, table_name: str = 'ibama_infracao') -> Dict[str, int]:
        """
        Obtém contagens reais diretamente do banco.
        CORRIGIDO: Usa paginação para contar infrações únicas corretamente.
        """
        try:
            print("🔍 Iniciando contagem real dos dados...")
            
            # 1. Count total de registros usando API do Supabase
            result_total = self.supabase.table(table_name).select('*', count='exact').limit(1).execute()
            total_records = getattr(result_total, 'count', 0)
            print(f"📊 Total de registros no banco: {total_records:,}")
            
            # 2. Para contar infrações únicas, precisa buscar todos os NUM_AUTO_INFRACAO
            print("🔄 Buscando todos os NUM_AUTO_INFRACAO para contagem única...")
            
            all_num_auto = set()
            page = 0
            
            while True:
                start = page * self.page_size
                end = start + self.page_size - 1
                
                try:
                    # Busca apenas a coluna NUM_AUTO_INFRACAO para economia de recursos
                    result = self.supabase.table(table_name).select('NUM_AUTO_INFRACAO').range(start, end).execute()
                    
                    if not result.data or len(result.data) == 0:
                        break
                    
                    # Adiciona ao set (automaticamente remove duplicatas)
                    for record in result.data:
                        num_auto = record.get('NUM_AUTO_INFRACAO')
                        if num_auto:
                            all_num_auto.add(num_auto)
                    
                    print(f"   📄 Página {page + 1}: {len(result.data)} registros, {len(all_num_auto)} únicos totais")
                    
                    # Se retornou menos que page_size, acabou
                    if len(result.data) < self.page_size:
                        break
                    
                    page += 1
                    
                    # Limite de segurança
                    if page > 50:
                        print(f"   ⚠️ Limite de segurança atingido")
                        break
                        
                except Exception as e:
                    print(f"   ❌ Erro na página {page + 1}: {e}")
                    break
            
            unique_count = len(all_num_auto)
            
            print(f"✅ Contagem concluída:")
            print(f"   📊 Total de registros: {total_records:,}")
            print(f"   🔢 Infrações únicas: {unique_count:,}")
            print(f"   📉 Duplicatas: {total_records - unique_count:,}")
            
            return {
                'total_records': total_records,
                'unique_infractions': unique_count,
                'duplicates': total_records - unique_count,
                'timestamp': pd.Timestamp.now()
            }
            
        except Exception as e:
            print(f"❌ Erro ao obter contagens reais: {e}")
            return {
                'total_records': 0,
                'unique_infractions': 0,
                'duplicates': 0,
                'timestamp': pd.Timestamp.now(),
                'error': str(e)
            }
    
    def clear_cache(self):
        """Limpa o cache específico desta sessão."""
        try:
            # Limpa cache do Streamlit para este método
            self.get_all_records.clear()
            
            # Remove dados da sessão
            if 'session_id' in st.session_state:
                del st.session_state.session_id
            
            print("🧹 Cache da sessão limpo")
            return True
        except Exception as e:
            print(f"❌ Erro ao limpar cache: {e}")
            return False
    
    def get_sample_data(self, limit: int = 1000) -> pd.DataFrame:
        """
        Busca uma amostra dos dados para testes rápidos.
        Útil para desenvolvimento e debug.
        """
        try:
            print(f"🔍 Buscando amostra de {limit} registros...")
            
            result = self.supabase.table('ibama_infracao').select('*').limit(limit).execute()
            
            if result.data:
                df = pd.DataFrame(result.data)
                
                # Remove duplicatas da amostra
                if 'NUM_AUTO_INFRACAO' in df.columns:
                    original_count = len(df)
                    df = df.drop_duplicates(subset=['NUM_AUTO_INFRACAO'], keep='first')
                    unique_count = len(df)
                    
                    print(f"📊 Amostra: {original_count} registros → {unique_count} únicos")
                
                return df
            else:
                return pd.DataFrame()
                
        except Exception as e:
            print(f"❌ Erro ao buscar amostra: {e}")
            return pd.DataFrame()
    
    def validate_data_integrity(self) -> Dict[str, Any]:
        """
        Valida a integridade dos dados carregados.
        Útil para diagnóstico e debug.
        """
        try:
            print("🔍 Validando integridade dos dados...")
            
            # Busca amostra para análise
            df_sample = self.get_sample_data(5000)
            
            if df_sample.empty:
                return {"error": "Nenhum dado disponível para validação"}
            
            # Análises de integridade
            validation_info = {
                "sample_size": len(df_sample),
                "columns_count": len(df_sample.columns),
                "has_num_auto_infracao": 'NUM_AUTO_INFRACAO' in df_sample.columns,
                "null_num_auto_count": 0,
                "empty_num_auto_count": 0,
                "unique_num_auto_count": 0,
                "duplicate_detection": False
            }
            
            if validation_info["has_num_auto_infracao"]:
                # Analisa coluna NUM_AUTO_INFRACAO
                validation_info["null_num_auto_count"] = df_sample['NUM_AUTO_INFRACAO'].isna().sum()
                validation_info["empty_num_auto_count"] = (df_sample['NUM_AUTO_INFRACAO'] == '').sum()
                validation_info["unique_num_auto_count"] = df_sample['NUM_AUTO_INFRACAO'].nunique()
                
                # Detecta duplicatas
                total_valid = len(df_sample) - validation_info["null_num_auto_count"] - validation_info["empty_num_auto_count"]
                validation_info["duplicate_detection"] = validation_info["unique_num_auto_count"] < total_valid
            
            # Análise de datas
            if 'DAT_HORA_AUTO_INFRACAO' in df_sample.columns:
                try:
                    df_sample['DATE_PARSED'] = pd.to_datetime(df_sample['DAT_HORA_AUTO_INFRACAO'], errors='coerce')
                    validation_info["valid_dates"] = df_sample['DATE_PARSED'].notna().sum()
                    validation_info["date_range"] = {
                        "min": df_sample['DATE_PARSED'].min(),
                        "max": df_sample['DATE_PARSED'].max()
                    }
                except:
                    validation_info["date_parsing_error"] = True
            
            return validation_info
            
        except Exception as e:
            return {"error": f"Erro na validação: {str(e)}"}
