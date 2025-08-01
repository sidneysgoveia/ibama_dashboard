import pandas as pd
import streamlit as st
from typing import List, Dict, Any, Optional
import hashlib
import time
import random
import uuid

class SupabasePaginator:
    """Classe para buscar todos os dados do Supabase com paginação e garantia de unicidade - CORRIGIDA."""
    
    def __init__(self, supabase_client):
        self.supabase = supabase_client
        self.page_size = 1000  # Tamanho da página (limite do Supabase)
        self.max_pages = 25    # AUMENTADO: permite até 25k registros (suficiente para 21k)
    
    def _get_session_key(self, table_name: str = 'ibama_infracao', filters: str = "") -> str:
        """Gera chave única POR SESSÃO para cache isolado."""
        # Usa session_id do Streamlit para isolamento real entre usuários
        if 'session_uuid' not in st.session_state:
            st.session_state.session_uuid = str(uuid.uuid4())[:8]
        
        session_id = st.session_state.session_uuid
        
        # Hash dos filtros para cache específico
        filter_hash = hashlib.md5(f"{table_name}_{filters}_{session_id}".encode()).hexdigest()[:8]
        return f"data_{session_id}_{filter_hash}"
    
    def get_all_records(self, table_name: str = 'ibama_infracao', cache_key: str = None) -> pd.DataFrame:
        """
        Busca TODOS os registros únicos da tabela usando paginação.
        CORRIGIDO: Cache por sessão e paginação completa.
        """
        # Se não forneceu cache_key, gera um único para esta sessão
        if cache_key is None:
            cache_key = self._get_session_key(table_name)
        
        # CRÍTICO: Verifica se já está em cache desta sessão
        cache_storage_key = f"paginated_data_{cache_key}"
        if cache_storage_key in st.session_state:
            print(f"✅ Retornando dados do cache da sessão: {cache_storage_key}")
            return st.session_state[cache_storage_key]
        
        print(f"🔄 Iniciando busca paginada da tabela {table_name} (sessão: {cache_key})")
        
        all_data = []
        page = 0
        seen_infractions = set()  # Para garantir unicidade durante a busca
        
        while True:
            start = page * self.page_size
            end = start + self.page_size - 1
            
            print(f"   📄 Página {page + 1}: registros {start} a {end}")
            
            try:
                # Busca uma página de dados
                result = self.supabase.table(table_name).select('*').range(start, end).execute()
                
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
                print(f"   📊 Carregados {len(unique_records)} registros únicos desta página (total acumulado: {len(all_data):,})")
                
                # Se retornou menos que o page_size, chegamos ao fim
                if len(result.data) < self.page_size:
                    print(f"   ✅ Última página alcançada (dados finalizados)")
                    break
                
                page += 1
                
                # CORRIGIDO: Limite de segurança aumentado para capturar todos os dados
                if page >= self.max_pages:
                    print(f"   ⚠️ Limite de segurança atingido ({self.max_pages} páginas)")
                    print(f"   💡 Total únicos coletados até aqui: {len(seen_infractions):,}")
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
        
        # CRÍTICO: Armazena no cache da sessão (não global)
        st.session_state[cache_storage_key] = df
        print(f"💾 Dados armazenados no cache da sessão: {cache_storage_key}")
        
        return df
    
    def get_filtered_data(self, selected_ufs: List[str] = None, year_range: tuple = None) -> pd.DataFrame:
        """
        Busca dados filtrados com garantia de unicidade.
        CORRIGIDO: Cache por sessão específica.
        """
        # Gera chave única para esta sessão e filtros
        filter_str = f"ufs_{selected_ufs}_years_{year_range}"
        cache_key = self._get_session_key('ibama_infracao', filter_str)
        
        print(f"🔍 Buscando dados filtrados - Cache Key: {cache_key}")
        
        # Busca todos os dados únicos desta sessão
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
    
    def get_real_count(self, table_name: str = 'ibama_infracao') -> Dict[str, Any]:
        """
        Obtém contagens reais diretamente do banco.
        CORRIGIDO: Usa paginação completa para contar infrações únicas corretamente.
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
                    
                    # CORRIGIDO: Limite de segurança aumentado
                    if page >= self.max_pages:
                        print(f"   ⚠️ Limite de segurança atingido ({self.max_pages} páginas)")
                        print(f"   📊 Total únicos coletados: {len(all_num_auto):,}")
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
            # Limpa apenas dados desta sessão
            session_uuid = st.session_state.get('session_uuid', '')
            
            keys_to_remove = []
            for key in st.session_state.keys():
                if key.startswith(f'paginated_data_data_{session_uuid}'):
                    keys_to_remove.append(key)
            
            for key in keys_to_remove:
                del st.session_state[key]
            
            # Gera novo UUID de sessão para forçar novo cache
            st.session_state.session_uuid = str(uuid.uuid4())[:8]
            
            print(f"🧹 Cache da sessão limpo ({len(keys_to_remove)} chaves removidas)")
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
