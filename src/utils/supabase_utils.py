# Diagnóstico Profundo e Correção Definitiva - supabase_utils.py

import pandas as pd
import streamlit as st
from typing import List, Dict, Any, Optional
import hashlib
import time
import random
import uuid

class SupabasePaginator:
    """Classe CORRIGIDA para buscar todos os dados do Supabase com análise profunda de duplicatas."""
    
    def __init__(self, supabase_client):
        self.supabase = supabase_client
        self.page_size = 1000  # Tamanho da página (limite do Supabase)
        self.max_pages = 35    # AUMENTADO: permite até 35k registros para garantir
    
    def _get_session_key(self, table_name: str = 'ibama_infracao', filters: str = "") -> str:
        """Gera chave única POR SESSÃO para cache isolado."""
        if 'session_uuid' not in st.session_state:
            st.session_state.session_uuid = str(uuid.uuid4())[:8]
        
        session_id = st.session_state.session_uuid
        filter_hash = hashlib.md5(f"{table_name}_{filters}_{session_id}".encode()).hexdigest()[:8]
        return f"data_{session_id}_{filter_hash}"
    
    def deep_analysis_duplicates(self, table_name: str = 'ibama_infracao') -> Dict[str, Any]:
        """
        NOVA FUNÇÃO: Análise profunda de duplicatas no banco.
        Identifica exatamente onde estão as duplicatas e por quê.
        """
        try:
            print("🔍 ANÁLISE PROFUNDA: Investigando duplicatas no banco...")
            
            analysis_result = {
                'total_records': 0,
                'unique_num_auto': 0,
                'duplicated_num_auto': 0,
                'most_duplicated': [],
                'null_num_auto': 0,
                'empty_num_auto': 0,
                'problematic_records': [],
                'sample_duplicates': []
            }
            
            all_num_auto = []
            all_records = []
            page = 0
            
            print("📊 Coletando TODOS os NUM_AUTO_INFRACAO para análise...")
            
            while True:
                start = page * self.page_size
                end = start + self.page_size - 1
                
                print(f"   📄 Analisando página {page + 1}: registros {start} a {end}")
                
                try:
                    # Busca apenas campos críticos para análise
                    result = self.supabase.table(table_name).select(
                        'NUM_AUTO_INFRACAO, DAT_HORA_AUTO_INFRACAO, UF, MUNICIPIO'
                    ).range(start, end).execute()
                    
                    if not result.data or len(result.data) == 0:
                        break
                    
                    for record in result.data:
                        all_records.append(record)
                        num_auto = record.get('NUM_AUTO_INFRACAO')
                        all_num_auto.append(num_auto)
                    
                    print(f"      ✅ Coletados {len(result.data)} registros (total: {len(all_records)})")
                    
                    if len(result.data) < self.page_size:
                        break
                    
                    page += 1
                    
                    if page >= self.max_pages:
                        print(f"   ⚠️ Limite de páginas atingido: {self.max_pages}")
                        break
                        
                except Exception as e:
                    print(f"   ❌ Erro na página {page + 1}: {e}")
                    break
            
            print(f"🎯 ANÁLISE COMPLETA: {len(all_records)} registros coletados")
            
            # Análise dos dados coletados
            df_analysis = pd.DataFrame(all_records)
            analysis_result['total_records'] = len(df_analysis)
            
            # Análise de NUM_AUTO_INFRACAO
            if 'NUM_AUTO_INFRACAO' in df_analysis.columns:
                # Conta nulos e vazios
                analysis_result['null_num_auto'] = df_analysis['NUM_AUTO_INFRACAO'].isna().sum()
                analysis_result['empty_num_auto'] = (df_analysis['NUM_AUTO_INFRACAO'] == '').sum()
                
                # Remove nulos e vazios para análise
                df_valid = df_analysis[
                    df_analysis['NUM_AUTO_INFRACAO'].notna() & 
                    (df_analysis['NUM_AUTO_INFRACAO'] != '')
                ].copy()
                
                if not df_valid.empty:
                    # Conta únicos
                    analysis_result['unique_num_auto'] = df_valid['NUM_AUTO_INFRACAO'].nunique()
                    
                    # Identifica duplicatas
                    duplicates = df_valid['NUM_AUTO_INFRACAO'].value_counts()
                    duplicated_nums = duplicates[duplicates > 1]
                    analysis_result['duplicated_num_auto'] = len(duplicated_nums)
                    
                    # Top 10 mais duplicados
                    if not duplicated_nums.empty:
                        analysis_result['most_duplicated'] = duplicated_nums.head(10).to_dict()
                        
                        # Amostras de registros duplicados
                        for num_auto, count in duplicated_nums.head(5).items():
                            sample_records = df_valid[df_valid['NUM_AUTO_INFRACAO'] == num_auto]
                            analysis_result['sample_duplicates'].append({
                                'num_auto': num_auto,
                                'count': count,
                                'sample_data': sample_records.head(3).to_dict('records')
                            })
                    
                    print(f"📈 RESULTADOS DA ANÁLISE:")
                    print(f"   📊 Total de registros: {analysis_result['total_records']:,}")
                    print(f"   🔢 NUM_AUTO_INFRACAO únicos: {analysis_result['unique_num_auto']:,}")
                    print(f"   ❌ Nulos: {analysis_result['null_num_auto']:,}")
                    print(f"   ❌ Vazios: {analysis_result['empty_num_auto']:,}")
                    print(f"   🔄 NUM_AUTO_INFRACAO duplicados: {analysis_result['duplicated_num_auto']:,}")
                    
                    if analysis_result['most_duplicated']:
                        print(f"   🔴 Exemplo de duplicata mais comum:")
                        most_common = list(analysis_result['most_duplicated'].items())[0]
                        print(f"      NUM_AUTO_INFRACAO: {most_common[0]} aparece {most_common[1]} vezes")
            
            return analysis_result
            
        except Exception as e:
            print(f"❌ Erro na análise profunda: {e}")
            return {"error": f"Erro na análise: {str(e)}"}
    
    def get_real_count_fixed(self, table_name: str = 'ibama_infracao') -> Dict[str, Any]:
        """
        VERSÃO CORRIGIDA: Obtém contagens reais com análise profunda.
        """
        try:
            print("🔍 CONTAGEM REAL CORRIGIDA: Iniciando análise completa...")
            
            # Executa análise profunda
            deep_analysis = self.deep_analysis_duplicates(table_name)
            
            if 'error' in deep_analysis:
                return deep_analysis
            
            # Monta resultado final
            result = {
                'total_records': deep_analysis['total_records'],
                'unique_infractions': deep_analysis['unique_num_auto'],
                'duplicates': deep_analysis['total_records'] - deep_analysis['unique_num_auto'],
                'null_records': deep_analysis['null_num_auto'],
                'empty_records': deep_analysis['empty_num_auto'],
                'duplicated_infractions': deep_analysis['duplicated_num_auto'],
                'most_duplicated_examples': deep_analysis['most_duplicated'],
                'timestamp': pd.Timestamp.now(),
                'analysis_complete': True
            }
            
            print(f"✅ CONTAGEM REAL FINALIZADA:")
            print(f"   📊 Total: {result['total_records']:,}")
            print(f"   🔢 Únicos: {result['unique_infractions']:,}")
            print(f"   📉 Duplicatas: {result['duplicates']:,}")
            print(f"   ❌ Nulos/Vazios: {result['null_records']:,}/{result['empty_records']:,}")
            
            return result
            
        except Exception as e:
            print(f"❌ Erro na contagem real corrigida: {e}")
            return {
                'total_records': 0,
                'unique_infractions': 0,
                'duplicates': 0,
                'timestamp': pd.Timestamp.now(),
                'error': str(e)
            }
    
    def get_all_records_fixed(self, table_name: str = 'ibama_infracao', cache_key: str = None) -> pd.DataFrame:
        """
        VERSÃO CORRIGIDA: Busca TODOS os registros únicos com verificação rigorosa.
        """
        if cache_key is None:
            cache_key = self._get_session_key(table_name)
        
        cache_storage_key = f"paginated_data_{cache_key}"
        if cache_storage_key in st.session_state:
            print(f"✅ Retornando dados do cache da sessão: {cache_storage_key}")
            return st.session_state[cache_storage_key]
        
        print(f"🔄 BUSCA CORRIGIDA: Iniciando paginação completa com verificação rigorosa...")
        
        all_data = []
        seen_infractions = set()
        page = 0
        
        while True:
            start = page * self.page_size
            end = start + self.page_size - 1
            
            print(f"   📄 Página {page + 1}: registros {start} a {end}")
            
            try:
                result = self.supabase.table(table_name).select('*').range(start, end).execute()
                
                if not result.data or len(result.data) == 0:
                    print(f"   ✅ Fim da paginação na página {page + 1}")
                    break
                
                # VERIFICAÇÃO RIGOROSA: Filtra registros únicos
                unique_records = []
                duplicates_found = 0
                
                for record in result.data:
                    num_auto = record.get('NUM_AUTO_INFRACAO')
                    
                    # Só aceita registros com NUM_AUTO_INFRACAO válido
                    if num_auto and str(num_auto).strip() != '':
                        if num_auto not in seen_infractions:
                            seen_infractions.add(num_auto)
                            unique_records.append(record)
                        else:
                            duplicates_found += 1
                
                all_data.extend(unique_records)
                
                if duplicates_found > 0:
                    print(f"      ⚠️ {duplicates_found} duplicatas encontradas e ignoradas nesta página")
                
                print(f"   📊 Únicos desta página: {len(unique_records)} (acumulado: {len(all_data):,})")
                
                if len(result.data) < self.page_size:
                    print(f"   ✅ Última página alcançada")
                    break
                
                page += 1
                
                if page >= self.max_pages:
                    print(f"   ⚠️ Limite máximo de páginas atingido: {self.max_pages}")
                    break
                
            except Exception as e:
                print(f"   ❌ Erro na página {page + 1}: {e}")
                break
        
        print(f"🎉 PAGINAÇÃO COMPLETA:")
        print(f"   📊 Total de registros únicos coletados: {len(all_data):,}")
        print(f"   🔢 NUM_AUTO_INFRACAO únicos: {len(seen_infractions):,}")
        
        df = pd.DataFrame(all_data)
        
        # VALIDAÇÃO FINAL RIGOROSA
        if not df.empty and 'NUM_AUTO_INFRACAO' in df.columns:
            original_count = len(df)
            df_final = df.drop_duplicates(subset=['NUM_AUTO_INFRACAO'], keep='first')
            final_count = len(df_final)
            
            if original_count != final_count:
                print(f"🚨 AVISO CRÍTICO: {original_count - final_count} duplicatas extras removidas na validação final")
                df = df_final
            else:
                print(f"✅ VALIDAÇÃO FINAL: Confirmados {final_count:,} registros únicos")
        
        # Armazena no cache da sessão
        st.session_state[cache_storage_key] = df
        print(f"💾 Dados armazenados no cache da sessão: {cache_storage_key}")
        
        return df
    
    def diagnostic_database_structure(self) -> Dict[str, Any]:
        """
        NOVA FUNÇÃO: Diagnostica a estrutura do banco para identificar problemas.
        """
        try:
            print("🔍 DIAGNÓSTICO DE ESTRUTURA: Analisando banco de dados...")
            
            # Busca uma amostra para análise de estrutura
            result = self.supabase.table('ibama_infracao').select('*').limit(100).execute()
            
            if not result.data:
                return {"error": "Nenhum dado encontrado"}
            
            df_sample = pd.DataFrame(result.data)
            
            structure_info = {
                'total_columns': len(df_sample.columns),
                'columns_list': list(df_sample.columns),
                'num_auto_exists': 'NUM_AUTO_INFRACAO' in df_sample.columns,
                'sample_size': len(df_sample),
                'data_types': df_sample.dtypes.to_dict(),
                'null_counts': df_sample.isnull().sum().to_dict(),
                'sample_num_auto': []
            }
            
            # Analisa NUM_AUTO_INFRACAO especificamente
            if structure_info['num_auto_exists']:
                sample_nums = df_sample['NUM_AUTO_INFRACAO'].dropna().head(10).tolist()
                structure_info['sample_num_auto'] = sample_nums
                
                # Verifica formato dos NUM_AUTO_INFRACAO
                unique_formats = set()
                for num in sample_nums:
                    if num:
                        unique_formats.add(type(num).__name__)
                structure_info['num_auto_formats'] = list(unique_formats)
            
            print(f"📋 ESTRUTURA DO BANCO:")
            print(f"   📊 Total de colunas: {structure_info['total_columns']}")
            print(f"   🔢 NUM_AUTO_INFRACAO existe: {structure_info['num_auto_exists']}")
            print(f"   📝 Formatos NUM_AUTO_INFRACAO: {structure_info.get('num_auto_formats', [])}")
            
            return structure_info
            
        except Exception as e:
            print(f"❌ Erro no diagnóstico de estrutura: {e}")
            return {"error": f"Erro no diagnóstico: {str(e)}"}
    
    # Métodos existentes mantidos para compatibilidade
    def get_real_count(self, table_name: str = 'ibama_infracao') -> Dict[str, Any]:
        """Método original mantido - chama a versão corrigida."""
        return self.get_real_count_fixed(table_name)
    
    def get_all_records(self, table_name: str = 'ibama_infracao', cache_key: str = None) -> pd.DataFrame:
        """Método original mantido - chama a versão corrigida."""
        return self.get_all_records_fixed(table_name, cache_key)
    
    def clear_cache(self):
        """Limpa o cache específico desta sessão."""
        try:
            session_uuid = st.session_state.get('session_uuid', '')
            
            keys_to_remove = []
            for key in st.session_state.keys():
                if key.startswith(f'paginated_data_data_{session_uuid}'):
                    keys_to_remove.append(key)
            
            for key in keys_to_remove:
                del st.session_state[key]
            
            st.session_state.session_uuid = str(uuid.uuid4())[:8]
            
            print(f"🧹 Cache da sessão limpo ({len(keys_to_remove)} chaves removidas)")
            return True
        except Exception as e:
            print(f"❌ Erro ao limpar cache: {e}")
            return False
