import streamlit as st
import pandas as pd
import plotly.express as px
import numpy as np

# Importa as funções de formatação
from src.utils.formatters import format_currency_brazilian, format_number_brazilian

class DataVisualization:
    def __init__(self, database=None):
        """Inicializa o componente de visualização com a conexão do banco de dados."""
        self.database = database

    def _get_filtered_data(self, selected_ufs: list, year_range: tuple) -> pd.DataFrame:
        """Obtém dados filtrados do Supabase."""
        try:
            # Busca TODOS os dados sem limite
            print("Buscando todos os dados do Supabase...")
            
            # Método 1: Busca tudo de uma vez (pode ser lento mas completo)
            try:
                result = self.database.supabase.table('ibama_infracao').select('*').execute()
                df = pd.DataFrame(result.data)
                print(f"Dados carregados: {len(df)} registros totais")
            except Exception as e:
                print(f"Erro ao buscar todos os dados: {e}")
                # Método 2: Busca com limite alto como fallback
                result = self.database.supabase.table('ibama_infracao').select('*').limit(100000).execute()
                df = pd.DataFrame(result.data)
                print(f"Dados carregados com limite: {len(df)} registros")
            
            if df.empty:
                return df
            
            # Aplica filtros APÓS carregar todos os dados
            original_count = len(df)
            
            # Filtro por UF
            if selected_ufs:
                df = df[df['UF'].isin(selected_ufs)]
                print(f"Após filtro UF: {len(df)} registros (era {original_count})")
            
            # Filtro por ano
            if year_range and 'DAT_HORA_AUTO_INFRACAO' in df.columns:
                try:
                    df['DAT_HORA_AUTO_INFRACAO'] = pd.to_datetime(df['DAT_HORA_AUTO_INFRACAO'], errors='coerce')
                    
                    # Conta registros por ano para debug
                    if not df.empty:
                        year_counts = df['DAT_HORA_AUTO_INFRACAO'].dt.year.value_counts()
                        print(f"Registros por ano: {dict(year_counts.head(10))}")
                    
                    df = df[
                        (df['DAT_HORA_AUTO_INFRACAO'].dt.year >= year_range[0]) &
                        (df['DAT_HORA_AUTO_INFRACAO'].dt.year <= year_range[1])
                    ]
                    print(f"Após filtro de ano ({year_range}): {len(df)} registros")
                except Exception as e:
                    print(f"Erro no filtro de ano: {e}")
                    pass  # Se não conseguir filtrar por data, continua sem filtro
            
            print(f"Dados finais retornados: {len(df)} registros")
            return df
            
        except Exception as e:
            st.error(f"Erro ao obter dados: {e}")
            return pd.DataFrame()

    def create_overview_metrics(self, selected_ufs: list, year_range: tuple):
        """Cria as métricas de visão geral usando dados do Supabase."""
        if not self.database or not self.database.supabase:
            st.warning("Banco de dados não disponível.")
            return

        try:
            df = self._get_filtered_data(selected_ufs, year_range)
            
            if df.empty:
                st.warning("Nenhum dado encontrado para os filtros selecionados.")
                return

            # Calcula métricas
            total_infracoes = len(df)
            
            # Valor total das multas
            try:
                df['VAL_AUTO_INFRACAO_NUMERIC'] = pd.to_numeric(
                    df['VAL_AUTO_INFRACAO'].astype(str).str.replace(',', '.'), 
                    errors='coerce'
                )
                valor_total_multas = df['VAL_AUTO_INFRACAO_NUMERIC'].sum()
                if np.isnan(valor_total_multas):
                    valor_total_multas = 0
            except:
                valor_total_multas = 0
            
            # Total de municípios
            total_municipios = df['MUNICIPIO'].nunique() if 'MUNICIPIO' in df.columns else 0

            # Exibe métricas
            col1, col2, col3 = st.columns(3)
            col1.metric("Total de Infrações", format_number_brazilian(total_infracoes))
            col2.metric("Valor Total das Multas", format_currency_brazilian(valor_total_multas))
            col3.metric("Municípios Afetados", format_number_brazilian(total_municipios))

        except Exception as e:
            st.error(f"Erro ao calcular métricas: {e}")

    def create_state_distribution_chart(self, selected_ufs: list, year_range: tuple):
        """Cria gráfico de distribuição por estado."""
        try:
            df = self._get_filtered_data(selected_ufs, year_range)
            
            if df.empty or 'UF' not in df.columns:
                st.warning("Dados de UF não disponíveis.")
                return
            
            # Agrupa por UF
            uf_counts = df['UF'].value_counts().head(15)
            
            if not uf_counts.empty:
                chart_df = pd.DataFrame({
                    'UF': uf_counts.index,
                    'total': uf_counts.values
                })
                
                fig = px.bar(
                    chart_df, 
                    x='UF', 
                    y='total', 
                    title="<b>Distribuição de Infrações por Estado</b>", 
                    color='total',
                    labels={'UF': 'Estado', 'total': 'Nº de Infrações'}
                )
                st.plotly_chart(fig, use_container_width=True)
                
        except Exception as e:
            st.error(f"Erro no gráfico de estados: {e}")

    def create_municipality_hotspots_chart(self, selected_ufs: list, year_range: tuple):
        """Cria gráfico dos municípios com mais infrações."""
        try:
            df = self._get_filtered_data(selected_ufs, year_range)
            
            if df.empty or 'MUNICIPIO' not in df.columns:
                st.warning("Dados de município não disponíveis.")
                return
            
            # Remove valores vazios
            df_clean = df[df['MUNICIPIO'].notna() & (df['MUNICIPIO'] != '')]
            
            if df_clean.empty:
                return
            
            # Top 10 municípios
            muni_counts = df_clean.groupby(['MUNICIPIO', 'UF']).size().reset_index(name='total')
            muni_counts = muni_counts.nlargest(10, 'total')
            
            if not muni_counts.empty:
                muni_counts['local'] = muni_counts['MUNICIPIO'].str.title() + ' (' + muni_counts['UF'] + ')'
                
                fig = px.bar(
                    muni_counts.sort_values('total'), 
                    y='local', 
                    x='total', 
                    orientation='h',
                    title="<b>Top 10 Municípios com Mais Infrações</b>",
                    labels={'local': 'Município', 'total': 'Nº de Infrações'},
                    text='total'
                )
                st.plotly_chart(fig, use_container_width=True)
                
        except Exception as e:
            st.error(f"Erro no gráfico de municípios: {e}")

    def create_fine_value_by_type_chart(self, selected_ufs: list, year_range: tuple):
        """Cria gráfico de valores de multa por tipo."""
        try:
            df = self._get_filtered_data(selected_ufs, year_range)
            
            if df.empty or 'TIPO_INFRACAO' not in df.columns:
                return
            
            # Converte valores
            df['VAL_AUTO_INFRACAO_NUMERIC'] = pd.to_numeric(
                df['VAL_AUTO_INFRACAO'].astype(str).str.replace(',', '.'), 
                errors='coerce'
            )
            
            # Remove valores inválidos
            df_clean = df[
                df['VAL_AUTO_INFRACAO_NUMERIC'].notna() & 
                df['TIPO_INFRACAO'].notna() & 
                (df['TIPO_INFRACAO'] != '')
            ]
            
            if df_clean.empty:
                return
            
            # Agrupa por tipo
            type_values = df_clean.groupby('TIPO_INFRACAO')['VAL_AUTO_INFRACAO_NUMERIC'].sum().nlargest(10)
            
            if not type_values.empty:
                chart_df = pd.DataFrame({
                    'TIPO_INFRACAO': type_values.index,
                    'valor_total': type_values.values
                })
                
                chart_df['TIPO_INFRACAO'] = chart_df['TIPO_INFRACAO'].str.title()
                
                fig = px.bar(
                    chart_df.sort_values('valor_total'), 
                    y='TIPO_INFRACAO', 
                    x='valor_total', 
                    orientation='h',
                    title="<b>Tipos de Infração por Valor de Multa (Top 10)</b>"
                )
                st.plotly_chart(fig, use_container_width=True)
                
        except Exception as e:
            st.error(f"Erro no gráfico de tipos: {e}")

    def create_gravity_distribution_chart(self, selected_ufs: list, year_range: tuple):
        """Cria gráfico de distribuição por gravidade."""
        try:
            df = self._get_filtered_data(selected_ufs, year_range)
            
            if df.empty or 'GRAVIDADE_INFRACAO' not in df.columns:
                return
            
            # Remove valores vazios
            df_clean = df[df['GRAVIDADE_INFRACAO'].notna() & (df['GRAVIDADE_INFRACAO'] != '')]
            
            if df_clean.empty:
                return
            
            gravity_counts = df_clean['GRAVIDADE_INFRACAO'].value_counts()
            
            if not gravity_counts.empty:
                fig = px.pie(
                    values=gravity_counts.values,
                    names=gravity_counts.index,
                    title="<b>Distribuição por Gravidade da Infração</b>", 
                    hole=0.4
                )
                st.plotly_chart(fig, use_container_width=True)
                
        except Exception as e:
            st.error(f"Erro no gráfico de gravidade: {e}")

    def create_main_offenders_chart(self, selected_ufs: list, year_range: tuple):
        """Cria gráfico dos principais infratores."""
        try:
            df = self._get_filtered_data(selected_ufs, year_range)
            
            if df.empty or 'NOME_INFRATOR' not in df.columns:
                return
            
            # Converte valores
            df['VAL_AUTO_INFRACAO_NUMERIC'] = pd.to_numeric(
                df['VAL_AUTO_INFRACAO'].astype(str).str.replace(',', '.'), 
                errors='coerce'
            )
            
            # Remove valores inválidos
            df_clean = df[
                df['VAL_AUTO_INFRACAO_NUMERIC'].notna() & 
                df['NOME_INFRATOR'].notna() & 
                (df['NOME_INFRATOR'] != '')
            ]
            
            if df_clean.empty:
                return
            
            # Top 10 infratores
            offender_values = df_clean.groupby('NOME_INFRATOR')['VAL_AUTO_INFRACAO_NUMERIC'].sum().nlargest(10)
            
            if not offender_values.empty:
                chart_df = pd.DataFrame({
                    'NOME_INFRATOR': offender_values.index,
                    'valor_total': offender_values.values
                })
                
                chart_df['NOME_INFRATOR'] = chart_df['NOME_INFRATOR'].str.title().str.slice(0, 40)
                
                fig = px.bar(
                    chart_df.sort_values('valor_total'), 
                    y='NOME_INFRATOR', 
                    x='valor_total', 
                    orientation='h',
                    title="<b>Top 10 Infratores por Valor de Multa</b>"
                )
                st.plotly_chart(fig, use_container_width=True)
                
        except Exception as e:
            st.error(f"Erro no gráfico de infratores: {e}")

    def create_infraction_map(self, selected_ufs: list, year_range: tuple):
        """Cria mapa de calor das infrações."""
        st.subheader("Mapa de Calor de Infrações")
        
        try:
            df = self._get_filtered_data(selected_ufs, year_range)
            
            if df.empty:
                st.warning("Nenhum dado encontrado.")
                return
            
            # Filtra dados com coordenadas
            required_cols = ['NUM_LATITUDE_AUTO', 'NUM_LONGITUDE_AUTO']
            if not all(col in df.columns for col in required_cols):
                st.warning("Dados de geolocalização não disponíveis.")
                return
            
            with st.spinner("Carregando dados do mapa..."):
                # Remove valores vazios e converte coordenadas
                df_map = df[
                    df['NUM_LATITUDE_AUTO'].notna() & 
                    df['NUM_LONGITUDE_AUTO'].notna() &
                    (df['NUM_LATITUDE_AUTO'] != '') &
                    (df['NUM_LONGITUDE_AUTO'] != '')
                ].copy()
                
                if df_map.empty:
                    st.warning("Nenhuma coordenada válida encontrada.")
                    return
                
                # Limita para performance
                if len(df_map) > 5000:
                    df_map = df_map.sample(n=5000)
                
                # Converte coordenadas
                df_map['lat'] = pd.to_numeric(df_map['NUM_LATITUDE_AUTO'].astype(str).str.replace(',', '.'), errors='coerce')
                df_map['lon'] = pd.to_numeric(df_map['NUM_LONGITUDE_AUTO'].astype(str).str.replace(',', '.'), errors='coerce')
                
                # Remove coordenadas inválidas
                df_map = df_map.dropna(subset=['lat', 'lon'])
                
                if not df_map.empty:
                    st.map(df_map[['lat', 'lon']], zoom=3)
                else:
                    st.warning("Nenhuma coordenada válida após conversão.")
                    
        except Exception as e:
            st.error(f"Erro no mapa: {e}")

    def create_infraction_status_chart(self, selected_ufs: list, year_range: tuple):
        """Cria gráfico do status das infrações."""
        try:
            df = self._get_filtered_data(selected_ufs, year_range)
            
            if df.empty or 'DES_STATUS_FORMULARIO' not in df.columns:
                return
            
            # Remove valores vazios
            df_clean = df[df['DES_STATUS_FORMULARIO'].notna() & (df['DES_STATUS_FORMULARIO'] != '')]
            
            if df_clean.empty:
                return
            
            status_counts = df_clean['DES_STATUS_FORMULARIO'].value_counts().head(10)
            
            if not status_counts.empty:
                chart_df = pd.DataFrame({
                    'DES_STATUS_FORMULARIO': status_counts.index,
                    'total': status_counts.values
                })
                
                chart_df['DES_STATUS_FORMULARIO'] = chart_df['DES_STATUS_FORMULARIO'].str.title()
                
                fig = px.bar(
                    chart_df.sort_values('total'), 
                    y='DES_STATUS_FORMULARIO', 
                    x='total', 
                    orientation='h',
                    title="<b>Estágio Atual das Infrações (Top 10)</b>", 
                    text='total'
                )
                st.plotly_chart(fig, use_container_width=True)
                
        except Exception as e:
            st.error(f"Erro no gráfico de status: {e}")
