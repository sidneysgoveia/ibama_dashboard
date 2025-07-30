import streamlit as st
import pandas as pd
import plotly.express as px
import numpy as np

# Importa as funções de formatação
from src.utils.formatters import format_currency_brazilian, format_number_brazilian

# Importa o paginador
try:
    from src.utils.supabase_utils import SupabasePaginator
except ImportError:
    # Fallback se o arquivo não existir
    class SupabasePaginator:
        def __init__(self, supabase_client):
            self.supabase = supabase_client
        
        def get_filtered_data(self, selected_ufs=None, year_range=None):
            # Método simples sem paginação como fallback
            result = self.supabase.table('ibama_infracao').select('*').limit(50000).execute()
            df = pd.DataFrame(result.data)
            
            if selected_ufs and not df.empty:
                df = df[df['UF'].isin(selected_ufs)]
            
            if year_range and 'DAT_HORA_AUTO_INFRACAO' in df.columns:
                try:
                    df['DAT_HORA_AUTO_INFRACAO'] = pd.to_datetime(df['DAT_HORA_AUTO_INFRACAO'], errors='coerce')
                    df = df[
                        (df['DAT_HORA_AUTO_INFRACAO'].dt.year >= year_range[0]) &
                        (df['DAT_HORA_AUTO_INFRACAO'].dt.year <= year_range[1])
                    ]
                except:
                    pass
            
            return df

class DataVisualization:
    def __init__(self, database=None):
        """Inicializa o componente de visualização com a conexão do banco de dados."""
        self.database = database
        
        # Inicializa o paginador se estiver usando Supabase
        if database and database.is_cloud and database.supabase:
            self.paginator = SupabasePaginator(database.supabase)
        else:
            self.paginator = None

    def _get_filtered_data(self, selected_ufs: list, year_range: tuple) -> pd.DataFrame:
        """Obtém dados filtrados usando paginação quando necessário (método legacy)."""
        
        if self.paginator:
            # Usa paginação para buscar todos os dados
            print("🔄 Usando paginação para buscar todos os dados...")
            return self.paginator.get_filtered_data(selected_ufs, year_range)
        else:
            # Fallback para método tradicional (DuckDB ou erro no Supabase)
            print("⚠️ Usando método tradicional (sem paginação)")
            try:
                if self.database.is_cloud:
                    # Tenta com limite alto
                    result = self.database.supabase.table('ibama_infracao').select('*').limit(50000).execute()
                    df = pd.DataFrame(result.data)
                else:
                    # DuckDB - usa query direta
                    df = self.database.execute_query("SELECT * FROM ibama_infracao")
                
                # Aplica filtros localmente
                if selected_ufs and not df.empty:
                    df = df[df['UF'].isin(selected_ufs)]
                
                if year_range and 'DAT_HORA_AUTO_INFRACAO' in df.columns:
                    try:
                        df['DAT_HORA_AUTO_INFRACAO'] = pd.to_datetime(df['DAT_HORA_AUTO_INFRACAO'], errors='coerce')
                        df = df[
                            (df['DAT_HORA_AUTO_INFRACAO'].dt.year >= year_range[0]) &
                            (df['DAT_HORA_AUTO_INFRACAO'].dt.year <= year_range[1])
                        ]
                    except:
                        pass
                
                return df
                
            except Exception as e:
                st.error(f"Erro ao obter dados: {e}")
                return pd.DataFrame()

    def _get_filtered_data_advanced(self, selected_ufs: list, date_filters: dict) -> pd.DataFrame:
        """Obtém dados filtrados usando os novos filtros avançados de data."""
        
        if self.paginator:
            # Usa paginação para buscar todos os dados
            print("🔄 Usando paginação para buscar todos os dados...")
            df = self.paginator.get_all_records()
        else:
            # Fallback para método tradicional (DuckDB ou erro no Supabase)
            print("⚠️ Usando método tradicional (sem paginação)")
            try:
                if self.database.is_cloud:
                    # Tenta com limite alto
                    result = self.database.supabase.table('ibama_infracao').select('*').limit(50000).execute()
                    df = pd.DataFrame(result.data)
                else:
                    # DuckDB - usa query direta
                    df = self.database.execute_query("SELECT * FROM ibama_infracao")
                
            except Exception as e:
                st.error(f"Erro ao obter dados: {e}")
                return pd.DataFrame()
        
        if df.empty:
            return df
        
        # Aplica filtro de UF
        if selected_ufs and 'UF' in df.columns:
            df = df[df['UF'].isin(selected_ufs)]
        
        # Aplica filtros de data avançados
        df = self._apply_date_filter_to_dataframe(df, date_filters)
        
        return df

    def _apply_date_filter_to_dataframe(self, df: pd.DataFrame, date_filters: dict) -> pd.DataFrame:
        """Aplica filtros de data ao DataFrame."""
        if df.empty or 'DAT_HORA_AUTO_INFRACAO' not in df.columns:
            return df
        
        try:
            # Converte coluna de data
            df['DATE_PARSED'] = pd.to_datetime(df['DAT_HORA_AUTO_INFRACAO'], errors='coerce')
            df_with_date = df[df['DATE_PARSED'].notna()].copy()
            
            if df_with_date.empty:
                return df_with_date
            
            if date_filters["mode"] == "simple":
                # Filtro simples por anos
                mask = df_with_date['DATE_PARSED'].dt.year.isin(date_filters["years"])
                return df_with_date[mask]
            
            else:
                # Filtro avançado por períodos
                masks = []
                for year, months in date_filters["periods"].items():
                    year_mask = df_with_date['DATE_PARSED'].dt.year == year
                    month_mask = df_with_date['DATE_PARSED'].dt.month.isin(months)
                    masks.append(year_mask & month_mask)
                
                if masks:
                    final_mask = masks[0]
                    for mask in masks[1:]:
                        final_mask = final_mask | mask
                    return df_with_date[final_mask]
                else:
                    return pd.DataFrame()
        
        except Exception as e:
            st.error(f"Erro ao aplicar filtro de data: {e}")
            return df

    # ======================== MÉTODOS AVANÇADOS ========================

    def create_overview_metrics_advanced(self, selected_ufs: list, date_filters: dict):
        """Cria as métricas de visão geral usando filtros avançados e contagem correta."""
        if not self.database:
            st.warning("Banco de dados não disponível.")
            return

        try:
            with st.spinner("Carregando dados completos..."):
                df = self._get_filtered_data_advanced(selected_ufs, date_filters)
            
            if df.empty:
                st.warning("Nenhum dado encontrado para os filtros selecionados.")
                return

            # Calcula métricas usando contagem de infrações únicas
            if 'NUM_AUTO_INFRACAO' in df.columns:
                total_infracoes = df['NUM_AUTO_INFRACAO'].nunique()
                metric_note = "infrações únicas"
            else:
                total_infracoes = len(df)
                metric_note = "registros (pode incluir duplicatas)"
            
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
            
            # Total de municípios - USA COD_MUNICIPIO para maior precisão
            if 'COD_MUNICIPIO' in df.columns:
                total_municipios = df['COD_MUNICIPIO'].nunique()
            elif 'MUNICIPIO' in df.columns:
                # Fallback para nome se código não estiver disponível
                total_municipios = df['MUNICIPIO'].nunique()
            else:
                total_municipios = 0

            # Exibe métricas
            col1, col2, col3 = st.columns(3)
            col1.metric("Total de Infrações", format_number_brazilian(total_infracoes))
            col2.metric("Valor Total das Multas", format_currency_brazilian(valor_total_multas))
            col3.metric("Municípios Afetados", format_number_brazilian(total_municipios))
            
            # Info de debug com descrição dos filtros
            st.caption(f"📊 Dados processados: {len(df):,} registros | {total_infracoes:,} {metric_note} | {date_filters['description']}")

        except Exception as e:
            st.error(f"Erro ao calcular métricas: {e}")

    def create_state_distribution_chart_advanced(self, selected_ufs: list, date_filters: dict):
        """Cria gráfico de distribuição por estado com contagem correta de infrações."""
        try:
            df = self._get_filtered_data_advanced(selected_ufs, date_filters)
            
            if df.empty or 'UF' not in df.columns:
                st.warning("Dados de UF não disponíveis.")
                return
            
            # Conta infrações únicas por UF se NUM_AUTO_INFRACAO disponível
            if 'NUM_AUTO_INFRACAO' in df.columns:
                uf_counts = df.groupby('UF')['NUM_AUTO_INFRACAO'].nunique().sort_values(ascending=False).head(15)
                method_note = "infrações únicas"
            else:
                # Fallback para contagem de registros
                uf_counts = df['UF'].value_counts().head(15)
                method_note = "registros (pode incluir duplicatas)"
            
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
                    labels={'UF': 'Estado', 'total': f'Nº de Infrações ({method_note})'}
                )
                
                # Adiciona nota sobre método
                fig.add_annotation(
                    text=f"* Contagem: {method_note}",
                    xref="paper", yref="paper",
                    x=1, y=1.02, xanchor='right', yanchor='bottom',
                    showarrow=False,
                    font=dict(size=10, color="gray")
                )
                
                st.plotly_chart(fig, use_container_width=True)
                
        except Exception as e:
            st.error(f"Erro no gráfico de estados: {e}")

    def create_municipality_hotspots_chart_advanced(self, selected_ufs: list, date_filters: dict):
        """Cria gráfico dos municípios com mais infrações usando contagem correta por NUM_AUTO_INFRACAO."""
        try:
            df = self._get_filtered_data_advanced(selected_ufs, date_filters)
            
            if df.empty:
                st.warning("Dados não disponíveis.")
                return
            
            # Verifica se temos os campos necessários
            required_fields = ['NUM_AUTO_INFRACAO', 'MUNICIPIO', 'UF']
            if not all(field in df.columns for field in required_fields):
                st.warning("Campos necessários para análise de municípios não encontrados.")
                return
            
            # Remove valores vazios nos campos necessários
            df_clean = df[
                df['NUM_AUTO_INFRACAO'].notna() & 
                df['MUNICIPIO'].notna() & 
                df['UF'].notna() &
                (df['NUM_AUTO_INFRACAO'] != '') & 
                (df['MUNICIPIO'] != '') & 
                (df['UF'] != '')
            ].copy()
            
            if df_clean.empty:
                st.warning("Dados válidos não disponíveis após limpeza.")
                return
            
            # Método preferido: usar código do município se disponível
            if 'COD_MUNICIPIO' in df.columns:
                # Remove códigos vazios
                df_clean = df_clean[
                    df_clean['COD_MUNICIPIO'].notna() & 
                    (df_clean['COD_MUNICIPIO'] != '')
                ]
                
                if df_clean.empty:
                    st.warning("Códigos de município não disponíveis.")
                    return
                
                # Conta INFRAÇÕES ÚNICAS por código do município
                muni_counts = df_clean.groupby(['COD_MUNICIPIO', 'MUNICIPIO', 'UF'])['NUM_AUTO_INFRACAO'].nunique().reset_index()
                muni_counts.rename(columns={'NUM_AUTO_INFRACAO': 'total_infracoes'}, inplace=True)
                muni_counts = muni_counts.nlargest(10, 'total_infracoes')
                
                method_note = "* Contagem por código IBGE + infrações únicas"
                
            else:
                # Fallback: usar nome do município
                st.caption("⚠️ Usando nomes de municípios (podem haver inconsistências)")
                
                # Conta INFRAÇÕES ÚNICAS por nome do município
                muni_counts = df_clean.groupby(['MUNICIPIO', 'UF'])['NUM_AUTO_INFRACAO'].nunique().reset_index()
                muni_counts.rename(columns={'NUM_AUTO_INFRACAO': 'total_infracoes'}, inplace=True)
                muni_counts = muni_counts.nlargest(10, 'total_infracoes')
                
                method_note = "* Contagem por nome + infrações únicas"
            
            if not muni_counts.empty:
                # Cria label combinado para exibição
                muni_counts['local'] = muni_counts['MUNICIPIO'].str.title() + ' (' + muni_counts['UF'] + ')'
                
                fig = px.bar(
                    muni_counts.sort_values('total_infracoes'), 
                    y='local', 
                    x='total_infracoes', 
                    orientation='h',
                    title="<b>Top 10 Municípios com Mais Infrações</b>",
                    labels={'local': 'Município', 'total_infracoes': 'Nº de Infrações Únicas'},
                    text='total_infracoes'
                )
                
                # Adiciona informação sobre o método usado
                fig.add_annotation(
                    text=method_note,
                    xref="paper", yref="paper",
                    x=1, y=-0.1, xanchor='right', yanchor='top',
                    showarrow=False,
                    font=dict(size=10, color="gray")
                )
                
                st.plotly_chart(fig, use_container_width=True)
                
        except Exception as e:
            st.error(f"Erro no gráfico de municípios: {e}")

    def create_fine_value_by_type_chart_advanced(self, selected_ufs: list, date_filters: dict):
        """Cria gráfico de valores de multa por tipo com filtros avançados."""
        try:
            df = self._get_filtered_data_advanced(selected_ufs, date_filters)
            
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

    def create_gravity_distribution_chart_advanced(self, selected_ufs: list, date_filters: dict):
        """Cria gráfico de distribuição por gravidade incluindo infrações sem avaliação (Baixa, Média, Sem avaliação feita)."""
        try:
            df = self._get_filtered_data_advanced(selected_ufs, date_filters)
            
            if df.empty or 'GRAVIDADE_INFRACAO' not in df.columns:
                return
            
            # Prepara DataFrame tratando valores vazios/nulos como "Sem avaliação feita"
            df_processed = df.copy()
            df_processed['GRAVIDADE_INFRACAO'] = df_processed['GRAVIDADE_INFRACAO'].fillna('Sem avaliação feita')
            df_processed['GRAVIDADE_INFRACAO'] = df_processed['GRAVIDADE_INFRACAO'].replace('', 'Sem avaliação feita')
            
            # Conta infrações únicas por gravidade se NUM_AUTO_INFRACAO disponível
            if 'NUM_AUTO_INFRACAO' in df_processed.columns:
                gravity_counts = df_processed.groupby('GRAVIDADE_INFRACAO')['NUM_AUTO_INFRACAO'].nunique()
                method_note = "infrações únicas"
            else:
                gravity_counts = df_processed['GRAVIDADE_INFRACAO'].value_counts()
                method_note = "registros"
            
            if not gravity_counts.empty:
                # Define cores específicas para as categorias
                color_map = {
                    'Baixa': '#28a745',          # Verde
                    'Média': '#ffc107',          # Amarelo  
                    'Sem avaliação feita': '#6c757d'  # Cinza
                }
                
                # Cria lista de cores baseada nos dados (ordem: Baixa, Média, Sem avaliação feita)
                gravity_order = ['Baixa', 'Média', 'Sem avaliação feita']
                ordered_counts = []
                ordered_names = []
                ordered_colors = []
                
                for gravity in gravity_order:
                    if gravity in gravity_counts.index:
                        ordered_counts.append(gravity_counts[gravity])
                        ordered_names.append(gravity)
                        ordered_colors.append(color_map.get(gravity, '#17a2b8'))
                
                # Adiciona outras categorias que não estão na ordem padrão
                for gravity, count in gravity_counts.items():
                    if gravity not in gravity_order:
                        ordered_counts.append(count)
                        ordered_names.append(gravity)
                        ordered_colors.append('#17a2b8')  # Cor padrão
                
                fig = px.pie(
                    values=ordered_counts,
                    names=ordered_names,
                    title=f"<b>Distribuição por Gravidade da Infração ({method_note})</b>", 
                    hole=0.4,
                    color_discrete_sequence=ordered_colors
                )
                
                # Adiciona informação sobre dados sem avaliação se existirem
                sem_avaliacao = gravity_counts.get('Sem avaliação feita', 0)
                if sem_avaliacao > 0:
                    total_infracoes = gravity_counts.sum()
                    percentual_sem_avaliacao = (sem_avaliacao / total_infracoes) * 100
                    
                    fig.add_annotation(
                        text=f"* {sem_avaliacao:,} infrações ({percentual_sem_avaliacao:.1f}%) sem avaliação de gravidade",
                        xref="paper", yref="paper",
                        x=0.5, y=-0.1, xanchor='center', yanchor='top',
                        showarrow=False,
                        font=dict(size=10, color="gray")
                    )
                
                st.plotly_chart(fig, use_container_width=True)
                
        except Exception as e:
            st.error(f"Erro no gráfico de gravidade: {e}")

    def create_main_offenders_chart_advanced(self, selected_ufs: list, date_filters: dict):
        """Cria gráficos dos principais infratores separados por pessoas físicas (CPF) e empresas (CNPJ)."""
        try:
            df = self._get_filtered_data_advanced(selected_ufs, date_filters)
            
            if df.empty:
                return
            
            # Verifica se temos as colunas necessárias
            required_cols = ['NOME_INFRATOR', 'CPF_CNPJ_INFRATOR', 'VAL_AUTO_INFRACAO']
            if not all(col in df.columns for col in required_cols):
                st.warning("Colunas necessárias para análise de infratores não encontradas.")
                return
            
            # Remove valores inválidos
            df_clean = df[
                df['NOME_INFRATOR'].notna() & 
                df['CPF_CNPJ_INFRATOR'].notna() &
                df['VAL_AUTO_INFRACAO'].notna() &
                (df['NOME_INFRATOR'] != '') & 
                (df['CPF_CNPJ_INFRATOR'] != '') &
                (df['VAL_AUTO_INFRACAO'] != '')
            ].copy()
            
            if df_clean.empty:
                st.warning("Dados válidos não disponíveis para análise de infratores.")
                return
            
            # Converte valores para numérico
            df_clean['VAL_AUTO_INFRACAO_NUMERIC'] = pd.to_numeric(
                df_clean['VAL_AUTO_INFRACAO'].astype(str).str.replace(',', '.'), 
                errors='coerce'
            )
            
            # Remove valores que não conseguiram ser convertidos
            df_clean = df_clean[df_clean['VAL_AUTO_INFRACAO_NUMERIC'].notna()]
            
            if df_clean.empty:
                st.warning("Nenhum valor de multa válido encontrado.")
                return
            
            # Função para identificar CPF (formato: XXX.XXX.XXX-XX)
            def is_cpf(cpf_cnpj):
                if pd.isna(cpf_cnpj):
                    return False
                cpf_cnpj_str = str(cpf_cnpj).strip()
                # CPF tem 14 caracteres com pontos e hífen: XXX.XXX.XXX-XX
                if len(cpf_cnpj_str) == 14 and cpf_cnpj_str.count('.') == 2 and cpf_cnpj_str.count('-') == 1:
                    return True
                return False
            
            # Função para identificar CNPJ (formato: XX.XXX.XXX/XXXX-XX)
            def is_cnpj(cpf_cnpj):
                if pd.isna(cpf_cnpj):
                    return False
                cpf_cnpj_str = str(cpf_cnpj).strip()
                # CNPJ tem 18 caracteres com pontos, barra e hífen: XX.XXX.XXX/XXXX-XX
                if len(cpf_cnpj_str) == 18 and cpf_cnpj_str.count('.') == 2 and cpf_cnpj_str.count('/') == 1 and cpf_cnpj_str.count('-') == 1:
                    return True
                return False
            
            # Separa pessoas físicas (CPF) e empresas (CNPJ)
            df_clean['is_cpf'] = df_clean['CPF_CNPJ_INFRATOR'].apply(is_cpf)
            df_clean['is_cnpj'] = df_clean['CPF_CNPJ_INFRATOR'].apply(is_cnpj)
            
            df_pessoas_fisicas = df_clean[df_clean['is_cpf']]
            df_empresas = df_clean[df_clean['is_cnpj']]
            
            # Cria duas colunas para os gráficos
            col1, col2 = st.columns(2)
            
            # Gráfico 1: Top 10 Pessoas Físicas (CPF)
            with col1:
                if not df_pessoas_fisicas.empty:
                    # Agrupa por NOME_INFRATOR e CPF_CNPJ_INFRATOR, soma os valores
                    pf_grouped = df_pessoas_fisicas.groupby(['NOME_INFRATOR', 'CPF_CNPJ_INFRATOR'])['VAL_AUTO_INFRACAO_NUMERIC'].sum().reset_index()
                    pf_grouped = pf_grouped.nlargest(10, 'VAL_AUTO_INFRACAO_NUMERIC')
                    
                    if not pf_grouped.empty:
                        # Cria rótulo combinado (nome + CPF mascarado)
                        pf_grouped['label'] = pf_grouped.apply(
                            lambda x: f"{x['NOME_INFRATOR'][:30]}{'...' if len(x['NOME_INFRATOR']) > 30 else ''}\n(CPF: {x['CPF_CNPJ_INFRATOR'][:3]}.***.***-{x['CPF_CNPJ_INFRATOR'][-2:]})", 
                            axis=1
                        )
                        
                        fig_pf = px.bar(
                            pf_grouped.sort_values('VAL_AUTO_INFRACAO_NUMERIC'), 
                            y='label', 
                            x='VAL_AUTO_INFRACAO_NUMERIC', 
                            orientation='h',
                            title="<b>Top 10 Pessoas Físicas por Valor de Multa</b>",
                            labels={'label': 'Pessoa Física', 'VAL_AUTO_INFRACAO_NUMERIC': 'Valor Total (R$)'},
                            text='VAL_AUTO_INFRACAO_NUMERIC'
                        )
                        
                        # Formata os valores no eixo X como moeda
                        fig_pf.update_layout(
                            xaxis_tickformat=',.0f',
                            height=500,
                            margin=dict(l=200)  # Mais espaço à esquerda para os nomes
                        )
                        
                        # Formata os textos dos valores
                        fig_pf.update_traces(
                            texttemplate='R$ %{x:,.0f}',
                            textposition='outside'
                        )
                        
                        st.plotly_chart(fig_pf, use_container_width=True)
                        
                        # Mostra estatísticas
                        total_pf = pf_grouped['VAL_AUTO_INFRACAO_NUMERIC'].sum()
                        st.caption(f"💰 Total: R$ {total_pf:,.2f} | 👥 {len(pf_grouped)} pessoas físicas")
                    else:
                        st.info("Nenhuma pessoa física encontrada nos dados filtrados.")
                else:
                    st.info("Nenhuma pessoa física encontrada nos dados filtrados.")
            
            # Gráfico 2: Top 10 Empresas (CNPJ)
            with col2:
                if not df_empresas.empty:
                    # Agrupa por NOME_INFRATOR e CPF_CNPJ_INFRATOR, soma os valores
                    empresa_grouped = df_empresas.groupby(['NOME_INFRATOR', 'CPF_CNPJ_INFRATOR'])['VAL_AUTO_INFRACAO_NUMERIC'].sum().reset_index()
                    empresa_grouped = empresa_grouped.nlargest(10, 'VAL_AUTO_INFRACAO_NUMERIC')
                    
                    if not empresa_grouped.empty:
                        # Cria rótulo combinado (nome + CNPJ mascarado)
                        empresa_grouped['label'] = empresa_grouped.apply(
                            lambda x: f"{x['NOME_INFRATOR'][:30]}{'...' if len(x['NOME_INFRATOR']) > 30 else ''}\n(CNPJ: {x['CPF_CNPJ_INFRATOR'][:2]}.***.***/****-{x['CPF_CNPJ_INFRATOR'][-2:]})", 
                            axis=1
                        )
                        
                        fig_empresa = px.bar(
                            empresa_grouped.sort_values('VAL_AUTO_INFRACAO_NUMERIC'), 
                            y='label', 
                            x='VAL_AUTO_INFRACAO_NUMERIC', 
                            orientation='h',
                            title="<b>Top 10 Empresas por Valor de Multa</b>",
                            labels={'label': 'Empresa', 'VAL_AUTO_INFRACAO_NUMERIC': 'Valor Total (R$)'},
                            text='VAL_AUTO_INFRACAO_NUMERIC',
                            color_discrete_sequence=['#ff6b6b']  # Cor diferente para empresas
                        )
                        
                        # Formata os valores no eixo X como moeda
                        fig_empresa.update_layout(
                            xaxis_tickformat=',.0f',
                            height=500,
                            margin=dict(l=200)  # Mais espaço à esquerda para os nomes
                        )
                        
                        # Formata os textos dos valores
                        fig_empresa.update_traces(
                            texttemplate='R$ %{x:,.0f}',
                            textposition='outside'
                        )
                        
                        st.plotly_chart(fig_empresa, use_container_width=True)
                        
                        # Mostra estatísticas
                        total_empresa = empresa_grouped['VAL_AUTO_INFRACAO_NUMERIC'].sum()
                        st.caption(f"💰 Total: R$ {total_empresa:,.2f} | 🏢 {len(empresa_grouped)} empresas")
                    else:
                        st.info("Nenhuma empresa encontrada nos dados filtrados.")
                else:
                    st.info("Nenhuma empresa encontrada nos dados filtrados.")
            
            # Estatísticas gerais
            total_identificados = len(df_pessoas_fisicas) + len(df_empresas)
            total_nao_identificados = len(df_clean) - total_identificados
            
            if total_nao_identificados > 0:
                st.info(f"📊 **Resumo:** {len(df_pessoas_fisicas)} pessoas físicas, {len(df_empresas)} empresas, {total_nao_identificados} registros com formato de CPF/CNPJ não identificado")
            else:
                st.info(f"📊 **Resumo:** {len(df_pessoas_fisicas)} pessoas físicas, {len(df_empresas)} empresas identificadas")
                
        except Exception as e:
            st.error(f"Erro no gráfico de infratores: {e}")

    def create_infraction_map_advanced(self, selected_ufs: list, date_filters: dict):
        """Cria mapa de calor das infrações com filtros avançados."""
        st.subheader("Mapa de Calor de Infrações")
        
        try:
            df = self._get_filtered_data_advanced(selected_ufs, date_filters)
            
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
                    st.caption(f"📍 Exibindo {len(df_map):,} pontos de {len(df):,} infrações | {date_filters['description']}")
                else:
                    st.warning("Nenhuma coordenada válida após conversão.")
                    
        except Exception as e:
            st.error(f"Erro no mapa: {e}")

    def create_infraction_status_chart_advanced(self, selected_ufs: list, date_filters: dict):
        """Cria gráfico do status das infrações com contagem correta."""
        try:
            df = self._get_filtered_data_advanced(selected_ufs, date_filters)
            
            if df.empty or 'DES_STATUS_FORMULARIO' not in df.columns:
                return
            
            # Remove valores vazios
            df_clean = df[df['DES_STATUS_FORMULARIO'].notna() & (df['DES_STATUS_FORMULARIO'] != '')]
            
            if df_clean.empty:
                return
            
            # Conta infrações únicas por status se NUM_AUTO_INFRACAO disponível
            if 'NUM_AUTO_INFRACAO' in df_clean.columns:
                status_counts = df_clean.groupby('DES_STATUS_FORMULARIO')['NUM_AUTO_INFRACAO'].nunique().sort_values(ascending=False).head(10)
                method_note = "infrações únicas"
            else:
                status_counts = df_clean['DES_STATUS_FORMULARIO'].value_counts().head(10)
                method_note = "registros"
            
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
                    title=f"<b>Estágio Atual das Infrações (Top 10 - {method_note})</b>", 
                    text='total'
                )
                st.plotly_chart(fig, use_container_width=True)
                
        except Exception as e:
            st.error(f"Erro no gráfico de status: {e}")

    # ======================== MÉTODOS LEGACY (para compatibilidade) ========================

    def create_overview_metrics(self, selected_ufs: list, year_range: tuple):
        """Método legacy - converte year_range para date_filters."""
        date_filters = {
            "mode": "simple",
            "years": list(range(year_range[0], year_range[1] + 1)),
            "year_range": year_range,
            "description": f"{year_range[0]}-{year_range[1]}"
        }
        return self.create_overview_metrics_advanced(selected_ufs, date_filters)

    def create_infraction_map(self, selected_ufs: list, year_range: tuple):
        """Método legacy - converte year_range para date_filters."""
        date_filters = {
            "mode": "simple",
            "years": list(range(year_range[0], year_range[1] + 1)),
            "year_range": year_range,
            "description": f"{year_range[0]}-{year_range[1]}"
        }
        return self.create_infraction_map_advanced(selected_ufs, date_filters)

    def create_municipality_hotspots_chart(self, selected_ufs: list, year_range: tuple):
        """Método legacy - converte year_range para date_filters."""
        date_filters = {
            "mode": "simple",
            "years": list(range(year_range[0], year_range[1] + 1)),
            "year_range": year_range,
            "description": f"{year_range[0]}-{year_range[1]}"
        }
        return self.create_municipality_hotspots_chart_advanced(selected_ufs, date_filters)

    def create_fine_value_by_type_chart(self, selected_ufs: list, year_range: tuple):
        """Método legacy - converte year_range para date_filters."""
        date_filters = {
            "mode": "simple",
            "years": list(range(year_range[0], year_range[1] + 1)),
            "year_range": year_range,
            "description": f"{year_range[0]}-{year_range[1]}"
        }
        return self.create_fine_value_by_type_chart_advanced(selected_ufs, date_filters)

    def create_gravity_distribution_chart(self, selected_ufs: list, year_range: tuple):
        """Método legacy - converte year_range para date_filters e inclui infrações sem avaliação."""
        date_filters = {
            "mode": "simple",
            "years": list(range(year_range[0], year_range[1] + 1)),
            "year_range": year_range,
            "description": f"{year_range[0]}-{year_range[1]}"
        }
        return self.create_gravity_distribution_chart_advanced(selected_ufs, date_filters)

    def create_state_distribution_chart(self, selected_ufs: list, year_range: tuple):
        """Método legacy - converte year_range para date_filters."""
        date_filters = {
            "mode": "simple",
            "years": list(range(year_range[0], year_range[1] + 1)),
            "year_range": year_range,
            "description": f"{year_range[0]}-{year_range[1]}"
        }
        return self.create_state_distribution_chart_advanced(selected_ufs, date_filters)

    def create_infraction_status_chart(self, selected_ufs: list, year_range: tuple):
        """Método legacy - converte year_range para date_filters."""
        date_filters = {
            "mode": "simple",
            "years": list(range(year_range[0], year_range[1] + 1)),
            "year_range": year_range,
            "description": f"{year_range[0]}-{year_range[1]}"
        }
        return self.create_infraction_status_chart_advanced(selected_ufs, date_filters)

    def create_main_offenders_chart(self, selected_ufs: list, year_range: tuple):
        """Método legacy - converte year_range para date_filters."""
        date_filters = {
            "mode": "simple",
            "years": list(range(year_range[0], year_range[1] + 1)),
            "year_range": year_range,
            "description": f"{year_range[0]}-{year_range[1]}"
        }
        return self.create_main_offenders_chart_advanced(selected_ufs, date_filters)

    def force_refresh(self):
        """Força atualização dos dados limpando cache."""
        if self.paginator:
            self.paginator.clear_cache()
            st.success("🔄 Cache limpo! Os dados serão recarregados.")
