import streamlit as st
import pandas as pd
import plotly.express as px

# Importa as funções de formatação
from src.utils.formatters import format_currency_brazilian, format_number_brazilian

class DataVisualization:
    def __init__(self, database=None):
        """Inicializa o componente de visualização com a conexão do banco de dados."""
        self.database = database

    def _build_where_clause(self, selected_ufs: list, year_range: tuple) -> str:
        """Constrói a cláusula WHERE dinâmica para as consultas SQL com base nos filtros."""
        where_clauses = ["1=1"]

        if selected_ufs:
            ufs_formatted = ", ".join([f"'{uf}'" for uf in selected_ufs])
            where_clauses.append(f"UF IN ({ufs_formatted})")

        if year_range:
            start_year, end_year = year_range
            where_clauses.append(f"EXTRACT(YEAR FROM TRY_CAST(DAT_HORA_AUTO_INFRACAO AS TIMESTAMP)) BETWEEN {start_year} AND {end_year}")

        return " AND ".join(where_clauses)

    def create_overview_metrics(self, selected_ufs: list, year_range: tuple):
        """Cria as métricas de visão geral, agora dinâmicas com base nos filtros."""
        if not self.database:
            st.warning("Banco de dados não disponível.")
            return

        where_clause = self._build_where_clause(selected_ufs, year_range)
        
        query = f"""
        SELECT
            COUNT(*) as total_infracoes,
            SUM(CAST(REPLACE(VAL_AUTO_INFRACAO, ',', '.') AS DOUBLE)) as valor_total_multas,
            COUNT(DISTINCT MUNICIPIO) as total_municipios
        FROM ibama_infracao
        WHERE {where_clause}
          AND VAL_AUTO_INFRACAO IS NOT NULL AND VAL_AUTO_INFRACAO != ''
        """
        metrics_df = self.database.execute_query(query)

        if metrics_df.empty or metrics_df['total_infracoes'].iloc[0] == 0:
            st.warning("Nenhum dado encontrado para os filtros selecionados.")
            return

        total_infracoes = metrics_df['total_infracoes'].iloc[0]
        valor_total_multas = metrics_df['valor_total_multas'].iloc[0]
        total_municipios = metrics_df['total_municipios'].iloc[0]

        col1, col2, col3 = st.columns(3)
        col1.metric("Total de Infrações", format_number_brazilian(total_infracoes))
        col2.metric("Valor Total das Multas", format_currency_brazilian(valor_total_multas))
        col3.metric("Municípios Afetados", format_number_brazilian(total_municipios))

    def create_time_series_chart(self, selected_ufs: list, year_range: tuple):
        """Cria o gráfico de série temporal de infrações."""
        where_clause = self._build_where_clause(selected_ufs, year_range)
        query = f"""
        SELECT
            SUBSTR(DAT_HORA_AUTO_INFRACAO, 1, 7) as mes,
            COUNT(*) as numero_infracoes
        FROM ibama_infracao
        WHERE {where_clause}
        GROUP BY mes
        ORDER BY mes
        """
        df = self.database.execute_query(query)
        if df.empty:
            st.info("Sem dados para o gráfico de evolução temporal com os filtros atuais.")
            return
        
        fig = px.line(df, x='mes', y='numero_infracoes', title="<b>Evolução Mensal de Infrações</b>", markers=True)
        st.plotly_chart(fig, use_container_width=True)

    def create_state_distribution_chart(self, selected_ufs: list, year_range: tuple):
        """Cria o gráfico de distribuição por estado."""
        where_clause = self._build_where_clause(selected_ufs, year_range)
        query = f"""
        SELECT UF, COUNT(*) as total
        FROM ibama_infracao
        WHERE {where_clause} AND UF IS NOT NULL
        GROUP BY UF
        ORDER BY total DESC
        LIMIT 15
        """
        df = self.database.execute_query(query)
        if df.empty:
            st.info("Sem dados para o gráfico de distribuição por estado com os filtros atuais.")
            return

        fig = px.bar(df, x='UF', y='total', title="<b>Distribuição de Infrações por Estado</b>", color='total',
                     labels={'UF': 'Estado', 'total': 'Nº de Infrações'})
        st.plotly_chart(fig, use_container_width=True)

    def create_municipality_hotspots_chart(self, selected_ufs: list, year_range: tuple):
        """Mostra os 10 municípios com mais infrações (hotspots)."""
        where_clause = self._build_where_clause(selected_ufs, year_range)
        query = f"""
        SELECT MUNICIPIO, UF, COUNT(*) as total
        FROM ibama_infracao
        WHERE {where_clause} AND MUNICIPIO IS NOT NULL AND MUNICIPIO != ''
        GROUP BY MUNICIPIO, UF
        ORDER BY total DESC
        LIMIT 10
        """
        df = self.database.execute_query(query)
        if df.empty:
            st.info("Sem dados para o gráfico de hotspots de municípios com os filtros atuais.")
            return
        
        df['local'] = df['MUNICIPIO'].str.title() + ' (' + df['UF'] + ')'
        fig = px.bar(df.sort_values('total'), y='local', x='total', orientation='h', 
                     title="<b>Top 10 Municípios com Mais Infrações (Hotspots)</b>",
                     labels={'local': 'Município', 'total': 'Nº de Infrações'},
                     text='total')
        fig.update_traces(textposition='outside')
        st.plotly_chart(fig, use_container_width=True)

    def create_fine_value_by_type_chart(self, selected_ufs: list, year_range: tuple):
        """Mostra o valor total de multas por tipo de infração."""
        where_clause = self._build_where_clause(selected_ufs, year_range)
        query = f"""
        SELECT
            TIPO_INFRACAO,
            SUM(CAST(REPLACE(VAL_AUTO_INFRACAO, ',', '.') AS DOUBLE)) as valor_total
        FROM ibama_infracao
        WHERE {where_clause}
          AND VAL_AUTO_INFRACAO IS NOT NULL AND VAL_AUTO_INFRACAO != ''
          AND TIPO_INFRACAO IS NOT NULL AND TIPO_INFRACAO != ''
        GROUP BY TIPO_INFRACAO
        ORDER BY valor_total DESC
        LIMIT 10
        """
        df = self.database.execute_query(query)
        if df.empty:
            st.info("Sem dados para o gráfico de valor por tipo de infração com os filtros atuais.")
            return

        df['TIPO_INFRACAO'] = df['TIPO_INFRACAO'].str.title().str.replace('Ã§', 'ç').replace('Ã£', 'ã')
        fig = px.bar(df.sort_values('valor_total'), y='TIPO_INFRACAO', x='valor_total', orientation='h',
                     title="<b>Tipos de Infração por Valor Total de Multas (Top 10)</b>",
                     labels={'TIPO_INFRACAO': 'Tipo de Infração', 'valor_total': 'Valor Total (R$)'})
        fig.update_layout(xaxis_title="Valor Total (R$)")
        st.plotly_chart(fig, use_container_width=True)

    def create_gravity_distribution_chart(self, selected_ufs: list, year_range: tuple):
        """Mostra a distribuição pela gravidade da infração."""
        where_clause = self._build_where_clause(selected_ufs, year_range)
        query = f"""
        SELECT GRAVIDADE_INFRACAO, COUNT(*) as total
        FROM ibama_infracao
        WHERE {where_clause} AND GRAVIDADE_INFRACAO IS NOT NULL AND GRAVIDADE_INFRACAO != ''
        GROUP BY GRAVIDADE_INFRACAO
        """
        df = self.database.execute_query(query)
        if df.empty:
            st.info("Sem dados para o gráfico de gravidade com os filtros atuais.")
            return
        
        fig = px.pie(df, names='GRAVIDADE_INFRACAO', values='total', 
                     title="<b>Distribuição por Gravidade da Infração</b>",
                     hole=0.4)
        st.plotly_chart(fig, use_container_width=True)

    def create_main_offenders_chart(self, selected_ufs: list, year_range: tuple):
        """Mostra os 10 maiores infratores por valor total de multas."""
        where_clause = self._build_where_clause(selected_ufs, year_range)
        query = f"""
        SELECT
            NOME_INFRATOR,
            SUM(CAST(REPLACE(VAL_AUTO_INFRACAO, ',', '.') AS DOUBLE)) as valor_total
        FROM ibama_infracao
        WHERE {where_clause}
          AND VAL_AUTO_INFRACAO IS NOT NULL AND VAL_AUTO_INFRACAO != ''
          AND NOME_INFRATOR IS NOT NULL AND NOME_INFRATOR != ''
        GROUP BY NOME_INFRATOR
        ORDER BY valor_total DESC
        LIMIT 10
        """
        df = self.database.execute_query(query)
        if df.empty:
            st.info("Sem dados para o gráfico de maiores infratores com os filtros atuais.")
            return
        
        df['NOME_INFRATOR'] = df['NOME_INFRATOR'].str.title().str.slice(0, 40)
        fig = px.bar(df.sort_values('valor_total'), y='NOME_INFRATOR', x='valor_total', orientation='h',
                     title="<b>Top 10 Infratores por Valor Total de Multas</b>",
                     labels={'NOME_INFRATOR': 'Infrator', 'valor_total': 'Valor Total (R$)'})
        fig.update_layout(xaxis_title="Valor Total (R$)")
        st.plotly_chart(fig, use_container_width=True)

    def create_infraction_map(self, selected_ufs: list, year_range: tuple):
        """Mostra um mapa com a localização das infrações."""
        st.subheader("Mapa de Calor de Infrações")
        
        where_clause = self._build_where_clause(selected_ufs, year_range)
        query = f"""
        (SELECT
            NUM_LATITUDE_AUTO,
            NUM_LONGITUDE_AUTO
        FROM ibama_infracao
        WHERE {where_clause}
          AND NUM_LATITUDE_AUTO IS NOT NULL AND NUM_LATITUDE_AUTO != ''
          AND NUM_LONGITUDE_AUTO IS NOT NULL AND NUM_LONGITUDE_AUTO != ''
        ORDER BY RANDOM()
        LIMIT 5000)
        """
        
        with st.spinner("Carregando dados do mapa..."):
            df_map = self.database.execute_query(query)

        if df_map.empty:
            st.warning("Nenhum dado de geolocalização encontrado para os filtros selecionados.")
            return
        
        df_map.rename(columns={'NUM_LATITUDE_AUTO': 'lat', 'NUM_LONGITUDE_AUTO': 'lon'}, inplace=True)
        
        df_map['lat'] = pd.to_numeric(df_map['lat'].str.replace(',', '.'), errors='coerce')
        df_map['lon'] = pd.to_numeric(df_map['lon'].str.replace(',', '.'), errors='coerce')
        df_map.dropna(subset=['lat', 'lon'], inplace=True)

        st.map(df_map, zoom=3)
        st.caption("Cada ponto no mapa representa a localização aproximada de uma infração.")

    def create_infraction_status_chart(self, selected_ufs: list, year_range: tuple):
        """Mostra a distribuição pelo estágio atual da infração."""
        where_clause = self._build_where_clause(selected_ufs, year_range)
        query = f"""
        SELECT DES_STATUS_FORMULARIO, COUNT(*) as total
        FROM ibama_infracao
        WHERE {where_clause} AND DES_STATUS_FORMULARIO IS NOT NULL AND DES_STATUS_FORMULARIO != ''
        GROUP BY DES_STATUS_FORMULARIO
        ORDER BY total DESC
        LIMIT 10
        """
        df = self.database.execute_query(query)
        if df.empty:
            st.info("Sem dados para o gráfico de estágio da infração com os filtros atuais.")
            return
        
        df['DES_STATUS_FORMULARIO'] = df['DES_STATUS_FORMULARIO'].str.title().str.replace('Ã§', 'ç').replace('Ã£', 'ã')
        
        fig = px.bar(df.sort_values('total'), y='DES_STATUS_FORMULARIO', x='total', orientation='h',
                     title="<b>Estágio Atual das Infrações (Top 10)</b>",
                     labels={'DES_STATUS_FORMULARIO': 'Situação', 'total': 'Nº de Infrações'},
                     text='total')
        fig.update_traces(textposition='outside')
        st.plotly_chart(fig, use_container_width=True)