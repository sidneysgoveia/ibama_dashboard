import streamlit as st
import pandas as pd
import os

# Importa os componentes do projeto
from src.utils.database import Database
from src.utils.llm_integration import LLMIntegration
from src.components.visualization import DataVisualization
from src.components.chatbot import Chatbot

def main():
    st.set_page_config(page_title="Análise de Infrações IBAMA", page_icon="🌳", layout="wide")
    
    st.title("🌳 Análise de Autos de Infração do IBAMA")
    
    # Inicializa os componentes principais
    db = Database()
    llm = LLMIntegration(database=db)
    viz = DataVisualization(database=db)
    chatbot = Chatbot(llm_integration=llm)
    chatbot.initialize_chat_state()

    # --- BARRA LATERAL (SIDEBAR) ---
    with st.sidebar:
        st.header("🔎 Filtros do Dashboard")

        try:
            # Query para obter a lista de UFs (já estava correta)
            ufs_list = db.execute_query("SELECT DISTINCT \"UF\" FROM ibama_infracao WHERE \"UF\" IS NOT NULL ORDER BY \"UF\"")['UF'].tolist()
            selected_ufs = st.multiselect("Selecione o Estado (UF)", options=ufs_list, default=[])

            # --- CORREÇÃO APLICADA AQUI ---
            # Constrói a query de anos dinamicamente com base no ambiente (nuvem ou local)
            if db.is_cloud:
                # Sintaxe para PostgreSQL (Supabase)
                years_query = "SELECT DISTINCT EXTRACT(YEAR FROM TO_TIMESTAMP(\"DAT_HORA_AUTO_INFRACAO\", 'YYYY-MM-DD HH24:MI:SS')) as ano FROM ibama_infracao WHERE \"DAT_HORA_AUTO_INFRACAO\" IS NOT NULL ORDER BY ano DESC"
            else:
                # Sintaxe para DuckDB (Local)
                years_query = "SELECT DISTINCT EXTRACT(YEAR FROM TRY_CAST(\"DAT_HORA_AUTO_INFRACAO\" AS TIMESTAMP)) as ano FROM ibama_infracao WHERE ano IS NOT NULL ORDER BY ano DESC"
            
            years_df = db.execute_query(years_query)
            # --- FIM DA CORREÇÃO ---

            years_list = [int(y) for y in years_df['ano'].dropna().tolist()]
            if years_list:
                min_year, max_year = min(years_list), max(years_list)
                year_range = st.slider("Selecione o Intervalo de Anos", min_year, max_year, (min_year, max_year))
            else:
                # Fallback caso a lista de anos venha vazia
                year_range = (2024, 2025) 
        except Exception as e:
            st.error(f"Erro ao carregar filtros: {e}. Verifique a conexão e a sintaxe da query.")
            selected_ufs = []
            year_range = (2024, 2025)

        st.divider()
        
        st.info("Os dados são atualizados diariamente pela equipe de administração.")
        
        chatbot.display_sample_questions()

        st.divider()
        with st.expander("⚠️ Avisos Importantes"):
            st.warning(
                "**Não use IA para escrever um texto inteiro!** O auxílio é melhor para gerar resumos, "
                "filtrar informações ou auxiliar a entender contextos - que depois devem ser checados. "
                "Inteligência Artificial comete erros!"
            )
            st.info(
                "Este projeto não se responsabiliza pelos conteúdos criados a partir deste site. "
                "Cheque as informações com os dados originais do Ibama e mais fontes."
            )

        with st.expander("ℹ️ Sobre este App"):
            st.markdown(
                """
                **Fonte dos dados:** [Portal de Dados Abertos do IBAMA](https://dadosabertos.ibama.gov.br/dataset/fiscalizacao-auto-de-infracao)
                **Desenvolvido por:** Reinaldo Chaves.
                """
            )

    # --- CONTEÚDO PRINCIPAL COM ABAS ---
    tab1, tab2, tab3 = st.tabs(["📊 Dashboard Interativo", "💬 Chatbot com IA", "🔍 Explorador SQL"])
    
    with tab1:
        st.header("Dashboard de Análise de Infrações Ambientais")
        st.caption("Use os filtros na barra lateral para explorar os dados.")
        viz.create_overview_metrics(selected_ufs, year_range)
        st.divider()
        viz.create_infraction_map(selected_ufs, year_range)
        st.divider()
        col1, col2 = st.columns(2)
        with col1:
            viz.create_municipality_hotspots_chart(selected_ufs, year_range)
            viz.create_fine_value_by_type_chart(selected_ufs, year_range)
            viz.create_gravity_distribution_chart(selected_ufs, year_range)
        with col2:
            viz.create_state_distribution_chart(selected_ufs, year_range)
            viz.create_infraction_status_chart(selected_ufs, year_range)
            viz.create_main_offenders_chart(selected_ufs, year_range)
    
    with tab2:
        chatbot.display_chat_interface()
    
    with tab3:
        st.header("Explorador de Dados SQL")
        query = st.text_area("Escreva sua consulta SQL (apenas SELECT)", value="SELECT * FROM ibama_infracao LIMIT 10", height=150)
        if st.button("Executar Consulta"):
            if query.strip().lower().startswith("select"):
                try:
                    df = db.execute_query(query)
                    st.dataframe(df)
                except Exception as e:
                    st.error(f"Erro na consulta: {e}")
            else:
                st.error("Apenas consultas SELECT são permitidas por segurança.")

if __name__ == "__main__":
    main()
