import streamlit as st
import pandas as pd
import os

# Configuração otimizada para reduzir uso de recursos
st.set_page_config(
    page_title="Análise de Infrações IBAMA", 
    page_icon="🌳", 
    layout="wide",
    initial_sidebar_state="expanded"
)

# Cache das importações para reduzir recarregamentos
@st.cache_resource
def load_components():
    """Carrega componentes de forma cached para reduzir file watching."""
    try:
        from src.utils.database import Database
        from src.utils.llm_integration import LLMIntegration
        from src.components.visualization import DataVisualization
        from src.components.chatbot import Chatbot
        return Database, LLMIntegration, DataVisualization, Chatbot
    except ImportError as e:
        st.error(f"Erro ao carregar componentes: {e}")
        return None, None, None, None

def main():
    st.title("🌳 Análise de Autos de Infração do IBAMA")
    
    # Carrega componentes com cache
    Database, LLMIntegration, DataVisualization, Chatbot = load_components()
    
    if not all([Database, LLMIntegration, DataVisualization, Chatbot]):
        st.error("Falha ao carregar componentes necessários.")
        st.stop()
    
    # Inicializa componentes apenas quando necessário
    try:
        # Inicialização lazy dos componentes
        if 'db' not in st.session_state:
            st.session_state.db = Database()
        
        if 'llm' not in st.session_state:
            st.session_state.llm = LLMIntegration(database=st.session_state.db)
        
        if 'viz' not in st.session_state:
            st.session_state.viz = DataVisualization(database=st.session_state.db)
        
        if 'chatbot' not in st.session_state:
            st.session_state.chatbot = Chatbot(llm_integration=st.session_state.llm)
            st.session_state.chatbot.initialize_chat_state()
            
    except Exception as e:
        st.error(f"Erro na inicialização: {e}")
        st.stop()

    # Sidebar com tratamento de erro
    with st.sidebar:
        st.header("🔎 Filtros do Dashboard")

        try:
            # Filtros UF - método otimizado
            with st.spinner("Carregando estados..."):
                ufs_list = st.session_state.db.get_unique_values('UF')
                
                # Fallback se não conseguir carregar UFs
                if not ufs_list:
                    st.warning("Não foi possível carregar UFs do banco. Usando lista padrão.")
                    ufs_list = [
                        'AC', 'AL', 'AP', 'AM', 'BA', 'CE', 'DF', 'ES', 'GO', 
                        'MA', 'MT', 'MS', 'MG', 'PA', 'PB', 'PR', 'PE', 'PI', 
                        'RJ', 'RN', 'RS', 'RO', 'RR', 'SC', 'SP', 'SE', 'TO'
                    ]
                
                # Mostra informação de debug
                st.sidebar.success(f"✅ {len(ufs_list)} estados carregados")
            
            selected_ufs = st.multiselect(
                "Selecione o Estado (UF)", 
                options=ufs_list, 
                default=[],
                help=f"Escolha entre {len(ufs_list)} estados disponíveis"
            )

            # Filtros de ano - método simplificado
            current_year = 2025
            min_year = 2024
            year_range = st.slider(
                "Selecione o Intervalo de Anos", 
                min_year, 
                current_year, 
                (min_year, current_year)
            )
                
        except Exception as e:
            st.error(f"Erro ao carregar filtros: {e}")
            selected_ufs = []
            year_range = (2024, 2025)

        st.divider()
        st.info("Os dados são atualizados diariamente.")
        
        # Sample questions do chatbot
        try:
            st.session_state.chatbot.display_sample_questions()
        except:
            pass

        st.divider()
        with st.expander("⚠️ Avisos Importantes"):
            st.warning("**Não use IA para escrever um texto inteiro!** Use para resumos e análises que devem ser verificados.")
            st.info("Cheque as informações com os dados originais do Ibama e outras fontes.")

        with st.expander("ℹ️ Sobre este App"):
            st.markdown("""
                **Fonte:** [Portal de Dados Abertos do IBAMA](https://dadosabertos.ibama.gov.br/dataset/fiscalizacao-auto-de-infracao)
                
                **Desenvolvido por:** Reinaldo Chaves
            """)

    # Abas principais
    tab1, tab2, tab3 = st.tabs(["📊 Dashboard Interativo", "💬 Chatbot com IA", "🔍 Explorador SQL"])
    
    with tab1:
        st.header("Dashboard de Análise de Infrações Ambientais")
        st.caption("Use os filtros na barra lateral para explorar os dados.")
        
        try:
            st.session_state.viz.create_overview_metrics(selected_ufs, year_range)
            st.divider()
            st.session_state.viz.create_infraction_map(selected_ufs, year_range)
            st.divider()
            
            col1, col2 = st.columns(2)
            with col1:
                st.session_state.viz.create_municipality_hotspots_chart(selected_ufs, year_range)
                st.session_state.viz.create_fine_value_by_type_chart(selected_ufs, year_range)
                st.session_state.viz.create_gravity_distribution_chart(selected_ufs, year_range)
            with col2:
                st.session_state.viz.create_state_distribution_chart(selected_ufs, year_range)
                st.session_state.viz.create_infraction_status_chart(selected_ufs, year_range)
                st.session_state.viz.create_main_offenders_chart(selected_ufs, year_range)
        except Exception as e:
            st.error(f"Erro ao gerar visualizações: {e}")
            st.info("Tentando recarregar os componentes...")
            
            # Força recarregamento dos componentes
            if 'viz' in st.session_state:
                del st.session_state.viz
            
            try:
                st.session_state.viz = DataVisualization(database=st.session_state.db)
                st.rerun()
            except:
                st.error("Não foi possível recarregar os componentes. Recarregue a página.")
    
    with tab2:
        try:
            st.session_state.chatbot.display_chat_interface()
        except Exception as e:
            st.error(f"Erro no chatbot: {e}")
    
    with tab3:
        st.header("Explorador de Dados SQL")
        query = st.text_area(
            "Escreva sua consulta SQL (apenas SELECT)", 
            value="SELECT * FROM ibama_infracao LIMIT 10", 
            height=150
        )
        
        if st.button("Executar Consulta"):
            if query.strip().lower().startswith("select"):
                try:
                    df = st.session_state.db.execute_query(query)
                    st.dataframe(df)
                except Exception as e:
                    st.error(f"Erro na consulta: {e}")
            else:
                st.error("Apenas consultas SELECT são permitidas por segurança.")

if __name__ == "__main__":
    main()
