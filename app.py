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

def get_ufs_from_database(database_obj):
    """Busca UFs do banco de dados sem cache."""
    # Lista padrão do Brasil como fallback
    brasil_ufs = [
        'AC', 'AL', 'AP', 'AM', 'BA', 'CE', 'DF', 'ES', 'GO', 
        'MA', 'MT', 'MS', 'MG', 'PA', 'PB', 'PR', 'PE', 'PI', 
        'RJ', 'RN', 'RS', 'RO', 'RR', 'SC', 'SP', 'SE', 'TO'
    ]
    
    try:
        if database_obj.is_cloud and database_obj.supabase:
            # Para Supabase, usa o paginador se disponível
            try:
                from src.utils.supabase_utils import SupabasePaginator
                paginator = SupabasePaginator(database_obj.supabase)
                
                # Busca todos os dados e extrai UFs únicos
                df = paginator.get_all_records()
                if not df.empty and 'UF' in df.columns:
                    ufs_from_data = df['UF'].dropna().unique().tolist()
                    unique_ufs = sorted([uf for uf in ufs_from_data if str(uf).strip()])
                    
                    if len(unique_ufs) >= 10:
                        return unique_ufs, f"Da base completa ({len(unique_ufs)} estados)"
            except ImportError:
                pass
            
            # Fallback: busca amostra direta do Supabase
            result = database_obj.supabase.table('ibama_infracao').select('UF').limit(50000).execute()
            
            if result.data:
                # Extrai UFs únicos da amostra
                all_ufs = [item['UF'] for item in result.data if item.get('UF') and str(item['UF']).strip()]
                unique_ufs = sorted(list(set(all_ufs)))
                
                # Se conseguiu UFs suficientes, usa da base
                if len(unique_ufs) >= 15:
                    return unique_ufs, f"Da base de dados ({len(unique_ufs)} estados)"
        
        # Não tenta SQL para Supabase (sabemos que não funciona)
        # Vai direto para o fallback
    
    except Exception as e:
        print(f"Erro ao buscar UFs: {e}")
    
    # Fallback: usa lista do Brasil
    return brasil_ufs, f"Lista padrão Brasil ({len(brasil_ufs)} estados)"

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
            # Filtros UF - método corrigido e simplificado
            with st.spinner("Carregando estados..."):
                ufs_list, source_info = get_ufs_from_database(st.session_state.db)
                
                # Feedback visual mais preciso
                if "base completa" in source_info:
                    st.success(source_info)
                elif "base de dados" in source_info or "amostra" in source_info:
                    st.info(source_info)
                else:
                    st.info(source_info)  # Para "Lista oficial Brasil"
            
            selected_ufs = st.multiselect(
                "Selecione o Estado (UF)", 
                options=ufs_list, 
                default=[],
                help=f"Estados disponíveis: {len(ufs_list)}"
            )

            # Filtros de ano - método simplificado
            current_year = 2025
            min_year = 2024
            year_range = st.slider(
                "Selecione o Intervalo de Anos", 
                min_year, 
                current_year, 
                (min_year, current_year),
                help="Dados disponíveis de 2024 a 2025"
            )
                
        except Exception as e:
            st.error(f"Erro ao carregar filtros: {e}")
            
            # Fallback completo em caso de erro total
            brasil_ufs = [
                'AC', 'AL', 'AP', 'AM', 'BA', 'CE', 'DF', 'ES', 'GO', 
                'MA', 'MT', 'MS', 'MG', 'PA', 'PB', 'PR', 'PE', 'PI', 
                'RJ', 'RN', 'RS', 'RO', 'RR', 'SC', 'SP', 'SE', 'TO'
            ]
            
            selected_ufs = st.multiselect(
                "Selecione o Estado (UF) - Modo Emergência", 
                options=brasil_ufs, 
                default=[],
                help="Lista padrão (erro ao conectar com base de dados)"
            )
            
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
    tab1, tab2, tab3, tab4 = st.tabs(["📊 Dashboard Interativo", "💬 Chatbot com IA", "🔍 Explorador SQL", "🔧 Diagnóstico"])
    
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
    
    with tab4:
        st.header("🔧 Diagnóstico do Sistema")
        st.caption("Identifica problemas de contagem de registros")
        
        if st.button("🚀 Executar Diagnóstico", type="primary"):
            with st.spinner("Executando diagnóstico..."):
                try:
                    # Teste 1: Count exato
                    st.subheader("📊 Testes de Contagem")
                    
                    if st.session_state.db.is_cloud and st.session_state.db.supabase:
                        supabase = st.session_state.db.supabase
                        
                        # Count exato
                        result = supabase.table('ibama_infracao').select('*', count='exact').limit(1).execute()
                        count_exact = getattr(result, 'count', 0)
                        
                        # Busca completa
                        result_all = supabase.table('ibama_infracao').select('*').execute()
                        count_all = len(result_all.data) if result_all.data else 0
                        
                        # Exibe resultados
                        col1, col2 = st.columns(2)
                        col1.metric("Count API", f"{count_exact:,}")
                        col2.metric("Busca Completa", f"{count_all:,}")
                        
                        if count_exact != count_all:
                            st.warning(f"⚠️ PROBLEMA: API diz {count_exact:,} mas carregou {count_all:,}")
                        
                        # Análise por ano
                        if result_all.data:
                            df = pd.DataFrame(result_all.data)
                            st.info(f"DataFrame: {len(df)} registros, {len(df.columns)} colunas")
                            
                            if 'DAT_HORA_AUTO_INFRACAO' in df.columns:
                                df['DAT_HORA_AUTO_INFRACAO'] = pd.to_datetime(df['DAT_HORA_AUTO_INFRACAO'], errors='coerce')
                                
                                # Por ano
                                year_counts = df['DAT_HORA_AUTO_INFRACAO'].dt.year.value_counts().sort_index()
                                
                                st.subheader("📅 Registros por Ano")
                                for year, count in year_counts.tail(6).items():
                                    if pd.notna(year) and year >= 2020:
                                        st.write(f"**{int(year)}:** {count:,} registros")
                                
                                # Foco 2024-2025
                                df_recent = df[df['DAT_HORA_AUTO_INFRACAO'].dt.year.isin([2024, 2025])]
                                
                                st.subheader("🎯 Simulação Dashboard")
                                total_infracoes = len(df_recent)
                                
                                if total_infracoes == 1000:
                                    st.error("❌ PROBLEMA CONFIRMADO: Limitado a 1.000 registros")
                                    st.info("💡 Supabase limita select('*') a 1000 registros por padrão")
                                elif total_infracoes > 15000:
                                    st.success(f"✅ Funcionando! {total_infracoes:,} registros")
                                else:
                                    st.warning(f"⚠️ Resultado inesperado: {total_infracoes:,} registros")
                        
                    else:
                        st.error("❌ Banco não está em modo cloud ou Supabase não inicializado")
                        
                except Exception as e:
                    st.error(f"❌ Erro no diagnóstico: {e}")
        
        # Teste rápido
        if st.button("⚡ Teste Rápido"):
            try:
                if st.session_state.db.is_cloud:
                    result = st.session_state.db.supabase.table('ibama_infracao').select('*', count='exact').limit(1).execute()
                    count = getattr(result, 'count', 0)
                    st.success(f"✅ {count:,} registros na base")
                else:
                    st.info("ℹ️ Modo local - não aplicável")
            except Exception as e:
                st.error(f"❌ {e}")

if __name__ == "__main__":
    main()
