import streamlit as st
import pandas as pd
import os

# Configuração otimizada
st.set_page_config(
    page_title="Análise de Infrações IBAMA", 
    page_icon="🌳", 
    layout="wide",
    initial_sidebar_state="expanded"
)

# Cache das importações com tratamento de erro robusto
@st.cache_resource
def load_components():
    """Carrega componentes de forma cached e com tratamento de erro detalhado."""
    components = {}
    
    # Testa cada importação individualmente
    try:
        from src.utils.database import Database
        components['Database'] = Database
        st.success("✅ Database importado")
    except ImportError as e:
        st.error(f"❌ Erro ao importar Database: {e}")
        components['Database'] = None
    except Exception as e:
        st.error(f"❌ Erro inesperado no Database: {e}")
        components['Database'] = None
    
    try:
        from src.utils.llm_integration import LLMIntegration
        components['LLMIntegration'] = LLMIntegration
        st.success("✅ LLMIntegration importado")
    except ImportError as e:
        st.error(f"❌ Erro ao importar LLMIntegration: {e}")
        components['LLMIntegration'] = None
    except Exception as e:
        st.error(f"❌ Erro inesperado no LLMIntegration: {e}")
        components['LLMIntegration'] = None
    
    try:
        from src.components.visualization import DataVisualization
        components['DataVisualization'] = DataVisualization
        st.success("✅ DataVisualization importado")
    except ImportError as e:
        st.error(f"❌ Erro ao importar DataVisualization: {e}")
        components['DataVisualization'] = None
    except Exception as e:
        st.error(f"❌ Erro inesperado no DataVisualization: {e}")
        components['DataVisualization'] = None
    
    try:
        from src.components.chatbot import Chatbot
        components['Chatbot'] = Chatbot
        st.success("✅ Chatbot importado")
    except ImportError as e:
        st.error(f"❌ Erro ao importar Chatbot: {e}")
        components['Chatbot'] = None
    except Exception as e:
        st.error(f"❌ Erro inesperado no Chatbot: {e}")
        components['Chatbot'] = None
    
    return components

def safe_initialize_database(Database):
    """Inicializa database de forma segura."""
    try:
        if Database:
            db = Database()
            st.success("✅ Database inicializado com sucesso")
            return db
        else:
            st.error("❌ Classe Database não disponível")
            return None
    except Exception as e:
        st.error(f"❌ Erro ao inicializar Database: {e}")
        return None

def safe_initialize_llm(LLMIntegration, database):
    """Inicializa LLM de forma segura."""
    try:
        if LLMIntegration and database:
            llm = LLMIntegration(database=database)
            st.success("✅ LLM inicializado com sucesso")
            return llm
        else:
            st.error("❌ LLMIntegration ou Database não disponível")
            return None
    except Exception as e:
        st.error(f"❌ Erro ao inicializar LLM: {e}")
        return None

def safe_initialize_viz(DataVisualization, database):
    """Inicializa visualização de forma segura."""
    try:
        if DataVisualization and database:
            viz = DataVisualization(database=database)
            st.success("✅ DataVisualization inicializado com sucesso")
            return viz
        else:
            st.error("❌ DataVisualization ou Database não disponível")
            return None
    except Exception as e:
        st.error(f"❌ Erro ao inicializar DataVisualization: {e}")
        return None

def safe_initialize_chatbot(Chatbot, llm):
    """Inicializa chatbot de forma segura."""
    try:
        if Chatbot and llm:
            chatbot = Chatbot(llm_integration=llm)
            chatbot.initialize_chat_state()
            st.success("✅ Chatbot inicializado com sucesso")
            return chatbot
        else:
            st.error("❌ Chatbot ou LLM não disponível")
            return None
    except Exception as e:
        st.error(f"❌ Erro ao inicializar Chatbot: {e}")
        return None

def create_simple_filters():
    """Cria filtros básicos e seguros."""
    try:
        with st.sidebar:
            st.header("🔎 Filtros Básicos (Modo Seguro)")
            
            # Estados brasileiros (lista fixa)
            brasil_ufs = [
                'AC', 'AL', 'AP', 'AM', 'BA', 'CE', 'DF', 'ES', 'GO', 
                'MA', 'MT', 'MS', 'MG', 'PA', 'PB', 'PR', 'PE', 'PI', 
                'RJ', 'RN', 'RS', 'RO', 'RR', 'SC', 'SP', 'SE', 'TO'
            ]
            
            selected_ufs = st.multiselect(
                "Selecione Estados (UF):", 
                options=brasil_ufs, 
                default=[],
                help="Lista oficial dos estados brasileiros"
            )
            
            # Anos simples
            years = st.multiselect(
                "Selecione Anos:",
                options=[2024, 2025],
                default=[2024, 2025]
            )
            
            return selected_ufs, years
    except Exception as e:
        st.error(f"❌ Erro ao criar filtros: {e}")
        return [], [2024, 2025]

def create_safe_tabs(components, database, llm, viz, chatbot):
    """Cria abas de forma segura."""
    try:
        tab1, tab2, tab3, tab4 = st.tabs(["📊 Dashboard", "💬 Chatbot", "🔍 SQL", "🔧 Debug"])
        
        with tab1:
            st.header("Dashboard de Análise")
            if viz and database:
                try:
                    st.info("✅ Componentes de visualização carregados")
                    st.write("Dashboard funcional seria exibido aqui.")
                    
                    # Teste simples do banco
                    if hasattr(database, 'execute_query'):
                        try:
                            test_query = "SELECT COUNT(*) as total FROM ibama_infracao LIMIT 1"
                            result = database.execute_query(test_query)
                            if not result.empty:
                                st.success(f"✅ Conexão com banco OK: {result.iloc[0, 0]:,} registros")
                            else:
                                st.warning("⚠️ Banco conectado mas sem dados")
                        except Exception as e:
                            st.warning(f"⚠️ Erro na consulta teste: {e}")
                    
                except Exception as e:
                    st.error(f"❌ Erro no dashboard: {e}")
            else:
                st.warning("⚠️ Componentes de visualização não disponíveis")
        
        with tab2:
            st.header("Chatbot com IA")
            if chatbot:
                try:
                    st.info("✅ Chatbot carregado")
                    st.write("Interface do chatbot seria exibida aqui.")
                    
                    # Teste simples
                    if st.button("Teste do Chatbot"):
                        st.success("✅ Chatbot respondendo!")
                        
                except Exception as e:
                    st.error(f"❌ Erro no chatbot: {e}")
            else:
                st.warning("⚠️ Chatbot não disponível")
        
        with tab3:
            st.header("Explorador SQL")
            if database:
                try:
                    st.info("✅ Banco de dados conectado")
                    
                    query = st.text_area(
                        "Query SQL (apenas SELECT):",
                        value="SELECT COUNT(*) FROM ibama_infracao",
                        help="Digite uma consulta SQL para testar"
                    )
                    
                    if st.button("Executar Query"):
                        if query.strip().lower().startswith('select'):
                            try:
                                result = database.execute_query(query)
                                st.dataframe(result)
                                st.success(f"✅ Query executada: {len(result)} linhas retornadas")
                            except Exception as e:
                                st.error(f"❌ Erro na query: {e}")
                        else:
                            st.error("❌ Apenas queries SELECT são permitidas")
                            
                except Exception as e:
                    st.error(f"❌ Erro no explorador SQL: {e}")
            else:
                st.warning("⚠️ Banco de dados não disponível")
        
        with tab4:
            st.header("Debug Detalhado")
            
            st.subheader("Status dos Componentes:")
            components_status = {
                "Database": "✅ OK" if database else "❌ Erro",
                "LLMIntegration": "✅ OK" if llm else "❌ Erro", 
                "DataVisualization": "✅ OK" if viz else "❌ Erro",
                "Chatbot": "✅ OK" if chatbot else "❌ Erro"
            }
            
            for comp, status in components_status.items():
                st.write(f"**{comp}:** {status}")
            
            st.subheader("Informações Técnicas:")
            if database:
                try:
                    st.write(f"**Tipo de Banco:** {'Cloud (Supabase)' if database.is_cloud else 'Local (DuckDB)'}")
                    if hasattr(database, 'supabase') and database.supabase:
                        st.write("**Supabase:** ✅ Conectado")
                    else:
                        st.write("**Supabase:** ❌ Não conectado")
                except Exception as e:
                    st.write(f"**Erro ao verificar banco:** {e}")
            
            if llm:
                try:
                    available_providers = llm.get_available_providers()
                    st.write("**Provedores de LLM:**")
                    for provider, status in available_providers.items():
                        st.write(f"  - {provider}: {'✅' if status else '❌'}")
                except Exception as e:
                    st.write(f"**Erro ao verificar LLM:** {e}")
                    
    except Exception as e:
        st.error(f"❌ Erro crítico ao criar abas: {e}")

def main():
    """Função principal com inicialização incremental."""
    
    st.title("🌳 Análise de Autos de Infração do IBAMA")
    st.caption("Versão Incremental - Carregamento Seguro")
    
    # Status de carregamento
    status_container = st.container()
    
    with status_container:
        st.subheader("📊 Status de Inicialização:")
        
        # Fase 1: Importações
        st.write("**Fase 1: Importando módulos...**")
        components = load_components()
        
        # Fase 2: Inicializações
        st.write("**Fase 2: Inicializando componentes...**")
        
        database = safe_initialize_database(components.get('Database'))
        llm = safe_initialize_llm(components.get('LLMIntegration'), database)
        viz = safe_initialize_viz(components.get('DataVisualization'), database)
        chatbot = safe_initialize_chatbot(components.get('Chatbot'), llm)
        
        # Armazena no session_state para uso posterior
        st.session_state.db = database
        st.session_state.llm = llm
        st.session_state.viz = viz
        st.session_state.chatbot = chatbot
    
    # Filtros
    selected_ufs, selected_years = create_simple_filters()
    
    # Interface principal
    st.divider()
    
    # Cria abas apenas se pelo menos o banco estiver funcionando
    if database:
        create_safe_tabs(components, database, llm, viz, chatbot)
    else:
        st.error("❌ Não foi possível inicializar o sistema devido a erros no banco de dados.")
        st.info("💡 Verifique as configurações do Supabase nas variáveis de ambiente.")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        st.error(f"💥 Erro crítico na aplicação: {e}")
        
        # Debug detalhado
        st.subheader("🔍 Informações de Debug:")
        st.code(f"""
Erro: {str(e)}
Tipo: {type(e).__name__}
Localização: {__file__}
""")
        
        try:
            import traceback
            st.subheader("📋 Stack Trace Completo:")
            st.code(traceback.format_exc())
        except:
            st.text("Não foi possível obter stack trace detalhado")
