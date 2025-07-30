import streamlit as st
import pandas as pd
import os

# Configuração básica
st.set_page_config(
    page_title="Análise de Infrações IBAMA", 
    page_icon="🌳", 
    layout="wide",
    initial_sidebar_state="expanded"
)

def test_basic_functionality():
    """Testa funcionalidades básicas do Streamlit."""
    st.write("✅ Streamlit está funcionando!")
    st.write("✅ Pandas está funcionando!")
    
    # Teste de dados simples
    test_df = pd.DataFrame({
        'Estado': ['SP', 'RJ', 'MG'],
        'Infrações': [100, 80, 60]
    })
    st.dataframe(test_df)

def safe_import_components():
    """Importa componentes de forma segura."""
    try:
        from src.utils.database import Database
        st.success("✅ Database importado com sucesso")
        return Database, None, None, None
    except ImportError as e:
        st.error(f"❌ Erro ao importar Database: {e}")
        return None, None, None, None
    except Exception as e:
        st.error(f"❌ Erro inesperado: {e}")
        return None, None, None, None

def main():
    """Função principal simplificada para debug."""
    
    st.title("🌳 Análise de Autos de Infração do IBAMA")
    st.write("**Versão de Debug - Testando Funcionalidades**")
    
    # Teste 1: Funcionalidades básicas
    st.header("🔍 Teste 1: Funcionalidades Básicas")
    try:
        test_basic_functionality()
    except Exception as e:
        st.error(f"❌ Erro nas funcionalidades básicas: {e}")
    
    # Teste 2: Importações
    st.header("🔍 Teste 2: Importações de Módulos")
    try:
        Database, _, _, _ = safe_import_components()
        
        if Database:
            st.success("✅ Componentes importados com sucesso")
        else:
            st.warning("⚠️ Alguns componentes não puderam ser importados")
            
    except Exception as e:
        st.error(f"❌ Erro na importação: {e}")
    
    # Teste 3: Variáveis de ambiente
    st.header("🔍 Teste 3: Variáveis de Ambiente")
    try:
        # Verifica se as principais variáveis estão definidas
        env_vars = [
            'GROQ_API_KEY',
            'GOOGLE_API_KEY', 
            'SUPABASE_URL',
            'SUPABASE_KEY'
        ]
        
        for var in env_vars:
            value = os.getenv(var)
            if value:
                st.success(f"✅ {var}: {'*' * 20} (definida)")
            else:
                # Tenta buscar em st.secrets
                try:
                    secret_value = st.secrets.get(var)
                    if secret_value:
                        st.success(f"✅ {var}: {'*' * 20} (em secrets)")
                    else:
                        st.warning(f"⚠️ {var}: não definida")
                except:
                    st.warning(f"⚠️ {var}: não definida")
                    
    except Exception as e:
        st.error(f"❌ Erro ao verificar variáveis: {e}")
    
    # Teste 4: Sidebar básica
    st.header("🔍 Teste 4: Sidebar")
    try:
        with st.sidebar:
            st.header("🔎 Teste de Sidebar")
            st.write("Se você está vendo isso, a sidebar funciona!")
            
            # Filtros básicos
            test_states = ['SP', 'RJ', 'MG', 'RS', 'PR']
            selected_states = st.multiselect(
                "Estados de Teste:",
                options=test_states,
                default=['SP', 'RJ']
            )
            
            st.write(f"Estados selecionados: {selected_states}")
            
    except Exception as e:
        st.error(f"❌ Erro na sidebar: {e}")
    
    # Teste 5: Abas básicas
    st.header("🔍 Teste 5: Sistema de Abas")
    try:
        tab1, tab2, tab3 = st.tabs(["📊 Teste Dashboard", "💬 Teste Chat", "🔧 Teste Debug"])
        
        with tab1:
            st.write("✅ Aba 1 funcionando!")
            st.bar_chart(pd.DataFrame({
                'valores': [1, 2, 3, 4, 5]
            }))
        
        with tab2:
            st.write("✅ Aba 2 funcionando!")
            if st.button("Teste de Botão"):
                st.success("Botão funcionando!")
        
        with tab3:
            st.write("✅ Aba 3 funcionando!")
            st.json({
                "status": "ok",
                "streamlit_version": st.__version__,
                "python_version": "3.11+"
            })
            
    except Exception as e:
        st.error(f"❌ Erro nas abas: {e}")
    
    # Informações do sistema
    st.header("🔍 Informações do Sistema")
    try:
        st.json({
            "streamlit_version": st.__version__,
            "pandas_version": pd.__version__,
            "working_directory": os.getcwd(),
            "python_path": os.sys.path[:3],  # Primeiros 3 caminhos
            "environment": "Streamlit Cloud" if os.getenv("STREAMLIT_SHARING_MODE") else "Local"
        })
    except Exception as e:
        st.error(f"❌ Erro ao obter informações: {e}")
    
    # Status final
    st.success("🎉 Se você está vendo esta mensagem, o Streamlit está funcionando corretamente!")
    st.info("💡 Se todos os testes passaram, o problema pode estar nos módulos específicos do projeto.")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        st.error(f"💥 Erro crítico na aplicação: {e}")
        st.error("🔧 Verifique os logs do Streamlit Cloud para mais detalhes.")
        
        # Informações de debug
        st.subheader("🔍 Debug Info")
        st.text(f"Erro: {str(e)}")
        st.text(f"Tipo: {type(e).__name__}")
        
        try:
            import traceback
            st.text("Stack trace:")
            st.code(traceback.format_exc())
        except:
            st.text("Não foi possível obter stack trace")
