import streamlit as st
import pandas as pd
import os
from datetime import datetime

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

def create_advanced_date_filters():
    """Cria filtros avançados de data por ano e mês."""
    st.subheader("📅 Filtros de Período")
    
    # Opção de filtro simples ou avançado
    filter_mode = st.radio(
        "Tipo de Filtro:", 
        ["Simples (por ano)", "Avançado (por mês)"],
        horizontal=True,
        help="Escolha entre filtro simples por ano ou filtro detalhado por mês"
    )
    
    if filter_mode == "Simples (por ano)":
        return create_simple_year_filter()
    else:
        return create_advanced_month_filter()

def create_simple_year_filter():
    """Cria filtro simples por anos."""
    st.write("**Selecione os anos:**")
    
    # Checkboxes para anos disponíveis
    col1, col2 = st.columns(2)
    
    with col1:
        year_2024 = st.checkbox("2024", value=True, key="year_2024")
    with col2:
        year_2025 = st.checkbox("2025", value=True, key="year_2025")
    
    # Valida seleção
    selected_years = []
    if year_2024:
        selected_years.append(2024)
    if year_2025:
        selected_years.append(2025)
    
    if not selected_years:
        st.warning("⚠️ Selecione pelo menos um ano!")
        selected_years = [2024, 2025]  # Default
    
    # Retorna filtros no formato esperado pelo sistema
    return {
        "mode": "simple",
        "years": selected_years,
        "year_range": (min(selected_years), max(selected_years)),
        "date_filter_sql": create_year_sql_filter(selected_years),
        "description": f"Anos: {', '.join(map(str, selected_years))}"
    }

def create_advanced_month_filter():
    """Cria filtro avançado por meses."""
    st.write("**Selecione períodos específicos:**")
    
    # Dicionário para armazenar seleções
    selected_periods = {}
    
    # Filtros para 2024
    with st.expander("📅 2024", expanded=True):
        col1, col2 = st.columns([1, 3])
        
        with col1:
            include_2024 = st.checkbox("Incluir 2024", value=True, key="include_2024")
        
        with col2:
            if include_2024:
                months_2024 = st.multiselect(
                    "Meses de 2024:",
                    options=list(range(1, 13)),
                    default=list(range(1, 13)),
                    format_func=lambda x: [
                        "Janeiro", "Fevereiro", "Março", "Abril", "Maio", "Junho",
                        "Julho", "Agosto", "Setembro", "Outubro", "Novembro", "Dezembro"
                    ][x-1],
                    key="months_2024"
                )
                if months_2024:
                    selected_periods[2024] = months_2024
    
    # Filtros para 2025
    with st.expander("📅 2025", expanded=True):
        col1, col2 = st.columns([1, 3])
        
        with col1:
            include_2025 = st.checkbox("Incluir 2025", value=True, key="include_2025")
        
        with col2:
            if include_2025:
                # Para 2025, limita aos meses já passados (assumindo que estamos em 2025)
                current_month = datetime.now().month
                available_months_2025 = list(range(1, min(13, current_month + 2)))  # +1 para incluir mês atual
                
                months_2025 = st.multiselect(
                    "Meses de 2025:",
                    options=available_months_2025,
                    default=available_months_2025,
                    format_func=lambda x: [
                        "Janeiro", "Fevereiro", "Março", "Abril", "Maio", "Junho",
                        "Julho", "Agosto", "Setembro", "Outubro", "Novembro", "Dezembro"
                    ][x-1],
                    key="months_2025"
                )
                if months_2025:
                    selected_periods[2025] = months_2025
    
    # Valida seleção
    if not selected_periods:
        st.warning("⚠️ Selecione pelo menos um período!")
        selected_periods = {2024: list(range(1, 13)), 2025: list(range(1, 8))}  # Default
    
    # Retorna filtros
    return {
        "mode": "advanced",
        "periods": selected_periods,
        "year_range": (min(selected_periods.keys()), max(selected_periods.keys())),
        "date_filter_sql": create_month_sql_filter(selected_periods),
        "description": format_period_description(selected_periods)
    }

def create_year_sql_filter(selected_years):
    """Cria filtro SQL para anos selecionados."""
    if len(selected_years) == 1:
        return f"EXTRACT(YEAR FROM TO_TIMESTAMP(DAT_HORA_AUTO_INFRACAO, 'YYYY-MM-DD HH24:MI:SS')) = {selected_years[0]}"
    else:
        years_str = ', '.join(map(str, selected_years))
        return f"EXTRACT(YEAR FROM TO_TIMESTAMP(DAT_HORA_AUTO_INFRACAO, 'YYYY-MM-DD HH24:MI:SS')) IN ({years_str})"

def create_month_sql_filter(selected_periods):
    """Cria filtro SQL para períodos específicos por mês."""
    conditions = []
    
    for year, months in selected_periods.items():
        if len(months) == 12:
            # Se todos os meses estão selecionados, filtra apenas por ano
            conditions.append(f"EXTRACT(YEAR FROM TO_TIMESTAMP(DAT_HORA_AUTO_INFRACAO, 'YYYY-MM-DD HH24:MI:SS')) = {year}")
        else:
            # Filtra por ano e meses específicos
            months_str = ', '.join(map(str, months))
            conditions.append(f"""(
                EXTRACT(YEAR FROM TO_TIMESTAMP(DAT_HORA_AUTO_INFRACAO, 'YYYY-MM-DD HH24:MI:SS')) = {year} 
                AND EXTRACT(MONTH FROM TO_TIMESTAMP(DAT_HORA_AUTO_INFRACAO, 'YYYY-MM-DD HH24:MI:SS')) IN ({months_str})
            )""")
    
    return ' OR '.join(conditions)

def format_period_description(selected_periods):
    """Formata descrição dos períodos selecionados."""
    descriptions = []
    month_names = [
        "Jan", "Fev", "Mar", "Abr", "Mai", "Jun",
        "Jul", "Ago", "Set", "Out", "Nov", "Dez"
    ]
    
    for year, months in selected_periods.items():
        if len(months) == 12:
            descriptions.append(f"{year} (todos os meses)")
        elif len(months) > 6:
            descriptions.append(f"{year} (quase todos os meses)")
        else:
            month_list = [month_names[m-1] for m in months]
            descriptions.append(f"{year} ({', '.join(month_list)})")
    
    return "; ".join(descriptions)

def apply_date_filter_to_dataframe(df, date_filters):
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

    # Sidebar com filtros melhorados
    with st.sidebar:
        st.header("🔎 Filtros do Dashboard")

        try:
            # Filtros UF - método existente
            with st.spinner("Carregando estados..."):
                ufs_list, source_info = get_ufs_from_database(st.session_state.db)
                
                # Feedback visual mais preciso
                if "base completa" in source_info:
                    st.success(source_info)
                elif "base de dados" in source_info or "amostra" in source_info:
                    st.info(source_info)
                else:
                    st.info(source_info)
            
            selected_ufs = st.multiselect(
                "Selecione o Estado (UF)", 
                options=ufs_list, 
                default=[],
                help=f"Estados disponíveis: {len(ufs_list)}"
            )

            st.divider()
            
            # Novos filtros de data avançados
            date_filters = create_advanced_date_filters()
            
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
            
            date_filters = {
                "mode": "simple",
                "years": [2024, 2025],
                "year_range": (2024, 2025),
                "description": "2024, 2025 (padrão)"
            }

        # Info sobre filtros aplicados
        st.divider()
        st.info(f"**Período selecionado:** {date_filters['description']}")
        
        if selected_ufs:
            st.info(f"**Estados:** {', '.join(selected_ufs)}")
        else:
            st.info("**Estados:** Todos")

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
            # Passa os novos filtros para as visualizações
            st.session_state.viz.create_overview_metrics_advanced(selected_ufs, date_filters)
            st.divider()
            st.session_state.viz.create_infraction_map_advanced(selected_ufs, date_filters)
            st.divider()
            
            col1, col2 = st.columns(2)
            with col1:
                st.session_state.viz.create_municipality_hotspots_chart_advanced(selected_ufs, date_filters)
                st.session_state.viz.create_fine_value_by_type_chart_advanced(selected_ufs, date_filters)
                st.session_state.viz.create_gravity_distribution_chart_advanced(selected_ufs, date_filters)
            with col2:
                st.session_state.viz.create_state_distribution_chart_advanced(selected_ufs, date_filters)
                st.session_state.viz.create_infraction_status_chart_advanced(selected_ufs, date_filters)
                st.session_state.viz.create_main_offenders_chart_advanced(selected_ufs, date_filters)
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
