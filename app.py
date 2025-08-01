import streamlit as st
import pandas as pd
import os
from datetime import datetime

# Configuração otimizada para reduzir uso de recursos
st.set_page_config(
    page_title="Análise de Infrações IBAMA (versão beta)", 
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

# Adicione esta função no app.py para criar uma página de diagnóstico completa

def create_diagnostic_page():
    """Cria página completa de diagnóstico integrada no Streamlit."""
    st.header("🔧 Diagnóstico Completo do Sistema")
    st.caption("Ferramenta avançada para debug e verificação de integridade dos dados")
    
    # Status geral
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if st.button("🔍 Teste Completo", type="primary"):
            run_complete_diagnostic()
    
    with col2:
        if st.button("📊 Contagem Real"):
            test_real_count()
    
    with col3:
        if st.button("🧹 Reset Total"):
            reset_all_caches()
    
    st.divider()
    
    # Seção de logs em tempo real
    if st.checkbox("📝 Mostrar Logs Detalhados"):
        st.subheader("📋 Logs do Sistema")
        
        # Container para logs
        log_container = st.empty()
        
        # Captura logs
        if st.button("▶️ Executar Diagnóstico com Logs"):
            run_diagnostic_with_logs(log_container)

# Função de diagnóstico CORRIGIDA para o app.py

def run_complete_diagnostic():
    """Executa diagnóstico completo dentro do Streamlit - CORRIGIDO."""
    try:
        st.subheader("🔍 Executando Diagnóstico Completo...")
        
        # Progresso
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        # Teste 1: Conexão
        status_text.text("1/6 - Testando conexão com Supabase...")
        progress_bar.progress(15)
        
        if not st.session_state.db.is_cloud or not st.session_state.db.supabase:
            st.error("❌ Não conectado ao Supabase")
            return
        
        st.success("✅ Conexão com Supabase OK")
        
        # Teste 2: Verificação do paginador
        status_text.text("2/6 - Verificando paginador...")
        progress_bar.progress(30)
        
        if not hasattr(st.session_state.viz, 'paginator') or not st.session_state.viz.paginator:
            st.error("❌ Paginador não inicializado")
            return
        
        st.success("✅ Paginador inicializado")
        
        # Teste 3: Contagem real no banco
        status_text.text("3/6 - Verificando contagem real no banco...")
        progress_bar.progress(50)
        
        real_counts = st.session_state.viz.paginator.get_real_count()
        
        if 'error' in real_counts:
            st.error(f"❌ Erro na contagem: {real_counts['error']}")
            return
        
        st.success(f"✅ Contagem real obtida: {real_counts['unique_infractions']:,} infrações únicas")
        
        # Teste 4: Limpeza de cache e nova busca
        status_text.text("4/6 - Limpando cache e iniciando nova busca...")
        progress_bar.progress(65)
        
        # Força limpeza completa do cache desta sessão
        st.session_state.viz.paginator.clear_cache()
        
        # Teste 5: Paginação completa
        status_text.text("5/6 - Testando paginação completa...")
        progress_bar.progress(80)
        
        # Gera nova chave de cache para esta sessão
        import time, random
        new_cache_key = f"diagnostic_{time.time()}_{random.randint(1000, 9999)}"
        
        df_paginated = st.session_state.viz.paginator.get_all_records('ibama_infracao', new_cache_key)
        
        # Teste 6: Análise dos dados carregados
        status_text.text("6/6 - Analisando dados carregados...")
        progress_bar.progress(100)
        
        paginated_count = len(df_paginated)
        paginated_unique = df_paginated['NUM_AUTO_INFRACAO'].nunique() if 'NUM_AUTO_INFRACAO' in df_paginated.columns else 0
        
        # Resultados
        st.subheader("📊 Resultados do Diagnóstico")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.metric("📊 Total no Banco", f"{real_counts['total_records']:,}")
            st.metric("🔢 Únicos no Banco", f"{real_counts['unique_infractions']:,}")
            st.metric("📉 Duplicatas no Banco", f"{real_counts.get('duplicates', 0):,}")
        
        with col2:
            st.metric("📊 Paginação Total", f"{paginated_count:,}")
            st.metric("🔢 Paginação Únicos", f"{paginated_unique:,}")
            
            # Calcula diferença
            if paginated_unique > 0:
                diff = real_counts['unique_infractions'] - paginated_unique
                st.metric("🔄 Diferença", f"{diff:,}", delta=f"{diff:,}")
        
        # Análise de status
        st.subheader("🎯 Análise de Status")
        
        # Verifica se dados estão corretos
        expected_total = 21030
        expected_unique = 21019
        
        # Status do banco
        if real_counts['total_records'] == expected_total and real_counts['unique_infractions'] == expected_unique:
            st.success("✅ **DADOS DO BANCO CORRETOS**")
            st.success(f"✅ {expected_total:,} registros, {expected_unique:,} únicos conforme esperado")
        else:
            st.error("❌ **DADOS DO BANCO INCORRETOS**")
            st.error(f"❌ Esperado: {expected_total:,}/{expected_unique:,}, Atual: {real_counts['total_records']:,}/{real_counts['unique_infractions']:,}")
        
        # Status da paginação
        if paginated_unique >= expected_unique * 0.95:  # Aceita 95% como sucesso
            st.success("✅ **PAGINAÇÃO FUNCIONANDO CORRETAMENTE**")
            st.success(f"✅ Dashboard deve mostrar {paginated_unique:,} infrações ({(paginated_unique/expected_unique)*100:.1f}% dos dados)")
        elif paginated_unique == 0:
            st.error("❌ **PAGINAÇÃO FALHOU COMPLETAMENTE**")
            st.error("❌ Nenhum dado foi carregado pela paginação")
        elif paginated_unique >= expected_unique * 0.80:  # Entre 80-95%
            st.warning("⚠️ **PAGINAÇÃO QUASE COMPLETA**")
            st.warning(f"⚠️ Carregou {paginated_unique:,} de {expected_unique:,} infrações ({(paginated_unique/expected_unique)*100:.1f}%)")
        else:
            st.error("❌ **PAGINAÇÃO PARCIAL**")
            st.error(f"❌ Carregou apenas {paginated_unique:,} de {expected_unique:,} infrações ({(paginated_unique/expected_unique)*100:.1f}%)")
            
            # Sugere soluções
            st.subheader("🔧 Soluções Sugeridas")
            
            st.info("💡 **Possíveis causas e soluções:**")
            st.write("• **Limite de paginação muito baixo** → Aumentar max_pages no SupabasePaginator")
            st.write("• **Timeout na conexão** → Verificar conexão de rede")
            st.write("• **Cache corrompido** → Usar botão 'Reset Total' abaixo")
            st.write("• **Problema no Supabase** → Verificar configurações da API")
            
            if st.button("🚀 Tentar Correção Automática"):
                fix_pagination_issues()
        
        # Informações da sessão
        st.subheader("🔒 Informações da Sessão")
        session_uuid = st.session_state.get('session_uuid', 'Não definido')
        st.info(f"**ID da Sessão:** {session_uuid}")
        st.info("**Isolamento:** Cada usuário tem seus próprios dados em cache")
        
        # Timestamp
        st.caption(f"⏰ Diagnóstico executado em: {real_counts['timestamp']}")
        
    except Exception as e:
        st.error(f"❌ Erro no diagnóstico: {e}")
        st.code(str(e), language="python")

def test_real_count():
    """Testa apenas a contagem real do banco - CORRIGIDO."""
    try:
        st.subheader("📊 Testando Contagem Real")
        
        with st.spinner("Verificando dados no banco..."):
            real_counts = st.session_state.viz.paginator.get_real_count()
        
        if 'error' in real_counts:
            st.error(f"❌ {real_counts['error']}")
            return
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric("Total Registros", f"{real_counts['total_records']:,}")
        
        with col2:
            st.metric("Infrações Únicas", f"{real_counts['unique_infractions']:,}")
        
        with col3:
            duplicates = real_counts.get('duplicates', 0)
            st.metric("Duplicatas", f"{duplicates:,}")
        
        # Verifica se está correto
        expected_total = 21030
        expected_unique = 21019
        
        if real_counts['total_records'] == expected_total and real_counts['unique_infractions'] == expected_unique:
            st.success("✅ **Dados do banco estão CORRETOS!**")
            st.success(f"✅ {expected_total:,} total, {expected_unique:,} únicos conforme esperado")
        else:
            st.error("❌ **Dados do banco estão INCORRETOS!**")
            st.error(f"❌ Esperado: {expected_total:,} total, {expected_unique:,} únicos")
            st.error(f"❌ Atual: {real_counts['total_records']:,} total, {real_counts['unique_infractions']:,} únicos")
            
    except Exception as e:
        st.error(f"❌ Erro: {e}")

def reset_all_caches():
    """Reset completo de todos os caches - CORRIGIDO."""
    try:
        st.subheader("🧹 Reset Completo de Caches")
        
        with st.spinner("Limpando todos os caches..."):
            # Limpa cache do paginador desta sessão
            if hasattr(st.session_state.viz, 'paginator') and st.session_state.viz.paginator:
                st.session_state.viz.paginator.clear_cache()
            
            # Limpa cache do Streamlit (global)
            st.cache_data.clear()
            st.cache_resource.clear()
            
            # Remove componentes da sessão e força reinicialização
            components_to_remove = ['viz', 'chatbot', 'session_uuid']
            for component in components_to_remove:
                if component in st.session_state:
                    del st.session_state[component]
        
        st.success("✅ **Todos os caches foram limpos!**")
        st.success("✅ Componentes da sessão foram resetados")
        st.info("💡 **Próximos passos:**")
        st.info("1. Recarregue a página (F5) para reinicializar completamente")
        st.info("2. Ou use o botão abaixo para recarregar o sistema automaticamente")
        
        # Botão para recarregar
        if st.button("🔄 Recarregar Sistema Automaticamente"):
            st.rerun()
            
    except Exception as e:
        st.error(f"❌ Erro no reset: {e}")

def fix_pagination_issues():
    """Tenta corrigir problemas de paginação automaticamente - CORRIGIDO."""
    try:
        st.subheader("🔧 Aplicando Correções Automáticas")
        
        progress = st.progress(0)
        
        # Correção 1: Aumenta limite de páginas
        progress.progress(25)
        st.write("1/4 - Ajustando configurações de paginação...")
        
        if hasattr(st.session_state.viz.paginator, 'max_pages'):
            # Aumenta para 30 páginas (30k registros)
            st.session_state.viz.paginator.max_pages = 30
            st.success("✅ Limite de páginas aumentado para 30 (30k registros)")
        
        # Correção 2: Limpa cache específico desta sessão
        progress.progress(50)
        st.write("2/4 - Limpando cache específico desta sessão...")
        st.session_state.viz.paginator.clear_cache()
        st.success("✅ Cache desta sessão limpo")
        
        # Correção 3: Gera novo ID de sessão
        progress.progress(75)
        st.write("3/4 - Gerando novo ID de sessão...")
        import uuid
        st.session_state.session_uuid = str(uuid.uuid4())[:8]
        st.success(f"✅ Novo ID de sessão: {st.session_state.session_uuid}")
        
        # Correção 4: Testa nova busca
        progress.progress(100)
        st.write("4/4 - Testando nova busca com configurações corrigidas...")
        
        import time, random
        test_key = f"fix_test_{time.time()}_{random.randint(1000, 9999)}"
        
        # Busca uma amostra para testar
        test_df = st.session_state.viz.paginator.get_sample_data(2000)
        
        if not test_df.empty:
            test_unique = test_df['NUM_AUTO_INFRACAO'].nunique() if 'NUM_AUTO_INFRACAO' in test_df.columns else 0
            st.success(f"✅ Teste OK: {len(test_df)} registros, {test_unique} únicos")
            
            if test_unique > 1500:  # Se conseguiu uma boa amostra
                st.success("🎉 **Correção aplicada com sucesso!**")
                st.info("💡 **Próximos passos:**")
                st.info("1. Recarregue a página para aplicar as correções")
                st.info("2. Execute novo diagnóstico para verificar")
                
                if st.button("🔄 Recarregar Página Automaticamente"):
                    st.rerun()
            else:
                st.warning("⚠️ Correção parcial - ainda há problemas na paginação")
        else:
            st.error("❌ Correção falhou - problema persiste")
            st.error("💡 **Possíveis próximos passos:**")
            st.error("• Verificar conexão com internet")
            st.error("• Verificar configurações do Supabase")
            st.error("• Contactar suporte técnico")
        
    except Exception as e:
        st.error(f"❌ Erro na correção: {e}")

def create_diagnostic_page():
    """Cria página completa de diagnóstico integrada no Streamlit - CORRIGIDA."""
    st.header("🔧 Diagnóstico Completo do Sistema")
    st.caption("Ferramenta avançada para debug e verificação de integridade dos dados por sessão")
    
    # Status geral
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if st.button("🔍 Teste Completo", type="primary"):
            run_complete_diagnostic()
    
    with col2:
        if st.button("📊 Contagem Real"):
            test_real_count()
    
    with col3:
        if st.button("🧹 Reset Total"):
            reset_all_caches()
    
    st.divider()
    
    # Informações da sessão atual
    st.subheader("🔒 Informações desta Sessão")
    
    session_uuid = st.session_state.get('session_uuid', 'Não definido')
    col1, col2 = st.columns(2)
    
    with col1:
        st.info(f"**ID da Sessão:** {session_uuid}")
        st.info(f"**Paginador:** {'✅ Ativo' if hasattr(st.session_state, 'viz') and hasattr(st.session_state.viz, 'paginator') else '❌ Inativo'}")
    
    with col2:
        # Conta dados em cache desta sessão
        cached_keys = 0
        if 'session_uuid' in st.session_state:
            session_uuid = st.session_state.session_uuid
            for key in st.session_state.keys():
                if key.startswith(f'paginated_data_data_{session_uuid}'):
                    cached_keys += 1
        
        st.info(f"**Dados em Cache:** {cached_keys} conjuntos")
        st.info(f"**Isolamento:** ✅ Dados isolados por sessão")
    
    # Diagnóstico avançado da sessão
    if hasattr(st.session_state, 'viz'):
        st.session_state.viz.display_session_diagnostic()
    
    st.divider()
    
    # Seção de logs em tempo real
    if st.checkbox("📝 Mostrar Logs Detalhados"):
        st.subheader("📋 Logs do Sistema")
        
        # Container para logs
        log_container = st.empty()
        
        # Captura logs
        if st.button("▶️ Executar Diagnóstico com Logs"):
            run_diagnostic_with_logs(log_container)
    
    # Informações técnicas
    st.divider()
    st.subheader("ℹ️ Informações Técnicas")
    
    st.info("""
    **Como funciona o isolamento por sessão:**
    - Cada usuário recebe um UUID único de sessão
    - Os dados são armazenados em cache isolado por sessão
    - Não há interferência entre diferentes usuários
    - Cache é limpo automaticamente ao sair da sessão
    """)
    
    st.warning("""
    **Valores esperados corretos:**
    - **Total no banco:** 21.030 registros
    - **Infrações únicas:** 21.019 (sem duplicatas)
    - **Duplicatas:** 11 registros
    """)

def run_diagnostic_with_logs(log_container):
    """Executa diagnóstico com logs em tempo real - CORRIGIDO."""
    import sys
    from io import StringIO
    
    # Captura logs
    old_stdout = sys.stdout
    sys.stdout = captured_output = StringIO()
    
    try:
        # Executa diagnóstico
        real_counts = st.session_state.viz.paginator.get_real_count()
        df = st.session_state.viz.paginator.get_all_records()
        
        # Restaura stdout
        sys.stdout = old_stdout
        
        # Mostra logs capturados
        logs = captured_output.getvalue()
        if logs:
            log_container.code(logs, language="text")
        else:
            log_container.info("Nenhum log capturado - executando em modo silencioso")
            
    except Exception as e:
        sys.stdout = old_stdout
        log_container.error(f"Erro: {e}")
        log_container.code(str(e), language="python")

def main():
    st.title("🌳 Análise de Autos de Infração do IBAMA (versão beta)")
    
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
        
        # ======================== SEÇÃO DE DIAGNÓSTICO ========================
        st.subheader("🔧 Diagnóstico")
        
        if st.button("🔍 Verificar Dados Reais", help="Verifica contagem real no banco de dados"):
            if st.session_state.db.is_cloud and hasattr(st.session_state, 'viz') and st.session_state.viz.paginator:
                try:
                    with st.spinner("Verificando dados no banco..."):
                        real_counts = st.session_state.viz.paginator.get_real_count()
                        
                        col1, col2 = st.columns(2)
                        with col1:
                            st.success(f"✅ **Total no banco:** {real_counts['total_records']:,}")
                        with col2:
                            st.success(f"✅ **Infrações únicas:** {real_counts['unique_infractions']:,}")
                        
                        st.caption(f"⏰ Verificado em: {real_counts['timestamp']}")
                        
                        # Verifica consistência
                        if real_counts['total_records'] != real_counts['unique_infractions']:
                            difference = real_counts['total_records'] - real_counts['unique_infractions']
                            st.warning(f"⚠️ **{difference:,} registros duplicados** detectados no banco")
                        else:
                            st.info("ℹ️ Todos os registros são únicos no banco")
                            
                except Exception as e:
                    st.error(f"❌ Erro na verificação: {str(e)}")
            else:
                st.warning("⚠️ Diagnóstico disponível apenas para modo cloud com Supabase")
        
        if st.button("🧹 Limpar Cache da Sessão", help="Remove cache local desta sessão"):
            try:
                # Limpa cache do visualization
                if hasattr(st.session_state, 'viz') and st.session_state.viz.paginator:
                    st.session_state.viz.paginator.clear_cache()
                
                # Limpa cache do Streamlit
                st.cache_data.clear()
                st.cache_resource.clear()
                
                # Remove dados da sessão
                session_keys_to_remove = ['viz', 'chatbot']
                for key in session_keys_to_remove:
                    if key in st.session_state:
                        del st.session_state[key]
                
                st.success("✅ Cache limpo! Recarregue a página para ver os dados atualizados.")
                st.info("💡 **Dica:** Use F5 ou Ctrl+R para recarregar completamente")
                
            except Exception as e:
                st.error(f"❌ Erro ao limpar cache: {str(e)}")
        
        # Informações sobre qualidade dos dados
        if st.button("📊 Qualidade dos Dados", help="Exibe informações detalhadas sobre os dados carregados"):
            if hasattr(st.session_state, 'viz'):
                try:
                    st.session_state.viz.display_data_quality_info(selected_ufs, date_filters)
                except Exception as e:
                    st.error(f"❌ Erro ao obter informações de qualidade: {str(e)}")
            else:
                st.warning("⚠️ Componente de visualização não inicializado")

        st.divider()
        
        # ======================== FILTROS DO LLM ========================
        st.subheader("🤖 Configurações de IA")
        
        # Seleção do provedor de LLM
        llm_provider = st.selectbox(
            "Modelo de IA:",
            options=["groq", "gemini"],
            index=0,
            format_func=lambda x: {
                "groq": "🦙 Llama 3.1 70B (Groq) - Rápido",
                "gemini": "💎 Gemini 1.5 Pro (Google) - Avançado"
            }.get(x, x),
            help="Escolha o modelo de IA para geração de SQL e análises"
        )
        
        # Recomendações de uso
        st.info("💡 **Recomendação:** É recomendado usar o Llama para perguntas simples no Chatbot. Para mais análise e perguntas complexas selecione Gemini 1.5 Pro")
        
        # Configurações avançadas do LLM (opcional)
        with st.expander("⚙️ Configurações Avançadas"):
            temperature = st.slider(
                "Criatividade (Temperature):",
                min_value=0.0,
                max_value=1.0,
                value=0.0,
                step=0.1,
                help="0 = Mais preciso e determinístico, 1 = Mais criativo"
            )
            
            max_tokens = st.slider(
                "Máximo de Tokens:",
                min_value=100,
                max_value=2000,
                value=500,
                step=100,
                help="Limite de tokens para as respostas do LLM"
            )
            
            # Informações sobre os modelos
            if llm_provider == "groq":
                st.info("🦙 **Llama 3.1 70B:** Modelo open-source rápido e eficiente para análise de dados. Ideal para perguntas diretas e consultas simples.")
            else:
                st.info("💎 **Gemini 1.5 Pro:** Modelo avançado do Google com melhor compreensão de contexto. Recomendado para análises complexas e textos elaborados.")
        
        # Status das APIs
        st.subheader("📡 Status das APIs")
        
        # Verifica status do Groq
        groq_status = "✅ Conectado" if st.session_state.llm.groq_client else "❌ Não configurado"
        st.write(f"**Groq API:** {groq_status}")
        
        # Verifica status do Gemini
        gemini_status = "✅ Conectado" if st.session_state.llm.gemini_model else "❌ Não configurado"
        st.write(f"**Gemini API:** {gemini_status}")
        
        # Aviso se nenhuma API estiver disponível
        if not st.session_state.llm.groq_client and not st.session_state.llm.gemini_model:
            st.error("⚠️ Nenhuma API de IA configurada! O chatbot funcionará em modo limitado.")
        
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
            st.error("**Modelos de IA podem ter erros, alucinações, vieses ou problemas éticos.** Sempre verifique as respostas!")

        with st.expander("ℹ️ Sobre este App"):
            st.markdown("""
                **Fonte:** [Portal de Dados Abertos do IBAMA](https://dadosabertos.ibama.gov.br/dataset/fiscalizacao-auto-de-infracao)
                
                **Desenvolvido por:** Reinaldo Chaves - [GitHub](https://github.com/reichaves/ibama_dashboard)

                **E-mail:** reichaves@gmail.com
            """)

    # Abas principais
    tab1, tab2, tab3, tab4 = st.tabs(["📊 Dashboard Interativo", "💬 Chatbot com IA", "🔍 Explorador SQL", "🔧 Diagnóstico"])
    
    with tab1:
        st.header("Dashboard de Análise de Infrações Ambientais")
        st.caption("Use os filtros na barra lateral para explorar os dados. Sem repetição do NUM_AUTO_INFRACAO")
        
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
            # Passa as configurações do LLM para o chatbot
            if hasattr(st.session_state.chatbot, 'set_llm_config'):
                st.session_state.chatbot.set_llm_config(
                    provider=llm_provider,
                    temperature=temperature,
                    max_tokens=max_tokens
                )
            
            st.session_state.chatbot.display_chat_interface()
        except Exception as e:
            st.error(f"Erro no chatbot: {e}")

    with tab4:
        create_diagnostic_page()
        
    with tab3:
        st.header("Explorador de Dados SQL")
        
        # Opção de usar IA para gerar SQL
        col1, col2 = st.columns([3, 1])
        
        with col1:
            query_mode = st.radio(
                "Modo de consulta:",
                ["Manual", "Gerar com IA"],
                horizontal=True,
                help="Escolha entre escrever SQL manualmente ou gerar com IA"
            )
        
        with col2:
            if query_mode == "Gerar com IA":
                st.write(f"🤖 Usando: {llm_provider}")
        
        if query_mode == "Manual":
            # Modo manual tradicional
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
        
        else:
            # Modo IA
            st.subheader("🤖 Geração Inteligente de SQL")
            
            # Input em linguagem natural
            natural_query = st.text_area(
                "Descreva o que você quer analisar:",
                placeholder="Ex: Quais são os 10 estados com mais infrações em 2024?",
                height=100
            )
            
            col1, col2 = st.columns([1, 1])
            
            with col1:
                if st.button("🔮 Gerar SQL", type="primary"):
                    if natural_query.strip():
                        try:
                            with st.spinner(f"🤖 {llm_provider.title()} gerando SQL..."):
                                # Gera SQL usando o LLM selecionado
                                generated_sql = st.session_state.llm.generate_sql(
                                    natural_query, 
                                    llm_provider,
                                    temperature,
                                    max_tokens
                                )
                                
                                # Exibe o SQL gerado
                                st.subheader("SQL Gerado:")
                                st.code(generated_sql, language="sql")
                                
                                # Armazena no session state para execução
                                st.session_state.generated_sql = generated_sql
                                
                        except Exception as e:
                            st.error(f"Erro ao gerar SQL: {e}")
                    else:
                        st.warning("Digite uma descrição da análise desejada.")
            
            with col2:
                if st.button("▶️ Executar SQL Gerado") and hasattr(st.session_state, 'generated_sql'):
                    try:
                        with st.spinner("Executando consulta..."):
                            df = st.session_state.db.execute_query(st.session_state.generated_sql)
                            
                            st.subheader("Resultados:")
                            st.dataframe(df)
                            
                            # Análise automática dos resultados
                            if not df.empty:
                                st.subheader("📊 Análise Automática:")
                                analysis_prompt = f"Analise estes resultados da consulta '{natural_query}': {df.head().to_string()}"
                                
                                try:
                                    analysis = st.session_state.llm.generate_analysis(
                                        analysis_prompt, 
                                        llm_provider,
                                        temperature,
                                        max_tokens
                                    )
                                    st.write(analysis)
                                except:
                                    st.info("Análise automática não disponível.")
                    
                    except Exception as e:
                        st.error(f"Erro na execução: {e}")
            
            # Exemplos de consultas
            st.subheader("💡 Exemplos de Consultas:")
            examples = [
                "Top 5 estados com mais infrações",
                "Valor total de multas por tipo de infração",
                "Infrações por mês em 2024",
                "Principais infratores por valor de multa",
                "Distribuição de infrações por gravidade"
            ]
            
            for example in examples:
                if st.button(f"📝 {example}", key=f"example_{hash(example)}"):
                    st.session_state.example_query = example
                    st.rerun()

if __name__ == "__main__":
    main()
