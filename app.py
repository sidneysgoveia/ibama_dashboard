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

# Funções de Diagnóstico Avançado para adicionar ao app.py

def run_deep_analysis_diagnostic():
    """Executa análise profunda dos dados para identificar exatamente onde estão as duplicatas."""
    try:
        st.subheader("🔬 Análise Profunda dos Dados")
        st.caption("Investigação detalhada das duplicatas no banco de dados")
        
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        # Etapa 1: Análise de estrutura
        status_text.text("1/4 - Analisando estrutura do banco...")
        progress_bar.progress(25)
        
        structure_info = st.session_state.viz.paginator.diagnostic_database_structure()
        
        if 'error' in structure_info:
            st.error(f"❌ Erro na análise de estrutura: {structure_info['error']}")
            return
        
        st.success(f"✅ Estrutura analisada: {structure_info['total_columns']} colunas")
        
        # Etapa 2: Análise profunda de duplicatas
        status_text.text("2/4 - Executando análise profunda de duplicatas...")
        progress_bar.progress(50)
        
        deep_analysis = st.session_state.viz.paginator.deep_analysis_duplicates()
        
        if 'error' in deep_analysis:
            st.error(f"❌ Erro na análise profunda: {deep_analysis['error']}")
            return
        
        st.success(f"✅ Análise profunda concluída: {deep_analysis['total_records']:,} registros analisados")
        
        # Etapa 3: Contagem real corrigida
        status_text.text("3/4 - Obtendo contagem real corrigida...")
        progress_bar.progress(75)
        
        real_counts = st.session_state.viz.paginator.get_real_count_fixed()
        
        if 'error' in real_counts:
            st.error(f"❌ Erro na contagem real: {real_counts['error']}")
            return
        
        # Etapa 4: Teste de paginação corrigida
        status_text.text("4/4 - Testando paginação corrigida...")
        progress_bar.progress(100)
        
        # Limpa cache e testa nova paginação
        st.session_state.viz.paginator.clear_cache()
        test_df = st.session_state.viz.paginator.get_all_records_fixed()
        
        # Resultados detalhados
        st.subheader("🔬 Resultados da Análise Profunda")
        
        # Aba 1: Estrutura do Banco
        tab1, tab2, tab3, tab4 = st.tabs(["📋 Estrutura", "🔍 Duplicatas", "📊 Contagens", "🧪 Teste Final"])
        
        with tab1:
            st.write("**Estrutura do Banco de Dados:**")
            col1, col2 = st.columns(2)
            
            with col1:
                st.metric("Total de Colunas", structure_info['total_columns'])
                st.metric("Amostra Analisada", structure_info['sample_size'])
            
            with col2:
                st.write("**NUM_AUTO_INFRACAO:**", "✅ Existe" if structure_info['num_auto_exists'] else "❌ Não existe")
                if structure_info.get('num_auto_formats'):
                    st.write("**Formatos:**", ', '.join(structure_info['num_auto_formats']))
            
            if structure_info.get('sample_num_auto'):
                st.write("**Exemplos de NUM_AUTO_INFRACAO:**")
                for i, num in enumerate(structure_info['sample_num_auto'][:5], 1):
                    st.code(f"{i}. {num}")
        
        with tab2:
            st.write("**Análise de Duplicatas:**")
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.metric("Total de Registros", f"{deep_analysis['total_records']:,}")
                st.metric("Registros Únicos", f"{deep_analysis['unique_num_auto']:,}")
            
            with col2:
                st.metric("Registros Nulos", f"{deep_analysis['null_num_auto']:,}")
                st.metric("Registros Vazios", f"{deep_analysis['empty_num_auto']:,}")
            
            with col3:
                st.metric("NUM_AUTO Duplicados", f"{deep_analysis['duplicated_num_auto']:,}")
                duplicates_total = deep_analysis['total_records'] - deep_analysis['unique_num_auto']
                st.metric("Total de Duplicatas", f"{duplicates_total:,}")
            
            # Mostrar exemplos de duplicatas
            if deep_analysis['most_duplicated']:
                st.write("**🔴 Top 10 NUM_AUTO_INFRACAO Mais Duplicados:**")
                for num_auto, count in list(deep_analysis['most_duplicated'].items())[:10]:
                    st.write(f"• **{num_auto}**: {count} ocorrências")
            
            # Mostrar amostras detalhadas
            if deep_analysis['sample_duplicates']:
                st.write("**📋 Amostras de Registros Duplicados:**")
                for sample in deep_analysis['sample_duplicates'][:3]:
                    with st.expander(f"NUM_AUTO_INFRACAO: {sample['num_auto']} ({sample['count']} ocorrências)"):
                        sample_df = pd.DataFrame(sample['sample_data'])
                        st.dataframe(sample_df)
        
        with tab3:
            st.write("**Contagens Finais:**")
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.metric("📊 Total no Banco", f"{real_counts['total_records']:,}")
                st.metric("🔢 Únicos no Banco", f"{real_counts['unique_infractions']:,}")
                st.metric("📉 Duplicatas no Banco", f"{real_counts['duplicates']:,}")
            
            with col2:
                st.metric("❌ Registros Nulos", f"{real_counts['null_records']:,}")
                st.metric("❌ Registros Vazios", f"{real_counts['empty_records']:,}")
                st.metric("🔄 NUM_AUTO Duplicados", f"{real_counts['duplicated_infractions']:,}")
            
            # Análise de correção
            expected_unique = 21019
            actual_unique = real_counts['unique_infractions']
            
            if actual_unique == expected_unique:
                st.success(f"✅ **CONTAGEM CORRETA**: {actual_unique:,} infrações únicas conforme esperado")
            else:
                difference = expected_unique - actual_unique
                st.error(f"❌ **CONTAGEM INCORRETA**: Faltam {difference:,} infrações únicas")
                st.error(f"❌ Esperado: {expected_unique:,}, Obtido: {actual_unique:,}")
                
                # Possíveis explicações
                st.write("**🔍 Possíveis Explicações:**")
                if real_counts['null_records'] > 0 or real_counts['empty_records'] > 0:
                    st.write(f"• {real_counts['null_records'] + real_counts['empty_records']:,} registros com NUM_AUTO_INFRACAO inválido")
                if real_counts['duplicates'] > 8500:  # Mais duplicatas que esperado
                    st.write(f"• {real_counts['duplicates']:,} duplicatas no banco (muito acima do esperado)")
                st.write("• Possível problema na estrutura dos dados originais")
                st.write("• Possível problema na importação para o Supabase")
        
        with tab4:
            st.write("**Teste de Paginação Corrigida:**")
            
            pag_total = len(test_df)
            pag_unique = test_df['NUM_AUTO_INFRACAO'].nunique() if 'NUM_AUTO_INFRACAO' in test_df.columns else 0
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.metric("📊 Registros Carregados", f"{pag_total:,}")
                st.metric("🔢 Únicos Carregados", f"{pag_unique:,}")
            
            with col2:
                difference = real_counts['unique_infractions'] - pag_unique
                st.metric("🔄 Diferença", f"{difference:,}")
                
                coverage = (pag_unique / real_counts['unique_infractions']) * 100 if real_counts['unique_infractions'] > 0 else 0
                st.metric("📈 Cobertura", f"{coverage:.1f}%")
            
            # Status final
            if pag_unique >= real_counts['unique_infractions'] * 0.98:  # 98% ou mais
                st.success("✅ **PAGINAÇÃO EXCELENTE**: Carregou quase todos os dados únicos")
            elif pag_unique >= real_counts['unique_infractions'] * 0.90:  # 90% ou mais
                st.warning("⚠️ **PAGINAÇÃO BOA**: Carregou a maioria dos dados únicos")
            else:
                st.error("❌ **PAGINAÇÃO INSUFICIENTE**: Muitos dados únicos não foram carregados")
        
        # Conclusões e recomendações
        st.subheader("🎯 Conclusões e Recomendações")
        
        if real_counts['unique_infractions'] < 21000:
            st.error("🚨 **PROBLEMA CRÍTICO IDENTIFICADO**")
            st.error("O banco de dados não contém as 21.019 infrações únicas esperadas")
            
            st.write("**🔧 Possíveis Soluções:**")
            st.write("1. **Verificar dados originais** - O arquivo CSV original pode ter problemas")
            st.write("2. **Verificar processo de upload** - Pode haver perda de dados durante a importação")
            st.write("3. **Re-upload dos dados** - Baixar novamente do IBAMA e fazer novo upload")
            st.write("4. **Verificar filtros no Supabase** - Pode haver filtros ocultos aplicados")
            
            if st.button("🔄 Tentar Re-análise Completa"):
                st.session_state.viz.paginator.clear_cache()
                st.cache_data.clear()
                st.rerun()
        
        else:
            st.success("✅ **DADOS DO BANCO CORRETOS**")
            st.success("O problema está na paginação, não nos dados originais")
            
            if pag_unique < real_counts['unique_infractions'] * 0.95:
                st.write("**🔧 Para Melhorar a Paginação:**")
                st.write("1. Aumentar `max_pages` no SupabasePaginator")
                st.write("2. Verificar timeouts de conexão")
                st.write("3. Otimizar queries do Supabase")
        
        st.caption(f"⏰ Análise executada em: {pd.Timestamp.now()}")
        
    except Exception as e:
        st.error(f"❌ Erro na análise profunda: {e}")
        st.code(str(e), language="python")

def create_diagnostic_page_advanced():
    """Versão avançada da página de diagnóstico."""
    st.header("🔧 Diagnóstico Avançado do Sistema")
    st.caption("Ferramenta de análise profunda para identificar e corrigir problemas de dados")
    
    # Botões principais
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        if st.button("🔬 Análise Profunda", type="primary"):
            run_deep_analysis_diagnostic()
    
    with col2:
        if st.button("🔍 Teste Completo"):
            run_complete_diagnostic()
    
    with col3:
        if st.button("📊 Contagem Real"):
            test_real_count()
    
    with col4:
        if st.button("🧹 Reset Total"):
            reset_all_caches()
    
    st.divider()
    
    # Seção de análise rápida
    st.subheader("⚡ Análise Rápida")
    
    if st.button("📈 Status Atual do Sistema"):
        show_current_system_status()
    
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
    
    # Seção de troubleshooting
    st.subheader("🛠️ Ferramentas de Correção")
    
    col1, col2 = st.columns(2)
    
    with col1:
        if st.button("🔧 Correção Automática"):
            fix_pagination_issues_advanced()
        
        if st.button("🎯 Forçar Recarregamento"):
            force_complete_reload()
    
    with col2:
        if st.button("📋 Relatório Completo"):
            generate_diagnostic_report()
        
        if st.button("🔬 Teste de Integridade"):
            run_integrity_test()
    
    # Informações técnicas
    st.divider()
    st.subheader("ℹ️ Informações Técnicas")
    
    with st.expander("📚 Como Interpretar os Resultados"):
        st.markdown("""
        **📊 Valores Esperados Corretos:**
        - **Total no banco:** 21.030 registros
        - **Infrações únicas:** 21.019 (sem duplicatas por NUM_AUTO_INFRACAO)
        - **Duplicatas:** 11 registros (diferença entre total e únicos)
        
        **🔍 Diagnóstico de Problemas:**
        - **Se únicos < 21.019:** Problema no banco ou na análise
        - **Se paginação < únicos:** Problema na paginação/cache
        - **Se diferença > 0:** Cache desatualizado ou filtros aplicados
        
        **🎯 Status Ideal:**
        - Total no banco: 21.030 ✅
        - Únicos no banco: 21.019 ✅
        - Paginação únicos: 21.019 ✅
        - Diferença: 0 ✅
        """)
    
    with st.expander("🔧 Soluções para Problemas Comuns"):
        st.markdown("""
        **❌ "Únicos no banco: 12.563 (incorreto)"**
        - Problema: Banco não tem todos os dados esperados
        - Solução: Re-upload dos dados originais do IBAMA
        - Ação: Verificar processo de importação
        
        **❌ "Paginação parcial (58.4%)"**
        - Problema: Limite de paginação muito baixo
        - Solução: Aumentar max_pages no código
        - Ação: Usar "Correção Automática"
        
        **❌ "Cache corrompido"**
        - Problema: Dados em cache desatualizados
        - Solução: Limpar cache e recarregar
        - Ação: Usar "Reset Total"
        
        **❌ "Timeout na conexão"**
        - Problema: Conexão lenta com Supabase
        - Solução: Reduzir page_size, aumentar timeout
        - Ação: Verificar conexão de internet
        """)

def show_current_system_status():
    """Mostra status atual rápido do sistema."""
    try:
        st.subheader("📈 Status Atual do Sistema")
        
        with st.spinner("Verificando status..."):
            # Testa conexão básica
            if not st.session_state.db.is_cloud or not st.session_state.db.supabase:
                st.error("❌ Não conectado ao Supabase")
                return
            
            # Teste rápido de contagem
            try:
                result = st.session_state.db.supabase.table('ibama_infracao').select('NUM_AUTO_INFRACAO', count='exact').limit(1).execute()
                total_count = getattr(result, 'count', 0)
            except:
                total_count = 0
            
            # Status dos componentes
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.metric("🔗 Conexão Supabase", "✅ Ativa" if total_count > 0 else "❌ Inativa")
                st.metric("📊 Registros no Banco", f"{total_count:,}" if total_count > 0 else "0")
            
            with col2:
                paginator_status = "✅ Ativo" if hasattr(st.session_state, 'viz') and hasattr(st.session_state.viz, 'paginator') else "❌ Inativo"
                st.metric("🔄 Paginador", paginator_status)
                
                cache_count = 0
                if 'session_uuid' in st.session_state:
                    session_uuid = st.session_state.session_uuid
                    for key in st.session_state.keys():
                        if key.startswith(f'paginated_data_data_{session_uuid}'):
                            cache_count += 1
                st.metric("💾 Cache Ativo", f"{cache_count} conjuntos")
            
            with col3:
                expected_status = "✅ Esperado" if total_count == 21030 else "❌ Incorreto"
                st.metric("🎯 Dados Esperados", expected_status)
                st.metric("🆔 Sessão", st.session_state.get('session_uuid', 'N/A'))
            
            # Avaliação geral
            if total_count == 21030 and paginator_status == "✅ Ativo":
                st.success("🎉 **Sistema funcionando corretamente!**")
            elif total_count == 21030:
                st.warning("⚠️ **Dados corretos, mas paginador pode ter problemas**")
            else:
                st.error("❌ **Sistema com problemas nos dados**")
    
    except Exception as e:
        st.error(f"❌ Erro ao verificar status: {e}")

def fix_pagination_issues_advanced():
    """Versão avançada da correção de problemas de paginação."""
    try:
        st.subheader("🔧 Correção Avançada de Paginação")
        
        progress = st.progress(0)
        status_text = st.empty()
        
        # Etapa 1: Diagnóstico inicial
        status_text.text("1/6 - Diagnosticando problemas atuais...")
        progress.progress(15)
        
        current_issues = []
        
        # Verifica conexão
        if not st.session_state.db.is_cloud or not st.session_state.db.supabase:
            current_issues.append("Conexão Supabase inativa")
        
        # Verifica paginador
        if not hasattr(st.session_state.viz, 'paginator'):
            current_issues.append("Paginador não inicializado")
        
        if current_issues:
            st.error(f"❌ Problemas detectados: {', '.join(current_issues)}")
            return
        
        st.success("✅ Componentes básicos funcionando")
        
        # Etapa 2: Configurações otimizadas
        status_text.text("2/6 - Aplicando configurações otimizadas...")
        progress.progress(30)
        
        # Aumenta limites para garantir captura completa
        st.session_state.viz.paginator.max_pages = 40  # 40k registros
        st.session_state.viz.paginator.page_size = 1000  # Mantém page_size eficiente
        
        st.success("✅ Configurações otimizadas aplicadas")
        
        # Etapa 3: Limpeza completa de cache
        status_text.text("3/6 - Limpeza completa de cache...")
        progress.progress(45)
        
        # Limpa tudo
        st.session_state.viz.paginator.clear_cache()
        st.cache_data.clear()
        
        # Gera novo UUID para forçar cache novo
        st.session_state.session_uuid = str(uuid.uuid4())[:8]
        
        st.success(f"✅ Cache limpo - Nova sessão: {st.session_state.session_uuid}")
        
        # Etapa 4: Teste de conectividade
        status_text.text("4/6 - Testando conectividade aprimorada...")
        progress.progress(60)
        
        try:
            # Teste de conectividade com timeout
            test_result = st.session_state.db.supabase.table('ibama_infracao').select('NUM_AUTO_INFRACAO').limit(10).execute()
            if test_result.data:
                st.success(f"✅ Conectividade OK - {len(test_result.data)} registros de teste")
            else:
                st.error("❌ Problema na conectividade")
                return
        except Exception as e:
            st.error(f"❌ Erro na conectividade: {e}")
            return
        
        # Etapa 5: Teste de paginação otimizada
        status_text.text("5/6 - Testando paginação otimizada...")
        progress.progress(80)
        
        # Força uma busca com as novas configurações
        test_cache_key = f"advanced_fix_{time.time()}_{random.randint(1000, 9999)}"
        
        try:
            test_df = st.session_state.viz.paginator.get_all_records_fixed('ibama_infracao', test_cache_key)
            
            if not test_df.empty:
                test_total = len(test_df)
                test_unique = test_df['NUM_AUTO_INFRACAO'].nunique() if 'NUM_AUTO_INFRACAO' in test_df.columns else 0
                
                st.success(f"✅ Paginação teste: {test_total:,} registros, {test_unique:,} únicos")
                
                # Avalia resultado
                if test_unique >= 20000:  # Próximo do esperado
                    st.success("🎉 **Correção bem-sucedida!**")
                elif test_unique >= 15000:  # Melhoria significativa
                    st.warning("⚠️ **Melhoria parcial** - ainda pode ser otimizado")
                else:
                    st.error("❌ **Correção insuficiente** - problemas persistem")
                    
            else:
                st.error("❌ Nenhum dado carregado no teste")
                return
                
        except Exception as e:
            st.error(f"❌ Erro no teste de paginação: {e}")
            return
        
        # Etapa 6: Validação final
        status_text.text("6/6 - Validação final...")
        progress.progress(100)
        
        # Executa contagem real para validar
        try:
            final_counts = st.session_state.viz.paginator.get_real_count_fixed()
            
            if 'error' not in final_counts:
                st.metric("📊 Contagem Final", f"{final_counts['unique_infractions']:,} únicos")
                
                if final_counts['unique_infractions'] >= 21000:
                    st.success("✅ **Sistema corrigido e funcionando!**")
                elif final_counts['unique_infractions'] >= 20000:
                    st.warning("⚠️ **Quase corrigido** - pequenos ajustes necessários")
                else:
                    st.error("❌ **Problema persiste** - investigação adicional necessária")
            else:
                st.error("❌ Erro na validação final")
                
        except Exception as e:
            st.warning(f"⚠️ Erro na validação: {e}")
        
        # Instruções finais
        st.subheader("📋 Próximos Passos")
        st.info("1. **Recarregue a página** (F5) para aplicar todas as correções")
        st.info("2. **Execute diagnóstico completo** para verificar melhorias")
        st.info("3. **Teste o dashboard principal** para confirmar dados corretos")
        
        if st.button("🔄 Recarregar Página Automaticamente"):
            st.rerun()
    
    except Exception as e:
        st.error(f"❌ Erro na correção avançada: {e}")

def force_complete_reload():
    """Força recarregamento completo do sistema."""
    try:
        st.subheader("🎯 Recarregamento Completo do Sistema")
        
        with st.spinner("Executando recarregamento completo..."):
            # Remove todos os componentes da sessão
            components_to_remove = ['viz', 'chatbot', 'db', 'llm', 'session_uuid']
            removed_count = 0
            
            for component in components_to_remove:
                if component in st.session_state:
                    del st.session_state[component]
                    removed_count += 1
            
            # Limpa todos os caches
            st.cache_data.clear()
            st.cache_resource.clear()
            
            # Limpa cache da sessão
            session_keys = [key for key in st.session_state.keys() if 'paginated_data' in key]
            for key in session_keys:
                del st.session_state[key]
        
        st.success(f"✅ **Recarregamento completo executado!**")
        st.success(f"✅ {removed_count} componentes removidos")
        st.success(f"✅ {len(session_keys)} chaves de cache removidas")
        st.success("✅ Cache global limpo")
        
        st.info("**🔄 Sistema será reinicializado na próxima interação**")
        st.info("**💡 Recomendação: Recarregue a página (F5) para garantir inicialização limpa**")
        
        if st.button("🔄 Recarregar Página Agora"):
            st.rerun()
    
    except Exception as e:
        st.error(f"❌ Erro no recarregamento: {e}")

def generate_diagnostic_report():
    """Gera relatório completo de diagnóstico."""
    try:
        st.subheader("📋 Relatório Completo de Diagnóstico")
        
        report_data = {
            "timestamp": pd.Timestamp.now(),
            "session_id": st.session_state.get('session_uuid', 'N/A'),
            "system_status": {},
            "data_analysis": {},
            "recommendations": []
        }
        
        with st.spinner("Gerando relatório..."):
            # Coleta informações do sistema
            report_data["system_status"] = {
                "supabase_connected": st.session_state.db.is_cloud and bool(st.session_state.db.supabase),
                "paginator_active": hasattr(st.session_state, 'viz') and hasattr(st.session_state.viz, 'paginator'),
                "cache_keys_count": len([k for k in st.session_state.keys() if 'paginated_data' in k])
            }
            
            # Executa análises
            if report_data["system_status"]["paginator_active"]:
                try:
                    real_counts = st.session_state.viz.paginator.get_real_count()
                    report_data["data_analysis"] = real_counts
                except:
                    report_data["data_analysis"] = {"error": "Não foi possível obter contagens"}
            
            # Gera recomendações
            if report_data["data_analysis"].get("unique_infractions", 0) < 21000:
                report_data["recommendations"].append("Verificar integridade dos dados no banco")
                report_data["recommendations"].append("Considerar re-upload dos dados originais")
            
            if report_data["system_status"]["cache_keys_count"] == 0:
                report_data["recommendations"].append("Cache vazio - primeira execução ou problema na paginação")
            
            if not report_data["system_status"]["supabase_connected"]:
                report_data["recommendations"].append("CRÍTICO: Problema na conexão com Supabase")
        
        # Exibe relatório
        st.json(report_data)
        
        # Download do relatório
        import json
        report_json = json.dumps(report_data, default=str, indent=2)
        st.download_button(
            label="📥 Baixar Relatório (JSON)",
            data=report_json,
            file_name=f"diagnostic_report_{report_data['session_id']}.json",
            mime="application/json"
        )
    
    except Exception as e:
        st.error(f"❌ Erro ao gerar relatório: {e}")

def run_integrity_test():
    """Executa teste de integridade dos dados."""
    try:
        st.subheader("🔬 Teste de Integridade dos Dados")
        
        with st.spinner("Executando testes de integridade..."):
            results = {
                "connection_test": False,
                "structure_test": False,
                "uniqueness_test": False,
                "completeness_test": False,
                "details": {}
            }
            
            # Teste 1: Conexão
            try:
                test_query = st.session_state.db.supabase.table('ibama_infracao').select('NUM_AUTO_INFRACAO').limit(1).execute()
                results["connection_test"] = bool(test_query.data)
                results["details"]["connection"] = "OK" if results["connection_test"] else "FALHA"
            except Exception as e:
                results["details"]["connection"] = f"ERRO: {str(e)[:100]}"
            
            # Teste 2: Estrutura
            try:
                structure_test = st.session_state.viz.paginator.diagnostic_database_structure()
                results["structure_test"] = structure_test.get("num_auto_exists", False)
                results["details"]["structure"] = f"Colunas: {structure_test.get('total_columns', 0)}"
            except Exception as e:
                results["details"]["structure"] = f"ERRO: {str(e)[:100]}"
            
            # Teste 3: Unicidade
            try:
                sample = st.session_state.db.supabase.table('ibama_infracao').select('NUM_AUTO_INFRACAO').limit(1000).execute()
                if sample.data:
                    df_sample = pd.DataFrame(sample.data)
                    total_sample = len(df_sample)
                    unique_sample = df_sample['NUM_AUTO_INFRACAO'].nunique()
                    results["uniqueness_test"] = unique_sample > 0
                    results["details"]["uniqueness"] = f"{unique_sample}/{total_sample} únicos na amostra"
                else:
                    results["details"]["uniqueness"] = "Sem dados na amostra"
            except Exception as e:
                results["details"]["uniqueness"] = f"ERRO: {str(e)[:100]}"
            
            # Teste 4: Completude
            try:
                if hasattr(st.session_state.viz, 'paginator'):
                    counts = st.session_state.viz.paginator.get_real_count()
                    if 'error' not in counts:
                        expected = 21019
                        actual = counts.get('unique_infractions', 0)
                        completeness = (actual / expected) * 100 if expected > 0 else 0
                        results["completeness_test"] = completeness >= 95  # 95% ou mais
                        results["details"]["completeness"] = f"{actual:,}/{expected:,} ({completeness:.1f}%)"
                    else:
                        results["details"]["completeness"] = "Erro na contagem"
                else:
                    results["details"]["completeness"] = "Paginador não disponível"
            except Exception as e:
                results["details"]["completeness"] = f"ERRO: {str(e)[:100]}"
        
        # Exibe resultados
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            status = "✅ PASS" if results["connection_test"] else "❌ FAIL"
            st.metric("🔗 Conexão", status)
            st.caption(results["details"].get("connection", "N/A"))
        
        with col2:
            status = "✅ PASS" if results["structure_test"] else "❌ FAIL"
            st.metric("📋 Estrutura", status)
            st.caption(results["details"].get("structure", "N/A"))
        
        with col3:
            status = "✅ PASS" if results["uniqueness_test"] else "❌ FAIL"
            st.metric("🔢 Unicidade", status)
            st.caption(results["details"].get("uniqueness", "N/A"))
        
        with col4:
            status = "✅ PASS" if results["completeness_test"] else "❌ FAIL"
            st.metric("📊 Completude", status)
            st.caption(results["details"].get("completeness", "N/A"))
        
        # Avaliação geral
        passed_tests = sum([results["connection_test"], results["structure_test"], 
                           results["uniqueness_test"], results["completeness_test"]])
        
        if passed_tests == 4:
            st.success("🎉 **TODOS OS TESTES PASSARAM** - Sistema íntegro!")
        elif passed_tests >= 3:
            st.warning(f"⚠️ **{passed_tests}/4 TESTES PASSARAM** - Problemas menores detectados")
        elif passed_tests >= 2:
            st.error(f"❌ **{passed_tests}/4 TESTES PASSARAM** - Problemas significativos")
        else:
            st.error(f"🚨 **{passed_tests}/4 TESTES PASSARAM** - Sistema com falhas críticas")
    
    except Exception as e:
        st.error(f"❌ Erro no teste de integridade: {e}")

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
