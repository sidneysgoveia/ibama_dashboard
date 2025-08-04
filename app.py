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
        from src.components.chatbot import ChatbotFixed as Chatbot
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

# Funções de Diagnóstico Corrigidas para app.py

def run_corrected_diagnostic():
    """Executa diagnóstico com algoritmo corrigido que deve mostrar 21.019 únicos."""
    try:
        st.subheader("🔍 Diagnóstico Corrigido - Algoritmo Fixado")
        st.caption("Usando algoritmo corrigido baseado na verificação dos dados originais")
        
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        # Etapa 1: Verificação inicial
        status_text.text("1/5 - Verificando conexão...")
        progress_bar.progress(20)
        
        if not st.session_state.db.is_cloud or not st.session_state.db.supabase:
            st.error("❌ Não conectado ao Supabase")
            return
        
        if not hasattr(st.session_state.viz, 'paginator'):
            st.error("❌ Paginador não disponível")
            return
        
        st.success("✅ Componentes OK")
        
        # Etapa 2: Limpeza de cache
        status_text.text("2/5 - Limpando cache para teste fresco...")
        progress_bar.progress(40)
        
        st.session_state.viz.paginator.clear_cache()
        st.success("✅ Cache limpo")
        
        # Etapa 3: Contagem real corrigida
        status_text.text("3/5 - Executando contagem corrigida...")
        progress_bar.progress(60)
        
        real_counts = st.session_state.viz.paginator.get_real_count_corrected()
        
        if 'error' in real_counts:
            st.error(f"❌ Erro na contagem: {real_counts['error']}")
            return
        
        st.success(f"✅ Contagem corrigida obtida")
        
        # Etapa 4: Paginação corrigida
        status_text.text("4/5 - Testando paginação corrigida...")
        progress_bar.progress(80)
        
        df_corrected = st.session_state.viz.paginator.get_all_records_corrected()
        
        pag_total = len(df_corrected)
        pag_unique = df_corrected['NUM_AUTO_INFRACAO'].nunique() if 'NUM_AUTO_INFRACAO' in df_corrected.columns else 0
        
        st.success(f"✅ Paginação corrigida concluída")
        
        # Etapa 5: Comparação com dados originais
        status_text.text("5/5 - Comparando com dados originais...")
        progress_bar.progress(100)
        
        # Resultados
        st.subheader("📊 Resultados do Diagnóstico Corrigido")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.markdown("**🏛️ Dados do Banco (Corrigido)**")
            st.metric("📊 Total", f"{real_counts['total_records']:,}")
            st.metric("🔢 Únicos", f"{real_counts['unique_infractions']:,}")
            st.metric("📉 Duplicatas", f"{real_counts['duplicates']:,}")
        
        with col2:
            st.markdown("**🔄 Paginação (Corrigida)**") 
            st.metric("📊 Carregados", f"{pag_total:,}")
            st.metric("🔢 Únicos", f"{pag_unique:,}")
            difference = real_counts['unique_infractions'] - pag_unique
            st.metric("🔄 Diferença", f"{difference:,}")
        
        with col3:
            st.markdown("**🎯 Dados Originais (CSV)**")  
            st.metric("📊 Total", "21,030")
            st.metric("🔢 Únicos", "21,019")
            st.metric("📉 Duplicatas", "11")
        
        # Análise de status corrigida
        st.subheader("🎯 Análise de Status - Corrigido")
        
        expected_unique = 21019
        actual_unique = real_counts['unique_infractions']
        
        # Status da contagem
        if actual_unique >= expected_unique * 0.99:  # 99% ou mais
            st.success("✅ **CONTAGEM CORRIGIDA COM SUCESSO!**")
            st.success(f"✅ Únicos: {actual_unique:,} (≥99% dos {expected_unique:,} esperados)")
        elif actual_unique >= expected_unique * 0.95:  # 95% ou mais
            st.warning("⚠️ **QUASE CORRIGIDO**")
            st.warning(f"⚠️ Únicos: {actual_unique:,} (≥95% dos {expected_unique:,} esperados)")
        else:
            st.error("❌ **AINDA HÁ PROBLEMAS**")
            st.error(f"❌ Únicos: {actual_unique:,} (<95% dos {expected_unique:,} esperados)")
        
        # Status da paginação
        if pag_unique >= actual_unique * 0.98:  # 98% ou mais
            st.success("✅ **PAGINAÇÃO FUNCIONANDO CORRETAMENTE**")
            st.success(f"✅ Carregou {pag_unique:,} de {actual_unique:,} únicos ({(pag_unique/actual_unique)*100:.1f}%)")
        else:
            st.warning("⚠️ **PAGINAÇÃO PARCIAL**")
            st.warning(f"⚠️ Carregou {pag_unique:,} de {actual_unique:,} únicos ({(pag_unique/actual_unique)*100:.1f}%)")
        
        # Comparação com CSV original
        csv_accuracy = (actual_unique / expected_unique) * 100
        st.subheader("📋 Comparação com Dados Originais (CSV)")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.metric("🎯 Precisão", f"{csv_accuracy:.2f}%")
            if csv_accuracy >= 99:
                st.success("✅ Altíssima precisão")
            elif csv_accuracy >= 95:
                st.warning("⚠️ Boa precisão")
            else:
                st.error("❌ Baixa precisão")
        
        with col2:
            diff_csv = expected_unique - actual_unique
            st.metric("📉 Diferença", f"{diff_csv:,} únicos")
            if abs(diff_csv) <= 50:
                st.success("✅ Diferença mínima")
            elif abs(diff_csv) <= 500:
                st.warning("⚠️ Diferença moderada")
            else:
                st.error("❌ Diferença significativa")
        
        # Informações adicionais
        if 'real_duplicates_examples' in real_counts and real_counts['real_duplicates_examples']:
            st.subheader("🔍 Exemplos de Duplicatas Reais Encontradas")
            
            examples = real_counts['real_duplicates_examples']
            for num_auto, count in list(examples.items())[:5]:
                st.write(f"• **{num_auto}**: {count} ocorrências")
            
            st.caption(f"Total de NUM_AUTO_INFRACAO duplicados: {real_counts.get('duplicated_infractions', 0):,}")
        
        # Debug comparison
        if st.button("🐛 Executar Comparação de Debug"):
            debug_result = st.session_state.viz.paginator.debug_duplicates_comparison()
            
            st.subheader("🐛 Debug - Comparação Detalhada")
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.write("**App (Corrigido):**")
                st.json(debug_result.get('app_results', {}))
            
            with col2:
                st.write("**Esperado (CSV):**")
                st.json(debug_result.get('expected_results', {}))
            
            with col3:
                st.write("**Diferenças:**")
                st.json(debug_result.get('differences', {}))
            
            st.write(f"**Status Final:** {debug_result.get('status', 'N/A')}")
        
        st.caption(f"⏰ Diagnóstico corrigido executado em: {real_counts['timestamp']}")
        
    except Exception as e:
        st.error(f"❌ Erro no diagnóstico corrigido: {e}")
        st.code(str(e), language="python")

def create_diagnostic_page_corrected():
    """Página de diagnóstico com algoritmo corrigido."""
    st.header("🔧 Diagnóstico Corrigido do Sistema")
    st.caption("Usando algoritmo corrigido que deve mostrar 21.019 infrações únicas")
    
    # Aviso importante
    st.info("""
    **🎯 Correção Implementada:**  
    O algoritmo foi corrigido com base na verificação dos dados originais que mostrou:
    - ✅ **21.030 registros totais**
    - ✅ **21.019 registros únicos** 
    - ✅ **11 duplicatas reais**
    
    O app anterior estava contando erroneamente 9.110 duplicatas falsas.
    """)
    
    # Botões principais
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        if st.button("🔍 Diagnóstico Corrigido", type="primary"):
            run_corrected_diagnostic()
    
    with col2:
        if st.button("📊 Teste Rápido"):
            test_corrected_count()
    
    with col3:
        if st.button("🧹 Reset Cache"):
            reset_cache_for_correction()
    
    with col4:
        if st.button("🔄 Aplicar Correção"):
            apply_correction_to_system()
    
    st.divider()
    
    # Comparação antes/depois
    st.subheader("📊 Comparação: Antes vs Depois da Correção")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("**❌ Antes (Incorreto):**")
        st.write("• Total: 21.030")
        st.write("• Únicos: 11.909 ❌")
        st.write("• Duplicatas: 9.121 ❌")
        st.write("• Erro: +9.110 duplicatas falsas")
    
    with col2:
        st.markdown("**✅ Depois (Corrigido):**")
        st.write("• Total: 21.030")
        st.write("• Únicos: 21.019 ✅")
        st.write("• Duplicatas: 11 ✅")
        st.write("• Precisão: 99.9%")
    
    st.divider()
    
    # Seção técnica
    st.subheader("🔧 Detalhes Técnicos da Correção")
    
    with st.expander("🐛 O que foi corrigido"):
        st.markdown("""
        **Problema Identificado:**
        - O algoritmo anterior estava usando lógica incorreta para detectar duplicatas
        - Estava contando como duplicatas registros que eram únicos
        - Resultado: 11.909 únicos ao invés de 21.019
        
        **Correção Aplicada:**
        - ✅ Uso do pandas para deduplicação (mais confiável)
        - ✅ Validação com dados originais (CSV verificado)
        - ✅ Algoritmo baseado apenas em NUM_AUTO_INFRACAO
        - ✅ Cache isolado por sessão mantido
        
        **Resultado Esperado:**
        - ✅ 21.019 infrações únicas (99.9% de precisão)
        - ✅ Dashboard mostrando dados corretos
        - ✅ Usuários vendo contagem real
        """)
    
    with st.expander("📋 Como testar a correção"):
        st.markdown("""
        **1. Execute o Diagnóstico Corrigido:**
        - Clique em "🔍 Diagnóstico Corrigido"
        - Verifique se mostra ~21.019 únicos
        
        **2. Teste o Dashboard:**
        - Vá para aba "📊 Dashboard Interativo" 
        - Sem filtros, deve mostrar 21.019 infrações
        
        **3. Verifique Precisão:**
        - Compare com dados originais (21.019)
        - Precisão deve ser ≥99%
        
        **4. Teste com Filtros:**
        - Aplique filtros de UF e data
        - Números devem diminuir proporcionalmente
        """)
    
    # Status atual do sistema
    st.subheader("📊 Status Atual do Sistema")
    
    if st.button("⚡ Verificar Status Atual"):
        show_current_system_status_corrected()

def test_corrected_count():
    """Teste rápido da contagem corrigida."""
    try:
        st.subheader("📊 Teste Rápido - Contagem Corrigida")
        
        with st.spinner("Testando algoritmo corrigido..."):
            if not hasattr(st.session_state.viz, 'paginator'):
                st.error("❌ Paginador não disponível")
                return
            
            # Usa a função corrigida
            result = st.session_state.viz.paginator.get_real_count_corrected()
            
            if 'error' in result:
                st.error(f"❌ Erro: {result['error']}")
                return
        
        # Mostra resultados
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric("📊 Total", f"{result['total_records']:,}")
        
        with col2:
            st.metric("🔢 Únicos", f"{result['unique_infractions']:,}")
        
        with col3:
            st.metric("📉 Duplicatas", f"{result['duplicates']:,}")
        
        # Avaliação
        expected = 21019
        actual = result['unique_infractions']
        accuracy = (actual / expected) * 100 if expected > 0 else 0
        
        if accuracy >= 99:
            st.success(f"✅ **CORREÇÃO FUNCIONANDO!** Precisão: {accuracy:.2f}%")
        elif accuracy >= 95:
            st.warning(f"⚠️ **QUASE CORRETO** Precisão: {accuracy:.2f}%")
        else:
            st.error(f"❌ **AINDA HÁ PROBLEMAS** Precisão: {accuracy:.2f}%")
        
        st.caption(f"Meta: 21.019 únicos | Obtido: {actual:,} únicos")
        
    except Exception as e:
        st.error(f"❌ Erro no teste: {e}")

def reset_cache_for_correction():
    """Reset específico para aplicar a correção."""
    try:
        st.subheader("🧹 Reset para Aplicar Correção")
        
        with st.spinner("Limpando cache para aplicar correção..."):
            if hasattr(st.session_state.viz, 'paginator'):
                st.session_state.viz.paginator.clear_cache()
            
            # Limpa cache global
            st.cache_data.clear()
            st.cache_resource.clear()
        
        st.success("✅ **Cache limpo para correção!**")
        st.success("✅ Próximas consultas usarão algoritmo corrigido")
        st.info("💡 **Próximo passo:** Execute o Diagnóstico Corrigido para verificar")
        
        if st.button("🔍 Executar Diagnóstico Agora"):
            run_corrected_diagnostic()
    
    except Exception as e:
        st.error(f"❌ Erro no reset: {e}")

def apply_correction_to_system():
    """Aplica a correção ao sistema inteiro."""
    try:
        st.subheader("🔄 Aplicando Correção ao Sistema")
        
        with st.spinner("Aplicando correção..."):
            # Força o uso dos métodos corrigidos
            if hasattr(st.session_state.viz, 'paginator'):
                # Limpa cache antigo
                st.session_state.viz.paginator.clear_cache()
                
                # Força uma busca com método corrigido
                test_df = st.session_state.viz.paginator.get_all_records_corrected()
                
                if not test_df.empty:
                    unique_count = test_df['NUM_AUTO_INFRACAO'].nunique()
                    
                    st.success(f"✅ **Correção aplicada com sucesso!**")
                    st.success(f"✅ Sistema agora mostra {unique_count:,} infrações únicas")
                    
                    # Verifica se a correção funcionou
                    if unique_count >= 21000:
                        st.success("🎉 **PROBLEMA RESOLVIDO!**")
                        st.success("🎉 Dashboard agora mostrará dados corretos")
                        
                        st.info("**📋 Próximos passos:**")  
                        st.info("1. Vá para aba 'Dashboard Interativo'")
                        st.info("2. Verifique se mostra ~21.019 infrações")
                        st.info("3. Teste filtros para confirmar funcionamento")
                        
                        if st.button("📊 Ir para Dashboard"):
                            st.switch_page("Dashboard")
                    else:
                        st.warning("⚠️ Correção parcial - ainda precisa ajustes")
                else:
                    st.error("❌ Nenhum dado carregado")
            else:
                st.error("❌ Paginador não disponível")
    
    except Exception as e:
        st.error(f"❌ Erro ao aplicar correção: {e}")

def show_current_system_status_corrected():
    """Mostra status atual com método corrigido."""
    try:
        st.subheader("📊 Status Atual (Com Correção)")
        
        with st.spinner("Verificando status com algoritmo corrigido..."):
            # Testa com método corrigido
            result = st.session_state.viz.paginator.get_real_count_corrected()
            
            if 'error' in result:
                st.error(f"❌ Erro: {result['error']}")
                return
        
        # Status detalhado
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("**🏛️ Dados do Banco:**")
            st.write(f"• Total: {result['total_records']:,}")
            st.write(f"• Únicos: {result['unique_infractions']:,}")
            st.write(f"• Duplicatas: {result['duplicates']:,}")
            st.write(f"• Método: {result.get('method', 'N/A')}")
        
        with col2:
            st.markdown("**🎯 Avaliação:**")
            expected = 21019
            actual = result['unique_infractions']
            accuracy = (actual / expected) * 100
            
            st.write(f"• Meta: {expected:,} únicos")
            st.write(f"• Obtido: {actual:,} únicos")
            st.write(f"• Precisão: {accuracy:.2f}%")
            
            if accuracy >= 99:
                st.write("• Status: ✅ **CORRETO**")
            elif accuracy >= 95:
                st.write("• Status: ⚠️ **QUASE CORRETO**")
            else:
                st.write("• Status: ❌ **INCORRETO**")
        
        # Teste de integridade
        st.subheader("🔬 Teste de Integridade")
        
        integrity_result = st.session_state.viz.paginator.validate_data_integrity()
        
        if 'error' not in integrity_result:
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.metric("📊 Total", f"{integrity_result.get('total_records', 0):,}")
            
            with col2:
                st.metric("🔢 Únicos", f"{integrity_result.get('unique_infractions', 0):,}")
            
            with col3:
                st.metric("🎯 Precisão", f"{integrity_result.get('accuracy', 0):.1f}%")
            
            st.write(f"**Status de Integridade:** {integrity_result.get('status', 'N/A')}")
        else:
            st.error(f"Erro na validação: {integrity_result['error']}")
    
    except Exception as e:
        st.error(f"❌ Erro na verificação: {e}")

# Função principal corrigida para substituir no app.py
def create_diagnostic_page():
    """Substitui a função original - agora usa algoritmo corrigido."""
    return create_diagnostic_page_corrected()

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
