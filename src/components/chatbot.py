import streamlit as st
import pandas as pd
from typing import Dict, Any, Optional

class Chatbot:
    def __init__(self, llm_integration=None):
        self.llm_integration = llm_integration
        self.cached_data = None  # Cache local dos dados
        
    def initialize_chat_state(self):
        """Inicializa o estado do chat."""
        if "messages" not in st.session_state:
            st.session_state.messages = []
    
    def _get_cached_data(self) -> pd.DataFrame:
        """Obtém dados em cache para análises rápidas."""
        if self.cached_data is None:
            try:
                # Usa o paginador se disponível
                if (hasattr(self.llm_integration, 'database') and 
                    self.llm_integration.database.is_cloud and 
                    self.llm_integration.database.supabase):
                    
                    try:
                        from src.utils.supabase_utils import SupabasePaginator
                        paginator = SupabasePaginator(self.llm_integration.database.supabase)
                        self.cached_data = paginator.get_all_records()
                        print(f"✅ Cache carregado: {len(self.cached_data)} registros")
                    except ImportError:
                        # Fallback sem paginador
                        result = self.llm_integration.database.supabase.table('ibama_infracao').select('*').limit(50000).execute()
                        self.cached_data = pd.DataFrame(result.data)
                else:
                    # DuckDB ou erro
                    self.cached_data = pd.DataFrame()
                    
            except Exception as e:
                print(f"Erro ao carregar cache: {e}")
                self.cached_data = pd.DataFrame()
        
        return self.cached_data
    
    def _answer_with_data_analysis(self, question: str) -> Dict[str, Any]:
        """Responde perguntas usando análise direta dos dados."""
        question_lower = question.lower()
        
        # Carrega dados
        df = self._get_cached_data()
        
        if df.empty:
            return {
                "answer": "❌ Não foi possível carregar os dados para análise.",
                "source": "error"
            }
        
        try:
            # Respostas para perguntas específicas sobre dados
            if any(keyword in question_lower for keyword in ["estados", "uf", "5 estados", "top estados"]):
                return self._analyze_top_states(df, question)
            
            elif any(keyword in question_lower for keyword in ["municípios", "cidades", "top municípios"]):
                return self._analyze_top_municipalities(df, question)
            
            elif any(keyword in question_lower for keyword in ["valor", "multa", "total", "dinheiro"]):
                return self._analyze_fines(df, question)
            
            elif any(keyword in question_lower for keyword in ["tipo", "infração", "categoria"]):
                return self._analyze_infraction_types(df, question)
            
            elif any(keyword in question_lower for keyword in ["ano", "tempo", "período", "quando"]):
                return self._analyze_by_year(df, question)
            
            elif any(keyword in question_lower for keyword in ["total", "quantos", "número"]):
                return self._analyze_totals(df, question)
            
            # Respostas sobre conceitos específicos do IBAMA
            elif any(keyword in question_lower for keyword in ["biopirataria", "org. gen.", "modificação genética", "organismo"]):
                return self._explain_concepts(question)
            
            elif any(keyword in question_lower for keyword in ["gravidade", "multa leve", "multa grave"]):
                return self._analyze_gravity(df, question)
            
            elif any(keyword in question_lower for keyword in ["fauna", "flora", "animal", "planta"]):
                return self._analyze_fauna_flora(df, question)
            
            else:
                # Resposta genérica
                return self._analyze_general(df, question)
        
        except Exception as e:
            return {
                "answer": f"❌ Erro na análise: {str(e)}",
                "source": "error"
            }
    
    def _analyze_top_states(self, df: pd.DataFrame, question: str) -> Dict[str, Any]:
        """Analisa os estados com mais infrações."""
        try:
            if 'UF' not in df.columns:
                return {"answer": "❌ Coluna UF não encontrada nos dados.", "source": "error"}
            
            # Conta infrações por estado
            state_counts = df['UF'].value_counts()
            
            # Extrai número do top (padrão 5)
            import re
            numbers = re.findall(r'\d+', question)
            top_n = int(numbers[0]) if numbers else 5
            top_n = min(top_n, 15)  # Máximo 15
            
            top_states = state_counts.head(top_n)
            
            # Formata resposta
            answer = f"**🏆 Top {top_n} Estados com Mais Infrações:**\n\n"
            for i, (uf, count) in enumerate(top_states.items(), 1):
                percentage = (count / len(df)) * 100
                answer += f"{i}. **{uf}**: {count:,} infrações ({percentage:.1f}%)\n"
            
            answer += f"\n📊 Total analisado: {len(df):,} infrações"
            
            return {"answer": answer, "source": "data_analysis"}
            
        except Exception as e:
            return {"answer": f"❌ Erro ao analisar estados: {e}", "source": "error"}
    
    def _analyze_top_municipalities(self, df: pd.DataFrame, question: str) -> Dict[str, Any]:
        """Analisa os municípios com mais infrações."""
        try:
            if 'MUNICIPIO' not in df.columns or 'UF' not in df.columns:
                return {"answer": "❌ Colunas necessárias não encontradas.", "source": "error"}
            
            # Remove valores vazios
            df_clean = df[df['MUNICIPIO'].notna() & (df['MUNICIPIO'] != '')]
            
            # Conta por município
            muni_counts = df_clean.groupby(['MUNICIPIO', 'UF']).size().reset_index(name='count')
            muni_counts = muni_counts.sort_values('count', ascending=False)
            
            # Top N
            import re
            numbers = re.findall(r'\d+', question)
            top_n = int(numbers[0]) if numbers else 5
            top_n = min(top_n, 10)
            
            top_munis = muni_counts.head(top_n)
            
            answer = f"**🏙️ Top {top_n} Municípios com Mais Infrações:**\n\n"
            for i, row in enumerate(top_munis.itertuples(), 1):
                answer += f"{i}. **{row.MUNICIPIO} ({row.UF})**: {row.count:,} infrações\n"
            
            answer += f"\n📊 Total de municípios analisados: {muni_counts['MUNICIPIO'].nunique():,}"
            
            return {"answer": answer, "source": "data_analysis"}
            
        except Exception as e:
            return {"answer": f"❌ Erro ao analisar municípios: {e}", "source": "error"}
    
    def _analyze_fines(self, df: pd.DataFrame, question: str) -> Dict[str, Any]:
        """Analisa valores de multas."""
        try:
            if 'VAL_AUTO_INFRACAO' not in df.columns:
                return {"answer": "❌ Coluna de valores não encontrada.", "source": "error"}
            
            # Converte valores
            df['VAL_NUMERIC'] = pd.to_numeric(
                df['VAL_AUTO_INFRACAO'].astype(str).str.replace(',', '.'), 
                errors='coerce'
            )
            
            df_valid = df[df['VAL_NUMERIC'].notna()]
            
            if df_valid.empty:
                return {"answer": "❌ Nenhum valor válido encontrado.", "source": "error"}
            
            # Estatísticas
            total_value = df_valid['VAL_NUMERIC'].sum()
            avg_value = df_valid['VAL_NUMERIC'].mean()
            max_value = df_valid['VAL_NUMERIC'].max()
            
            # Formata valores
            def format_currency(value):
                if value >= 1_000_000_000:
                    return f"R$ {value/1_000_000_000:.1f} bilhões"
                elif value >= 1_000_000:
                    return f"R$ {value/1_000_000:.1f} milhões"
                else:
                    return f"R$ {value:,.2f}"
            
            answer = f"**💰 Análise de Valores de Multas:**\n\n"
            answer += f"• **Total**: {format_currency(total_value)}\n"
            answer += f"• **Média por infração**: {format_currency(avg_value)}\n"
            answer += f"• **Maior multa**: {format_currency(max_value)}\n"
            answer += f"• **Infrações com valor**: {len(df_valid):,} de {len(df):,}\n"
            
            return {"answer": answer, "source": "data_analysis"}
            
        except Exception as e:
            return {"answer": f"❌ Erro ao analisar valores: {e}", "source": "error"}
    
    def _analyze_infraction_types(self, df: pd.DataFrame, question: str) -> Dict[str, Any]:
        """Analisa tipos de infrações."""
        try:
            if 'TIPO_INFRACAO' not in df.columns:
                return {"answer": "❌ Coluna de tipos não encontrada.", "source": "error"}
            
            df_clean = df[df['TIPO_INFRACAO'].notna() & (df['TIPO_INFRACAO'] != '')]
            type_counts = df_clean['TIPO_INFRACAO'].value_counts().head(10)
            
            answer = "**📋 Principais Tipos de Infrações:**\n\n"
            for i, (tipo, count) in enumerate(type_counts.items(), 1):
                percentage = (count / len(df_clean)) * 100
                answer += f"{i}. **{tipo.title()}**: {count:,} casos ({percentage:.1f}%)\n"
            
            return {"answer": answer, "source": "data_analysis"}
            
        except Exception as e:
            return {"answer": f"❌ Erro ao analisar tipos: {e}", "source": "error"}
    
    def _analyze_by_year(self, df: pd.DataFrame, question: str) -> Dict[str, Any]:
        """Analisa dados por ano."""
        try:
            if 'DAT_HORA_AUTO_INFRACAO' not in df.columns:
                return {"answer": "❌ Coluna de data não encontrada.", "source": "error"}
            
            df['DATE'] = pd.to_datetime(df['DAT_HORA_AUTO_INFRACAO'], errors='coerce')
            df_with_date = df[df['DATE'].notna()]
            
            year_counts = df_with_date['DATE'].dt.year.value_counts().sort_index()
            
            answer = "**📅 Infrações por Ano:**\n\n"
            for year, count in year_counts.tail(5).items():
                answer += f"• **{int(year)}**: {count:,} infrações\n"
            
            answer += f"\n📊 Total com data válida: {len(df_with_date):,}"
            
            return {"answer": answer, "source": "data_analysis"}
            
        except Exception as e:
            return {"answer": f"❌ Erro ao analisar por ano: {e}", "source": "error"}
    
    def _analyze_totals(self, df: pd.DataFrame, question: str) -> Dict[str, Any]:
        """Analisa totais gerais."""
        try:
            total_records = len(df)
            total_states = df['UF'].nunique() if 'UF' in df.columns else 0
            total_municipalities = df['MUNICIPIO'].nunique() if 'MUNICIPIO' in df.columns else 0
            
            answer = "**📊 Resumo Geral dos Dados:**\n\n"
            answer += f"• **Total de infrações**: {total_records:,}\n"
            answer += f"• **Estados envolvidos**: {total_states}\n"
            answer += f"• **Municípios afetados**: {total_municipalities:,}\n"
            
            # Período dos dados
            if 'DAT_HORA_AUTO_INFRACAO' in df.columns:
                df['DATE'] = pd.to_datetime(df['DAT_HORA_AUTO_INFRACAO'], errors='coerce')
                df_with_date = df[df['DATE'].notna()]
                if not df_with_date.empty:
                    min_year = df_with_date['DATE'].dt.year.min()
                    max_year = df_with_date['DATE'].dt.year.max()
                    answer += f"• **Período**: {int(min_year)} a {int(max_year)}\n"
            
            return {"answer": answer, "source": "data_analysis"}
            
        except Exception as e:
            return {"answer": f"❌ Erro ao calcular totais: {e}", "source": "error"}
    
    def _explain_concepts(self, question: str) -> Dict[str, Any]:
        """Explica conceitos relacionados às infrações ambientais."""
        question_lower = question.lower()
        
        if any(keyword in question_lower for keyword in ["org. gen.", "modificação genética", "organismo geneticamente modificado"]):
            answer = """**🧬 Organismos Geneticamente Modificados (OGM):**

**Definição:** Organismos cujo material genético foi alterado através de técnicas de engenharia genética.

**No contexto do IBAMA:**
• Controle da introdução de OGMs no meio ambiente
• Licenciamento para pesquisa e cultivo
• Monitoramento de impactos ambientais
• Fiscalização do transporte e armazenamento

**Principais infrações:**
• Cultivo sem autorização
• Transporte irregular
• Falta de isolamento adequado
• Não cumprimento de medidas de biossegurança"""

        elif "biopirataria" in question_lower:
            answer = """**🏴‍☠️ Biopirataria:**

**Definição:** Apropriação ilegal de recursos biológicos e conhecimentos tradicionais sem autorização ou compensação.

**Principais modalidades:**
• **Coleta ilegal** de espécimes da fauna e flora
• **Extração não autorizada** de material genético
• **Uso comercial** sem licença de recursos naturais
• **Apropriação** de conhecimentos de comunidades tradicionais

**No contexto do IBAMA:**
• Fiscalização da coleta científica
• Controle de acesso ao patrimônio genético
• Licenciamento para pesquisa biológica
• Proteção de conhecimentos tradicionais

**Penalidades:**
• Multas de R$ 200 a R$ 2 milhões
• Apreensão do material coletado
• Processo criminal
• Reparação de danos ambientais"""

        else:
            # Resposta genérica sobre conceitos
            answer = """**📚 Conceitos Ambientais no IBAMA:**

**Principais áreas de atuação:**
• **Biopirataria:** Apropriação ilegal de recursos biológicos
• **OGMs:** Controle de organismos geneticamente modificados  
• **Fauna:** Proteção de animais silvestres
• **Flora:** Conservação da vegetação nativa
• **Recursos hídricos:** Gestão de águas
• **Unidades de conservação:** Proteção de áreas especiais

**Tipos de infração:**
• Leves, graves e gravíssimas
• Multas de R$ 50 a R$ 50 milhões
• Medidas administrativas
• Responsabilização criminal"""

        return {"answer": answer, "source": "knowledge_base"}
    
    def _analyze_gravity(self, df: pd.DataFrame, question: str) -> Dict[str, Any]:
        """Analisa distribuição por gravidade das infrações."""
        try:
            if 'GRAVIDADE_INFRACAO' not in df.columns:
                return {"answer": "❌ Coluna de gravidade não encontrada nos dados.", "source": "error"}
            
            df_clean = df[df['GRAVIDADE_INFRACAO'].notna() & (df['GRAVIDADE_INFRACAO'] != '')]
            gravity_counts = df_clean['GRAVIDADE_INFRACAO'].value_counts()
            
            answer = "**⚖️ Distribuição por Gravidade das Infrações:**\n\n"
            
            for gravity, count in gravity_counts.items():
                percentage = (count / len(df_clean)) * 100
                
                # Emoji por gravidade
                if "leve" in gravity.lower():
                    emoji = "🟢"
                elif "grave" in gravity.lower() and "gravíssima" not in gravity.lower():
                    emoji = "🟡"
                elif "gravíssima" in gravity.lower():
                    emoji = "🔴"
                else:
                    emoji = "⚫"
                
                answer += f"{emoji} **{gravity.title()}**: {count:,} infrações ({percentage:.1f}%)\n"
            
            answer += f"\n📊 Total analisado: {len(df_clean):,} infrações com gravidade definida"
            
            # Explicação das gravidades
            answer += "\n\n**ℹ️ Classificação:**\n"
            answer += "🟢 **Leves:** Multa de R$ 50 a R$ 10.000\n"
            answer += "🟡 **Graves:** Multa de R$ 10.001 a R$ 1.000.000\n"
            answer += "🔴 **Gravíssimas:** Multa de R$ 1.000.001 a R$ 50.000.000"
            
            return {"answer": answer, "source": "data_analysis"}
            
        except Exception as e:
            return {"answer": f"❌ Erro ao analisar gravidade: {e}", "source": "error"}
    
    def _analyze_fauna_flora(self, df: pd.DataFrame, question: str) -> Dict[str, Any]:
        """Analisa infrações relacionadas à fauna e flora."""
        try:
            if 'TIPO_INFRACAO' not in df.columns:
                return {"answer": "❌ Coluna de tipos de infração não encontrada.", "source": "error"}
            
            df_clean = df[df['TIPO_INFRACAO'].notna() & (df['TIPO_INFRACAO'] != '')]
            
            # Busca por termos relacionados à fauna e flora
            fauna_terms = ['fauna', 'animal', 'caça', 'pesca', 'peixe', 'ave', 'mamífero']
            flora_terms = ['flora', 'planta', 'árvore', 'madeira', 'vegetal', 'floresta']
            
            fauna_mask = df_clean['TIPO_INFRACAO'].str.contains(
                '|'.join(fauna_terms), case=False, na=False
            )
            flora_mask = df_clean['TIPO_INFRACAO'].str.contains(
                '|'.join(flora_terms), case=False, na=False
            )
            
            fauna_count = fauna_mask.sum()
            flora_count = flora_mask.sum()
            
            answer = "**🌿 Análise de Infrações Fauna e Flora:**\n\n"
            
            if fauna_count > 0:
                answer += f"🐾 **Infrações contra Fauna**: {fauna_count:,} casos\n"
                fauna_types = df_clean[fauna_mask]['TIPO_INFRACAO'].value_counts().head(5)
                for tipo, count in fauna_types.items():
                    answer += f"   • {tipo.title()}: {count:,}\n"
                answer += "\n"
            
            if flora_count > 0:
                answer += f"🌳 **Infrações contra Flora**: {flora_count:,} casos\n"
                flora_types = df_clean[flora_mask]['TIPO_INFRACAO'].value_counts().head(5)
                for tipo, count in flora_types.items():
                    answer += f"   • {tipo.title()}: {count:,}\n"
                answer += "\n"
            
            other_count = len(df_clean) - fauna_count - flora_count
            answer += f"⚖️ **Outras infrações**: {other_count:,} casos\n"
            
            answer += f"\n📊 Total analisado: {len(df_clean):,} infrações"
            
            return {"answer": answer, "source": "data_analysis"}
            
        except Exception as e:
            return {"answer": f"❌ Erro ao analisar fauna/flora: {e}", "source": "error"}
        """Análise genérica dos dados."""
        return {
            "answer": f"📊 Tenho {len(df):,} registros de infrações do IBAMA disponíveis para análise.\n\n" +
                     "**Posso ajudar com:**\n" +
                     "• Top estados com mais infrações\n" +
                     "• Principais municípios afetados\n" +
                     "• Análise de valores de multas\n" +
                     "• Tipos de infrações mais comuns\n" +
                     "• Dados por ano\n\n" +
                     "**Exemplo:** 'Quais são os 5 estados com mais infrações?'",
            "source": "data_analysis"
        }
    
    def query(self, question: str, provider: str = 'direct') -> Dict[str, Any]:
        """Processa uma pergunta do usuário."""
        
        question_lower = question.lower()
        
        # Palavras-chave que indicam perguntas sobre dados ou conceitos (não web)
        data_keywords = [
            "estados", "uf", "municípios", "cidades", "valor", "multa", 
            "tipo", "infração", "ano", "total", "quantos", "top", "maior", "menor",
            "biopirataria", "org. gen.", "modificação genética", "organismo",
            "gravidade", "leve", "grave", "gravíssima", "fauna", "flora", 
            "animal", "planta", "ibama", "ambiental"
        ]
        
        # Palavras que realmente precisam de busca web
        web_keywords = [
            "endereço", "telefone", "contato", "site oficial", "história do ibama",
            "quem é o presidente", "localização da sede", "como chegar"
        ]
        
        # Se tem palavras web específicas, tenta LLM/web
        if any(keyword in question_lower for keyword in web_keywords):
            if self.llm_integration:
                try:
                    return self.llm_integration.query(question, provider)
                except Exception as e:
                    return {
                        "answer": f"❌ Busca na internet não disponível: {str(e)}",
                        "source": "error"
                    }
        
        # Para perguntas sobre dados ou conceitos, usa análise local
        if any(keyword in question_lower for keyword in data_keywords):
            return self._answer_with_data_analysis(question)
        
        # Para perguntas genéricas sobre o sistema, responde diretamente
        if any(keyword in question_lower for keyword in ["o que", "como", "explicar", "definir"]):
            return self._answer_with_data_analysis(question)
        
        # Default: tenta análise local primeiro
        try:
            return self._answer_with_data_analysis(question)
        except Exception as e:
            return {
                "answer": "❌ Não consegui processar sua pergunta. Tente perguntas sobre:\n\n" +
                         "• Estados com mais infrações\n" +
                         "• Valores de multas\n" +
                         "• Tipos de infrações\n" +
                         "• Conceitos como biopirataria\n" +
                         "• Distribuição por gravidade",
                "source": "error"
            }
    
    def display_chat_interface(self):
        """Exibe a interface do chatbot."""
        
        # Botão para limpar cache
        if st.button("🔄 Recarregar Dados", help="Limpa cache e recarrega dados"):
            self.cached_data = None
            st.success("Cache limpo! Próxima consulta carregará dados atualizados.")
        
        # Histórico de mensagens
        for message in st.session_state.messages:
            with st.chat_message(message["role"]):
                st.markdown(message["content"])
        
        # Input do usuário
        if prompt := st.chat_input("Faça sua pergunta sobre os dados do IBAMA..."):
            # Adiciona mensagem do usuário
            st.session_state.messages.append({"role": "user", "content": prompt})
            with st.chat_message("user"):
                st.markdown(prompt)
            
            # Processa resposta
            with st.chat_message("assistant"):
                with st.spinner("🤖 A IA está analisando os dados..."):
                    try:
                        response = self.query(prompt)
                        answer = response.get("answer", "❌ Não foi possível processar sua pergunta.")
                        
                        # Adiciona informação sobre a fonte
                        if response.get("source") == "data_analysis":
                            answer += "\n\n*💡 Resposta baseada em análise direta dos dados*"
                        
                        st.markdown(answer)
                        
                        # Adiciona ao histórico
                        st.session_state.messages.append({"role": "assistant", "content": answer})
                        
                    except Exception as e:
                        error_msg = f"❌ Erro ao processar pergunta: {str(e)}"
                        st.markdown(error_msg)
                        st.session_state.messages.append({"role": "assistant", "content": error_msg})
    
    def display_sample_questions(self):
        """Exibe perguntas de exemplo."""
        with st.expander("💡 Perguntas de Exemplo"):
            
            # Categorias de perguntas
            st.write("**📊 Análise de Dados:**")
            data_questions = [
                "Quais são os 5 estados com mais infrações?",
                "Quais os principais municípios afetados?", 
                "Qual o valor total das multas?",
                "Quais os tipos de infrações mais comuns?",
                "Como está a distribuição por gravidade?"
            ]
            
            for question in data_questions:
                if st.button(question, key=f"data_{hash(question)}"):
                    self._handle_sample_question(question)
            
            st.write("**🧬 Conceitos Ambientais:**")
            concept_questions = [
                "O que é biopirataria?",
                "O que é Org. Gen. Modific.?",
                "Como funcionam as multas por gravidade?",
                "Quais infrações afetam fauna e flora?"
            ]
            
            for question in concept_questions:
                if st.button(question, key=f"concept_{hash(question)}"):
                    self._handle_sample_question(question)
    
    def _handle_sample_question(self, question: str):
        """Manipula clique em pergunta de exemplo."""
        # Adiciona pergunta do usuário
        st.session_state.messages.append({"role": "user", "content": question})
        
        # Processa resposta
        response = self.query(question)
        answer = response.get("answer", "❌ Erro ao processar pergunta.")
        
        # Adiciona indicador de fonte
        if response.get("source") == "data_analysis":
            answer += "\n\n*💡 Resposta baseada em análise dos dados*"
        elif response.get("source") == "knowledge_base":
            answer += "\n\n*📚 Resposta baseada em conhecimento especializado*"
        
        st.session_state.messages.append({"role": "assistant", "content": answer})
        st.rerun()
