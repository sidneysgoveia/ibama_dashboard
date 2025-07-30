import streamlit as st
import pandas as pd
from typing import Dict, Any, Optional

class Chatbot:
    def __init__(self, llm_integration=None):
        self.llm_integration = llm_integration
        self.cached_data = None  # Cache local dos dados
        self.llm_config = {
            "provider": "groq",
            "temperature": 0.0,
            "max_tokens": 500
        }
        
    def set_llm_config(self, provider="groq", temperature=0.0, max_tokens=500):
        """Define configurações do LLM."""
        self.llm_config = {
            "provider": provider,
            "temperature": temperature,
            "max_tokens": max_tokens
        }
        
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
            # Análises específicas por UF e tipo
            if any(keyword in question_lower for keyword in ["amazonas", "rio grande do sul", "são paulo", "minas gerais"]) and any(keyword in question_lower for keyword in ["pesca", "fauna", "flora"]):
                return self._analyze_specific_region_type(df, question)
            
            # Análises de pessoas físicas vs empresas
            elif any(keyword in question_lower for keyword in ["pessoas físicas", "empresas", "infrator", "quem mais"]):
                return self._analyze_top_offenders_detailed(df, question)
            
            # Respostas para perguntas específicas sobre dados
            elif any(keyword in question_lower for keyword in ["estados", "uf", "5 estados", "top estados"]):
                return self._analyze_top_states(df, question)
            
            elif any(keyword in question_lower for keyword in ["municípios", "cidades", "top municípios"]):
                return self._analyze_top_municipalities(df, question)
            
            elif any(keyword in question_lower for keyword in ["valor", "multa", "total", "dinheiro"]):
                return self._analyze_fines(df, question)
            
            elif any(keyword in question_lower for keyword in ["tipo", "infração", "categoria"]) and "o que" not in question_lower:
                return self._analyze_infraction_types(df, question)
            
            elif any(keyword in question_lower for keyword in ["ano", "tempo", "período", "quando"]):
                return self._analyze_by_year(df, question)
            
            elif any(keyword in question_lower for keyword in ["total", "quantos", "número"]) and "o que" not in question_lower:
                return self._analyze_totals(df, question)
            
            # Explicações conceituais (não análise de dados)
            elif any(keyword in question_lower for keyword in ["o que é", "o que são", "definir", "explicar"]):
                return self._explain_concepts_or_entities(question)
            
            # Respostas sobre conceitos específicos do IBAMA
            elif any(keyword in question_lower for keyword in ["biopirataria", "org. gen.", "modificação genética", "organismo"]):
                return self._explain_concepts(question)
            
            elif any(keyword in question_lower for keyword in ["gravidade", "multa leve", "multa grave"]):
                return self._analyze_gravity(df, question)
            
            elif any(keyword in question_lower for keyword in ["fauna", "flora", "animal", "planta"]) and "o que" not in question_lower:
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
        """Analisa os estados com mais infrações usando contagem correta."""
        try:
            if 'UF' not in df.columns:
                return {"answer": "❌ Coluna UF não encontrada nos dados.", "source": "error"}
            
            # Conta infrações únicas por estado se NUM_AUTO_INFRACAO disponível
            if 'NUM_AUTO_INFRACAO' in df.columns:
                state_counts = df.groupby('UF')['NUM_AUTO_INFRACAO'].nunique().sort_values(ascending=False)
                method_info = "infrações únicas"
            else:
                # Fallback para contagem de registros
                state_counts = df['UF'].value_counts()
                method_info = "registros (pode incluir duplicatas)"
            
            # Extrai número do top (padrão 5)
            import re
            numbers = re.findall(r'\d+', question)
            top_n = int(numbers[0]) if numbers else 5
            top_n = min(top_n, 15)  # Máximo 15
            
            top_states = state_counts.head(top_n)
            
            # Formata resposta
            answer = f"**🏆 Top {top_n} Estados com Mais Infrações:**\n\n"
            for i, (uf, count) in enumerate(top_states.items(), 1):
                percentage = (count / state_counts.sum()) * 100
                answer += f"{i}. **{uf}**: {count:,} infrações ({percentage:.1f}%)\n"
            
            answer += f"\n📊 Total analisado: {state_counts.sum():,} {method_info}"
            
            return {"answer": answer, "source": "data_analysis"}
            
        except Exception as e:
            return {"answer": f"❌ Erro ao analisar estados: {e}", "source": "error"}
    
    def _analyze_top_municipalities(self, df: pd.DataFrame, question: str) -> Dict[str, Any]:
        """Analisa os municípios com mais infrações usando contagem correta."""
        try:
            # Verifica colunas disponíveis
            required_base_cols = ['UF', 'MUNICIPIO']
            if not all(col in df.columns for col in required_base_cols):
                return {"answer": "❌ Colunas necessárias não encontradas.", "source": "error"}
            
            # Remove valores vazios
            df_clean = df[
                df['MUNICIPIO'].notna() & 
                df['UF'].notna() &
                (df['MUNICIPIO'] != '') & 
                (df['UF'] != '')
            ].copy()
            
            if df_clean.empty:
                return {"answer": "❌ Nenhum dado válido encontrado.", "source": "error"}
            
            # Método preferido: usar código do município se disponível
            if 'COD_MUNICIPIO' in df.columns and 'NUM_AUTO_INFRACAO' in df.columns:
                df_clean = df_clean[
                    df_clean['COD_MUNICIPIO'].notna() & 
                    df_clean['NUM_AUTO_INFRACAO'].notna() &
                    (df_clean['COD_MUNICIPIO'] != '') &
                    (df_clean['NUM_AUTO_INFRACAO'] != '')
                ]
                
                if df_clean.empty:
                    return {"answer": "❌ Códigos de município não disponíveis.", "source": "error"}
                
                # Conta INFRAÇÕES ÚNICAS por código do município
                muni_data = df_clean.groupby(['COD_MUNICIPIO', 'MUNICIPIO', 'UF'])['NUM_AUTO_INFRACAO'].nunique().reset_index()
                muni_data.rename(columns={'NUM_AUTO_INFRACAO': 'count'}, inplace=True)
                muni_data = muni_data.sort_values('count', ascending=False)
                
                method_info = "contagem por código IBGE + infrações únicas"
                
            elif 'NUM_AUTO_INFRACAO' in df.columns:
                # Fallback: usar nome do município com contagem única
                df_clean = df_clean[
                    df_clean['NUM_AUTO_INFRACAO'].notna() &
                    (df_clean['NUM_AUTO_INFRACAO'] != '')
                ]
                
                muni_data = df_clean.groupby(['MUNICIPIO', 'UF'])['NUM_AUTO_INFRACAO'].nunique().reset_index()
                muni_data.rename(columns={'NUM_AUTO_INFRACAO': 'count'}, inplace=True)
                muni_data = muni_data.sort_values('count', ascending=False)
                
                method_info = "contagem por nome + infrações únicas"
                
            else:
                # Último fallback: contagem simples de registros
                muni_data = df_clean.groupby(['MUNICIPIO', 'UF']).size().reset_index(name='count')
                muni_data = muni_data.sort_values('count', ascending=False)
                
                method_info = "contagem por nome (pode incluir duplicatas)"
            
            # Extrai número do top (padrão 5)
            import re
            numbers = re.findall(r'\d+', question)
            top_n = int(numbers[0]) if numbers else 5
            top_n = min(top_n, 15)  # Máximo 15
            
            top_munis = muni_data.head(top_n)
            
            answer = f"**🏙️ Top {top_n} Municípios com Mais Infrações:**\n\n"
            for i, row in enumerate(top_munis.itertuples(), 1):
                suffix = " únicas" if "únicas" in method_info else ""
                answer += f"{i}. **{row.MUNICIPIO} ({row.UF})**: {row.count:,} infrações{suffix}\n"
            
            answer += f"\n📊 Total de municípios únicos: {len(muni_data):,}"
            answer += f"\n*Método: {method_info}*"
            
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
            
            # Verifica se a pergunta é específica sobre a maior multa
            question_lower = question.lower()
            if any(keyword in question_lower for keyword in ["maior multa", "quem", "pessoa", "empresa", "infrator"]):
                # Encontra a maior multa e quem foi multado
                max_idx = df_valid['VAL_NUMERIC'].idxmax()
                max_row = df_valid.loc[max_idx]
                max_value = max_row['VAL_NUMERIC']
                
                # Informações do infrator
                infrator = max_row.get('NOME_INFRATOR', 'Não informado')
                uf = max_row.get('UF', 'N/A')
                municipio = max_row.get('MUNICIPIO', 'N/A')
                tipo_infracao = max_row.get('TIPO_INFRACAO', 'Não especificado')
                data = max_row.get('DAT_HORA_AUTO_INFRACAO', 'N/A')
                
                def format_currency(value):
                    if value >= 1_000_000_000:
                        return f"R$ {value/1_000_000_000:.1f} bilhões"
                    elif value >= 1_000_000:
                        return f"R$ {value/1_000_000:.1f} milhões"
                    else:
                        return f"R$ {value:,.2f}"
                
                answer = f"**💰 Maior Multa Aplicada:**\n\n"
                answer += f"• **Valor**: {format_currency(max_value)}\n"
                answer += f"• **Infrator**: {infrator}\n"
                answer += f"• **Local**: {municipio} - {uf}\n"
                answer += f"• **Tipo de Infração**: {tipo_infracao}\n"
                if data != 'N/A':
                    try:
                        data_formatada = pd.to_datetime(data).strftime('%d/%m/%Y')
                        answer += f"• **Data**: {data_formatada}\n"
                    except:
                        answer += f"• **Data**: {data}\n"
                
                return {"answer": answer, "source": "data_analysis"}
            
            else:
                # Análise geral dos valores
                total_value = df_valid['VAL_NUMERIC'].sum()
                avg_value = df_valid['VAL_NUMERIC'].mean()
                max_value = df_valid['VAL_NUMERIC'].max()
                
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
            
            # Conta infrações únicas por tipo se NUM_AUTO_INFRACAO disponível
            if 'NUM_AUTO_INFRACAO' in df_clean.columns:
                type_counts = df_clean.groupby('TIPO_INFRACAO')['NUM_AUTO_INFRACAO'].nunique().sort_values(ascending=False).head(10)
                method_info = "infrações únicas"
            else:
                type_counts = df_clean['TIPO_INFRACAO'].value_counts().head(10)
                method_info = "registros"
            
            answer = f"**📋 Principais Tipos de Infrações ({method_info}):**\n\n"
            for i, (tipo, count) in enumerate(type_counts.items(), 1):
                percentage = (count / type_counts.sum()) * 100
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
            
            # Conta infrações únicas por ano se NUM_AUTO_INFRACAO disponível
            if 'NUM_AUTO_INFRACAO' in df_with_date.columns:
                year_counts = df_with_date.groupby(df_with_date['DATE'].dt.year)['NUM_AUTO_INFRACAO'].nunique().sort_index()
                method_info = "infrações únicas"
            else:
                year_counts = df_with_date['DATE'].dt.year.value_counts().sort_index()
                method_info = "registros"
            
            answer = f"**📅 Infrações por Ano ({method_info}):**\n\n"
            for year, count in year_counts.tail(5).items():
                answer += f"• **{int(year)}**: {count:,} infrações\n"
            
            answer += f"\n📊 Total com data válida: {len(df_with_date):,}"
            
            return {"answer": answer, "source": "data_analysis"}
            
        except Exception as e:
            return {"answer": f"❌ Erro ao analisar por ano: {e}", "source": "error"}
    
    def _analyze_totals(self, df: pd.DataFrame, question: str) -> Dict[str, Any]:
        """Analisa totais gerais usando contagem correta."""
        try:
            # Conta infrações únicas se NUM_AUTO_INFRACAO disponível
            if 'NUM_AUTO_INFRACAO' in df.columns:
                total_records = df['NUM_AUTO_INFRACAO'].nunique()
                records_method = "infrações únicas"
            else:
                total_records = len(df)
                records_method = "registros (pode incluir duplicatas)"
            
            total_states = df['UF'].nunique() if 'UF' in df.columns else 0
            
            # Usa código do município se disponível (mais preciso)
            if 'COD_MUNICIPIO' in df.columns:
                total_municipalities = df['COD_MUNICIPIO'].nunique()
                municipality_method = "por código IBGE"
            elif 'MUNICIPIO' in df.columns:
                total_municipalities = df['MUNICIPIO'].nunique()
                municipality_method = "por nome (pode haver duplicatas)"
            else:
                total_municipalities = 0
                municipality_method = "não disponível"
            
            answer = "**📊 Resumo Geral dos Dados:**\n\n"
            answer += f"• **Total de infrações**: {total_records:,} ({records_method})\n"
            answer += f"• **Estados envolvidos**: {total_states}\n"
            answer += f"• **Municípios afetados**: {total_municipalities:,}\n"
            
            if municipality_method != "não disponível":
                answer += f"  *(contagem {municipality_method})*\n"
            
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
            
            # Conta infrações únicas por gravidade se NUM_AUTO_INFRACAO disponível
            if 'NUM_AUTO_INFRACAO' in df_clean.columns:
                gravity_counts = df_clean.groupby('GRAVIDADE_INFRACAO')['NUM_AUTO_INFRACAO'].nunique()
                method_info = "infrações únicas"
            else:
                gravity_counts = df_clean['GRAVIDADE_INFRACAO'].value_counts()
                method_info = "registros"
            
            answer = f"**⚖️ Distribuição por Gravidade ({method_info}):**\n\n"
            
            for gravity, count in gravity_counts.items():
                percentage = (count / gravity_counts.sum()) * 100
                
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
            
            answer += f"\n📊 Total analisado: {gravity_counts.sum():,} {method_info}"
            
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
            
            # Conta infrações únicas se NUM_AUTO_INFRACAO disponível
            if 'NUM_AUTO_INFRACAO' in df_clean.columns:
                fauna_count = df_clean[fauna_mask]['NUM_AUTO_INFRACAO'].nunique()
                flora_count = df_clean[flora_mask]['NUM_AUTO_INFRACAO'].nunique()
                method_info = "infrações únicas"
            else:
                fauna_count = fauna_mask.sum()
                flora_count = flora_mask.sum()
                method_info = "registros"
            
            answer = f"**🌿 Análise de Infrações Fauna e Flora ({method_info}):**\n\n"
            
            if fauna_count > 0:
                answer += f"🐾 **Infrações contra Fauna**: {fauna_count:,} casos\n"
                if 'NUM_AUTO_INFRACAO' in df_clean.columns:
                    fauna_types = df_clean[fauna_mask].groupby('TIPO_INFRACAO')['NUM_AUTO_INFRACAO'].nunique().nlargest(5)
                else:
                    fauna_types = df_clean[fauna_mask]['TIPO_INFRACAO'].value_counts().head(5)
                for tipo, count in fauna_types.items():
                    answer += f"   • {tipo.title()}: {count:,}\n"
                answer += "\n"
            
            if flora_count > 0:
                answer += f"🌳 **Infrações contra Flora**: {flora_count:,} casos\n"
                if 'NUM_AUTO_INFRACAO' in df_clean.columns:
                    flora_types = df_clean[flora_mask].groupby('TIPO_INFRACAO')['NUM_AUTO_INFRACAO'].nunique().nlargest(5)
                else:
                    flora_types = df_clean[flora_mask]['TIPO_INFRACAO'].value_counts().head(5)
                for tipo, count in flora_types.items():
                    answer += f"   • {tipo.title()}: {count:,}\n"
                answer += "\n"
            
            if 'NUM_AUTO_INFRACAO' in df_clean.columns:
                other_count = df_clean['NUM_AUTO_INFRACAO'].nunique() - fauna_count - flora_count
            else:
                other_count = len(df_clean) - fauna_count - flora_count
            
            answer += f"⚖️ **Outras infrações**: {other_count:,} casos\n"
            
            total_analyzed = fauna_count + flora_count + other_count
            answer += f"\n📊 Total analisado: {total_analyzed:,} {method_info}"
            
            return {"answer": answer, "source": "data_analysis"}
            
        except Exception as e:
            return {"answer": f"❌ Erro ao analisar fauna e flora: {e}", "source": "error"}
    
    def _analyze_specific_region_type(self, df: pd.DataFrame, question: str) -> Dict[str, Any]:
        """Analisa infrações específicas por região e tipo."""
        try:
            question_lower = question.lower()
            
            # Identifica UF
            uf_map = {
                "amazonas": "AM", "rio grande do sul": "RS", "são paulo": "SP", 
                "minas gerais": "MG", "bahia": "BA", "paraná": "PR"
            }
            
            target_uf = None
            for state_name, uf_code in uf_map.items():
                if state_name in question_lower:
                    target_uf = uf_code
                    break
            
            if not target_uf:
                return {"answer": "❌ Estado não identificado na pergunta.", "source": "error"}
            
            # Filtra por UF
            df_uf = df[df['UF'] == target_uf] if 'UF' in df.columns else df
            
            if df_uf.empty:
                return {"answer": f"❌ Nenhum registro encontrado para {target_uf}.", "source": "error"}
            
            # Identifica tipo de infração
            infraction_type = None
            if "pesca" in question_lower:
                df_filtered = df_uf[df_uf['TIPO_INFRACAO'].str.contains('pesca', case=False, na=False)]
                infraction_type = "Pesca"
            elif "fauna" in question_lower:
                df_filtered = df_uf[df_uf['TIPO_INFRACAO'].str.contains('fauna', case=False, na=False)]
                infraction_type = "Fauna"
            elif "flora" in question_lower:
                df_filtered = df_uf[df_uf['TIPO_INFRACAO'].str.contains('flora', case=False, na=False)]
                infraction_type = "Flora"
            else:
                df_filtered = df_uf
                infraction_type = "Todas"
            
            if df_filtered.empty:
                return {"answer": f"❌ Nenhuma infração de {infraction_type} encontrada em {target_uf}.", "source": "error"}
            
            # Filtra por ano se especificado
            if "2024" in question_lower:
                df_filtered['DATE'] = pd.to_datetime(df_filtered['DAT_HORA_AUTO_INFRACAO'], errors='coerce')
                df_filtered = df_filtered[df_filtered['DATE'].dt.year == 2024]
            
            if df_filtered.empty:
                return {"answer": f"❌ Nenhum registro encontrado para os critérios especificados.", "source": "error"}
            
            # Analisa infratores
            if 'NOME_INFRATOR' not in df_filtered.columns:
                return {"answer": "❌ Coluna de infratores não encontrada.", "source": "error"}
            
            # Identifica se quer pessoas físicas ou empresas
            if "pessoas físicas" in question_lower:
                # Filtra pessoas físicas (heurística: nomes com espaços, sem LTDA/SA)
                mask = ~df_filtered['NOME_INFRATOR'].str.contains(r'(LTDA|S\.A\.|S/A|EMPRESA|CIA|COMPANHIA)', case=False, na=False)
                df_people = df_filtered[mask & df_filtered['NOME_INFRATOR'].str.contains(' ', na=False)]
                entity_type = "Pessoas Físicas"
            elif "empresas" in question_lower:
                # Filtra empresas (contém LTDA, SA, etc.)
                mask = df_filtered['NOME_INFRATOR'].str.contains(r'(LTDA|S\.A\.|S/A|EMPRESA|CIA|COMPANHIA)', case=False, na=False)
                df_people = df_filtered[mask]
                entity_type = "Empresas"
            else:
                df_people = df_filtered
                entity_type = "Infratores"
            
            if df_people.empty:
                return {"answer": f"❌ Nenhuma {entity_type.lower()} encontrada para {infraction_type} em {target_uf}.", "source": "error"}
            
            # Top infratores com contagem correta
            import re
            numbers = re.findall(r'\d+', question_lower)
            top_n = int(numbers[0]) if numbers else 5
            
            # Conta infrações únicas por infrator se NUM_AUTO_INFRACAO disponível
            if 'NUM_AUTO_INFRACAO' in df_people.columns:
                top_offenders = df_people.groupby('NOME_INFRATOR')['NUM_AUTO_INFRACAO'].nunique().nlargest(top_n)
                method_info = "infrações únicas"
            else:
                top_offenders = df_people['NOME_INFRATOR'].value_counts().head(top_n)
                method_info = "registros"
            
            answer = f"**🎯 Top {top_n} {entity_type} - {infraction_type} em {target_uf}:**\n\n"
            
            for i, (name, count) in enumerate(top_offenders.items(), 1):
                # Trunca nomes muito longos
                display_name = name[:50] + "..." if len(name) > 50 else name
                suffix = " únicas" if "únicas" in method_info else ""
                answer += f"{i}. **{display_name.title()}**: {count:,} infrações{suffix}\n"
            
            answer += f"\n📊 Total de {entity_type.lower()}: {df_people['NOME_INFRATOR'].nunique():,}"
            answer += f"\n📊 Total de infrações de {infraction_type}: {len(df_people):,}"
            answer += f"\n*Método: {method_info}*"
            
            return {"answer": answer, "source": "data_analysis"}
            
        except Exception as e:
            return {"answer": f"❌ Erro na análise específica: {e}", "source": "error"}
    
    def _analyze_top_offenders_detailed(self, df: pd.DataFrame, question: str) -> Dict[str, Any]:
        """Análise detalhada de infratores."""
        try:
            question_lower = question.lower()
            
            if 'NOME_INFRATOR' not in df.columns:
                return {"answer": "❌ Coluna de infratores não encontrada.", "source": "error"}
            
            df_clean = df[df['NOME_INFRATOR'].notna() & (df['NOME_INFRATOR'] != '')]
            
            # Determina se quer pessoas físicas ou empresas
            if "pessoas físicas" in question_lower:
                # Heurística para pessoas físicas
                mask = ~df_clean['NOME_INFRATOR'].str.contains(r'(LTDA|S\.A\.|S/A|EMPRESA|CIA|COMPANHIA)', case=False, na=False)
                df_filtered = df_clean[mask & df_clean['NOME_INFRATOR'].str.contains(' ', na=False)]
                entity_type = "Pessoas Físicas"
            elif "empresas" in question_lower:
                # Heurística para empresas
                mask = df_clean['NOME_INFRATOR'].str.contains(r'(LTDA|S\.A\.|S/A|EMPRESA|CIA|COMPANHIA)', case=False, na=False)
                df_filtered = df_clean[mask]
                entity_type = "Empresas"
            else:
                df_filtered = df_clean
                entity_type = "Infratores"
            
            if df_filtered.empty:
                return {"answer": f"❌ Nenhuma {entity_type.lower()} encontrada.", "source": "error"}
            
            # Top N
            import re
            numbers = re.findall(r'\d+', question_lower)
            top_n = int(numbers[0]) if numbers else 10
            
            # Conta infrações únicas por infrator se NUM_AUTO_INFRACAO disponível
            if 'NUM_AUTO_INFRACAO' in df_filtered.columns:
                top_offenders = df_filtered.groupby('NOME_INFRATOR')['NUM_AUTO_INFRACAO'].nunique().nlargest(top_n)
                method_info = "infrações únicas"
            else:
                top_offenders = df_filtered['NOME_INFRATOR'].value_counts().head(top_n)
                method_info = "registros"
            
            answer = f"**👥 Top {top_n} {entity_type} com Mais Infrações ({method_info}):**\n\n"
            
            for i, (name, count) in enumerate(top_offenders.items(), 1):
                # Informações adicionais do infrator
                offender_data = df_filtered[df_filtered['NOME_INFRATOR'] == name]
                ufs = offender_data['UF'].unique() if 'UF' in offender_data.columns else []
                
                # Trunca nome se muito longo
                display_name = name[:40] + "..." if len(name) > 40 else name
                
                answer += f"{i}. **{display_name.title()}**\n"
                suffix = " únicas" if "únicas" in method_info else ""
                answer += f"   • Infrações: {count:,}{suffix}\n"
                if len(ufs) > 0:
                    answer += f"   • Estados: {', '.join(ufs[:3])}{'...' if len(ufs) > 3 else ''}\n\n"
                else:
                    answer += "\n"
            
            return {"answer": answer, "source": "data_analysis"}
            
        except Exception as e:
            return {"answer": f"❌ Erro na análise de infratores: {e}", "source": "error"}
    
    def _explain_concepts_or_entities(self, question: str) -> Dict[str, Any]:
        """Explica conceitos ou entidades específicas."""
        question_lower = question.lower()
        
        if "vale" in question_lower:
            return {
                "answer": """**⛰️ Vale S.A.:**

**Nome oficial:** Vale S.A. (antiga Companhia Vale do Rio Doce)

**Sobre a empresa:**
• Uma das maiores mineradoras do mundo
• Maior produtora de minério de ferro e níquel
• Fundada em 1942, privatizada em 1997
• Sede no Rio de Janeiro

**Relação com o IBAMA:**
• Licenciamento de projetos de mineração
• Monitoramento de impactos ambientais
• Fiscalização de barragens de rejeitos
• Controle de desmatamento e recuperação

**Principais questões ambientais:**
• Rompimento de barragens (Mariana 2015, Brumadinho 2019)
• Impactos na qualidade da água
• Desmatamento para mineração
• Poluição do ar por particulados

*A Vale frequentemente aparece em processos do IBAMA devido ao porte de suas operações de mineração e histórico de acidentes ambientais.*""",
                "source": "knowledge_base"
            }
        
        elif "infrações contra fauna" in question_lower:
            return {
                "answer": """**🐾 Infrações Contra a Fauna:**

**Definição:** Crimes que prejudicam animais silvestres e seus habitats naturais.

**Principais tipos:**
• **Caça ilegal:** Abate de animais protegidos
• **Captura:** Retirada de animais da natureza
• **Comercialização:** Venda de animais ou produtos
• **Maus-tratos:** Ferimentos ou morte de animais
• **Destruição de habitat:** Alteração de áreas de reprodução

**Exemplos específicos:**
• Caça de onças, jaguatiricas, aves raras
• Captura de papagaios, araras, tucanos
• Pesca predatória e em locais proibidos
• Comercialização de peles, penas, carne
• Destruição de ninhos e criadouros

**Penalidades (Lei 9.605/98):**
• Multa: R$ 500 a R$ 5.000 por espécime
• Detenção: 6 meses a 1 ano
• Apreensão dos animais
• Reparação de danos ambientais

**Agravantes:**
• Espécies ameaçadas de extinção
• Períodos de reprodução
• Uso de métodos cruéis
• Finalidade comercial""",
                "source": "knowledge_base"
            }
        
        else:
            # Chama o método original para outros conceitos
            return self._explain_concepts(question)
    
    def _analyze_general(self, df: pd.DataFrame, question: str) -> Dict[str, Any]:
        """Análise genérica dos dados ou responde perguntas gerais."""
        question_lower = question.lower()
        
        # Perguntas sobre entidades específicas
        if "petrobras" in question_lower:
            return {
                "answer": """**🛢️ Petrobras:**

**Nome oficial:** Petróleo Brasileiro S.A.

**Sobre a empresa:**
• Maior empresa do Brasil e uma das maiores petrolíferas do mundo
• Sociedade anônima de capital misto (pública e privada)
• Fundada em 1953 pelo presidente Getúlio Vargas
• Atua em exploração, produção, refino e distribuição de petróleo

**Relação com o IBAMA:**
• Licenciamento ambiental para exploração de petróleo
• Monitoramento de impactos ambientais
• Fiscalização de vazamentos e acidentes
• Controle de atividades offshore (mar)

**Principais questões ambientais:**
• Vazamentos de óleo
• Impactos na fauna marinha
• Licenciamento de plataformas
• Recuperação de áreas degradadas

*A Petrobras frequentemente aparece em processos do IBAMA devido ao porte de suas operações e potencial impacto ambiental.*""",
                "source": "knowledge_base"
            }
        
        elif "ibama" in question_lower:
            return {
                "answer": """**🌳 Instituto Brasileiro do Meio Ambiente (IBAMA):**

**Criação:** 1989, pela Lei 7.735

**Missão:** Proteger o meio ambiente e promover o desenvolvimento sustentável

**Principais funções:**
• Fiscalização ambiental
• Licenciamento de atividades
• Proteção da fauna e flora
• Controle de produtos químicos
• Gestão de unidades de conservação

**Tipos de infração:**
• Contra a fauna (caça, pesca ilegal)
• Contra a flora (desmatamento)
• Poluição (água, ar, solo)
• Atividades sem licença

**Penalidades:**
• Multas de R$ 50 a R$ 50 milhões
• Apreensão de produtos
• Embargo de atividades
• Recuperação de danos""",
                "source": "knowledge_base"
            }
        
        else:
            # Resposta genérica com dados disponíveis
            if not df.empty:
                # Conta infrações únicas se NUM_AUTO_INFRACAO disponível
                if 'NUM_AUTO_INFRACAO' in df.columns:
                    total_records = df['NUM_AUTO_INFRACAO'].nunique()
                    records_type = "infrações únicas"
                else:
                    total_records = len(df)
                    records_type = "registros"
                
                total_states = df['UF'].nunique() if 'UF' in df.columns else 0
                
                if 'COD_MUNICIPIO' in df.columns:
                    total_municipalities = df['COD_MUNICIPIO'].nunique()
                elif 'MUNICIPIO' in df.columns:
                    total_municipalities = df['MUNICIPIO'].nunique()
                else:
                    total_municipalities = 0
                
                answer = f"📊 **Sistema de Análise IBAMA:**\n\n"
                answer += f"Tenho {total_records:,} {records_type} disponíveis para análise.\n\n"
                answer += f"**Dados incluem:**\n"
                answer += f"• {total_states} estados brasileiros\n"
                answer += f"• {total_municipalities:,} municípios afetados\n"
                answer += f"• Período: 2024-2025\n"
                answer += f"• Valores de multas, tipos de infração, gravidade\n\n"
                
                answer += f"**Posso ajudar com:**\n"
                answer += f"• Análise por estado/município\n"
                answer += f"• Valores e estatísticas de multas\n"
                answer += f"• Tipos de infrações mais comuns\n"
                answer += f"• Distribuição por gravidade\n"
                answer += f"• Conceitos ambientais (biopirataria, OGMs)\n"
                answer += f"• Informações sobre IBAMA e legislação\n\n"
                
                answer += f"**Exemplo:** 'Quais são os 5 estados com mais infrações?'"
            else:
                answer = "❌ Não foi possível carregar os dados para análise."
            
            return {"answer": answer, "source": "data_analysis"}
    
    def _add_ai_warning(self, answer: str, source: str) -> str:
        """Adiciona aviso sobre IA a todas as respostas."""
        # Sempre adiciona o aviso, independente da fonte
        warning = "\n\n⚠️ **Aviso Importante:** Todas as respostas precisam ser checadas. Os modelos de IA podem ter erros de alucinação, baixa qualidade em certos pontos, vieses ou problemas éticos."
        
        # Adiciona informação sobre a fonte
        if source == "data_analysis":
            source_info = "\n\n*💡 Resposta baseada em análise direta dos dados*"
        elif source == "knowledge_base":
            source_info = "\n\n*📚 Resposta baseada em conhecimento especializado*"
        elif source == "llm":
            model_name = "Llama 3.1" if self.llm_config["provider"] == "groq" else "Gemini 1.5"
            source_info = f"\n\n*🤖 Resposta gerada por {model_name}*"
        else:
            source_info = ""
        
        return answer + source_info + warning
    
    def query(self, question: str, provider: str = 'direct') -> Dict[str, Any]:
        """Processa uma pergunta do usuário."""
        
        question_lower = question.lower()
        
        # Palavras-chave que indicam perguntas sobre dados ou conceitos (não web)
        data_keywords = [
            "estados", "uf", "municípios", "cidades", "valor", "multa", 
            "tipo", "infração", "ano", "total", "quantos", "top", "maior", "menor",
            "biopirataria", "org. gen.", "modificação genética", "organismo",
            "gravidade", "leve", "grave", "gravíssima", "fauna", "flora", 
            "animal", "planta", "ibama", "ambiental", "petrobras", "empresa",
            "pessoa", "infrator", "quem", "qual", "o que é", "vale", "mineradora",
            "pesca", "amazonas", "rio grande do sul", "pessoas físicas", "empresas",
            "infrações contra", "conceito", "definição"
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
                    return self.llm_integration.query(question, self.llm_config["provider"])
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
        
        # Header com informações do modelo atual
        col1, col2, col3 = st.columns([2, 1, 1])
        
        with col1:
            st.subheader("💬 Chatbot Inteligente")
        
        with col2:
            # Indicador do modelo atual
            model_emoji = "🦙" if self.llm_config["provider"] == "groq" else "💎"
            model_name = "Llama 3.1" if self.llm_config["provider"] == "groq" else "Gemini 1.5"
            st.caption(f"{model_emoji} Usando: {model_name}")
        
        with col3:
            # Botão para limpar cache
            if st.button("🔄 Recarregar", help="Limpa cache e recarrega dados"):
                self.cached_data = None
                st.success("Cache limpo!")
        
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
                model_emoji = "🦙" if self.llm_config["provider"] == "groq" else "💎"
                with st.spinner(f"{model_emoji} A IA está analisando os dados..."):
                    try:
                        response = self.query(prompt)
                        answer = response.get("answer", "❌ Não foi possível processar sua pergunta.")
                        source = response.get("source", "unknown")
                        
                        # Adiciona aviso obrigatório sobre IA a TODAS as respostas
                        final_answer = self._add_ai_warning(answer, source)
                        
                        st.markdown(final_answer)
                        
                        # Adiciona ao histórico
                        st.session_state.messages.append({"role": "assistant", "content": final_answer})
                        
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
                "A maior multa foi de qual pessoa ou empresa?",
                "Top 5 pessoas físicas com mais infrações por Pesca no Amazonas",
                "Top 5 empresas com mais infrações por Fauna no RS em 2024"
            ]
            
            for question in data_questions:
                if st.button(question, key=f"data_{hash(question)}"):
                    self._handle_sample_question(question)
            
            st.write("**🧬 Conceitos e Entidades:**")
            concept_questions = [
                "O que é biopirataria?",
                "O que é a Vale?",
                "O que são infrações contra fauna?",
                "Como funciona o IBAMA?"
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
        source = response.get("source", "unknown")
        
        # Adiciona aviso obrigatório sobre IA a TODAS as respostas
        final_answer = self._add_ai_warning(answer, source)
        
        st.session_state.messages.append({"role": "assistant", "content": final_answer})
        st.rerun()
