import os
import json
from typing import Dict, Any, Optional, Literal
from openai import OpenAI
import google.generativeai as genai
from decouple import config
import pandas as pd

# Importa a ferramenta de busca que usa a API da Serper
from src.utils.tools import search_internet

class LLMIntegration:
    # ... (todo o início da classe permanece o mesmo) ...
    def __init__(self, database=None):
        self.database = database
        self.groq_client = None
        self.gemini_model = None
        self.groq_model_name = "llama-3.3-70b-versatile"
        self.gemini_model_name = "gemini-1.5-pro"

        groq_api_key = config('GROQ_API_KEY')
        if groq_api_key:
            self.groq_client = OpenAI(api_key=groq_api_key, base_url="https://api.groq.com/openai/v1")

        google_api_key = config('GOOGLE_API_KEY')
        if google_api_key:
            genai.configure(api_key=google_api_key)
            self.gemini_model = genai.GenerativeModel(model_name=self.gemini_model_name)

    def _get_system_prompt(self) -> str:
        try:
            schema_df = self.database.get_table_info()
            schema_str = "\n".join([f"- {row['name']} ({row['type']})" for _, row in schema_df.iterrows()])
        except Exception:
            schema_str = "Não foi possível carregar o esquema da tabela."
        return f"""
        Você é um assistente especialista em análise de dados ambientais do IBAMA.
        Sua função é usar os dados fornecidos para formular uma resposta clara e completa.
        Esquema da tabela `ibama_infracao`:
        {schema_str}
        """

    def _decide_tool(self, question: str) -> Literal['database', 'internet']:
        question_lower = question.lower()
        
        web_keywords = [
            "endereço", "o que é", "significado de", "site oficial", "telefone", 
            "contato", "história", "quem é o presidente", "localização", "site"
        ]
        if any(keyword in question_lower for keyword in web_keywords):
            return 'internet'

        db_keywords = ["mostre", "liste", "quais são", "infrações", "multas", "autos de infração", "relatório"]
        if "cnpj" in question_lower and not any(keyword in question_lower for keyword in db_keywords):
            return 'internet'

        return 'database'

    def _generate_final_answer_from_text(self, context: str, question: str, provider: str) -> str:
        prompt = f"Com base no seguinte contexto, responda à pergunta do usuário de forma clara e concisa.\n\nContexto:\n{context}\n\nPergunta do Usuário:\n{question}"
        
        if provider == 'gemini' and self.gemini_model:
            response = self.gemini_model.generate_content(prompt, generation_config={"temperature": 0.2})
            return response.text
        elif provider == 'groq' and self.groq_client:
            response = self.groq_client.chat.completions.create(
                model=self.groq_model_name,
                messages=[{"role": "system", "content": self._get_system_prompt()}, {"role": "user", "content": prompt}],
                temperature=0.2
            )
            return response.choices[0].message.content
        return "Provedor de LLM não configurado."

    def query(self, question: str, provider: Literal['groq', 'gemini']) -> Dict[str, Any]:
        if (provider == 'groq' and not self.groq_client) or (provider == 'gemini' and not self.gemini_model):
            return {"answer": f"Provedor '{provider}' não configurado. Verifique suas chaves de API.", "source": "error"}

        tool_choice = self._decide_tool(question)
        print(f"🤖 Ferramenta escolhida: {tool_choice}")

        try:
            if tool_choice == 'internet':
                search_results = search_internet(question)
                final_answer = self._generate_final_answer_from_text(search_results, question, provider)
                return {"answer": final_answer, "source": "internet", "debug_info": {"search_query": question, "results": search_results}}
            
            elif tool_choice == 'database':
                sql_query = self.generate_sql(question, provider)
                if not sql_query:
                    return {"answer": "Não consegui gerar uma consulta SQL. Tente reformular.", "source": "error"}
                
                db_results = self.database.execute_query(sql_query)
                
                if db_results.empty:
                    print("⚠️ Consulta inicial não retornou resultados. Tentando uma consulta de fallback mais ampla...")
                    
                    main_keyword = None
                    if 'fauna' in question.lower(): main_keyword = 'fauna'
                    elif 'flora' in question.lower(): main_keyword = 'flora'
                    elif 'pesca' in question.lower(): main_keyword = 'pesca'
                    
                    if main_keyword:
                        fallback_sql = f"SELECT NOME_INFRATOR, CPF_CNPJ_INFRATOR, TIPO_INFRACAO, DES_INFRACAO FROM ibama_infracao WHERE LOWER(TIPO_INFRACAO) = '{main_keyword}' LIMIT 5"
                        print(f"🔧 Executando consulta de fallback: {fallback_sql}")
                        db_results = self.database.execute_query(fallback_sql)
                        
                        if not db_results.empty:
                            fallback_message = "A sua consulta original foi muito específica e não retornou resultados. Aqui estão alguns registros mais gerais sobre o tema:\n\n"
                            final_answer = fallback_message + self._format_results(question, db_results)
                        else:
                            final_answer = f"Não encontrei dados para '{main_keyword}', mesmo em uma busca mais ampla."
                    else:
                        final_answer = "Não encontrei dados para sua consulta no banco de dados."
                else:
                    final_answer = self._format_results(question, db_results)
                
                return {"answer": final_answer, "source": "database", "debug_info": {"sql_query": sql_query}}

        except Exception as e:
            print(f"❌ Erro ao processar a query: {e}")
            return {"answer": f"Ocorreu um erro inesperado: {str(e)}", "source": "error"}

    def generate_sql(self, question: str, provider: str) -> Optional[str]:
        print("Gerando SQL com LLM...")
        
        prompt = f"""
        Gere uma única consulta SQL para DuckDB para responder à seguinte pergunta.
        Retorne APENAS o código SQL, nada mais.

        {self._get_system_prompt()}

        Pergunta: {question}

        Regras IMPORTANTÍSSIMAS e OBRIGATÓRIAS:
        1. Para análises temporais, use `EXTRACT(YEAR FROM TRY_CAST(DAT_HORA_AUTO_INFRACAO AS TIMESTAMP)) AS ano`.
        2. Para fazer qualquer cálculo (SUM, AVG, etc.) na coluna `VAL_AUTO_INFRACAO`, você DEVE seguir estes dois passos na ordem:
           a. Primeiro, filtre os valores inválidos: `WHERE VAL_AUTO_INFRACAO IS NOT NULL AND VAL_AUTO_INFRACAO != ''`
           b. Segundo, use a expressão EXATA para converter o valor: `CAST(REPLACE(VAL_AUTO_INFRACAO, ',', '.') AS DOUBLE)`
        3. Sempre inclua `CPF_CNPJ_INFRATOR` junto com `NOME_INFRATOR` no SELECT.
        4. Ao usar funções de agregação como SUM, AVG, COUNT, SEMPRE dê um apelido (alias) para a coluna.
        5. INTERPRETAÇÃO DE INTENÇÃO: Se o usuário pedir para "mostrar" algo "com [uma coluna]" (ex: 'infrações de fauna com CNPJ'), isso é um pedido para INCLUIR a coluna no `SELECT`. NÃO adicione um filtro `WHERE` para essa coluna a menos que o usuário peça explicitamente.
        """
        
        if provider == 'gemini' and self.gemini_model:
            response = self.gemini_model.generate_content(prompt, generation_config={"temperature": 0.0})
            sql = response.text
        elif provider == 'groq' and self.groq_client:
            response = self.groq_client.chat.completions.create(
                model=self.groq_model_name,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.0, max_tokens=500
            )
            sql = response.choices[0].message.content
        else:
            return None

        sql = sql.strip().replace('```sql', '').replace('```', '').strip()
        return sql if sql.lower().startswith(('select', 'with')) else None

    def _fix_encoding(self, text: str) -> str:
        """Corrige problemas comuns de codificação de caracteres."""
        if not isinstance(text, str):
            return text
        # Mapeamento dos erros de encoding mais comuns
        replacements = {
            'Ã§': 'ç', 'Ã‡': 'Ç',
            'Ã£': 'ã', 'Ãµ': 'õ',
            'Ã¡': 'á', 'Ã©': 'é', 'Ã­': 'í', 'Ã³': 'ó', 'Ãº': 'ú',
            'Ã¢': 'â', 'Ãª': 'ê', 'Ã´': 'ô',
            'Ã ': 'à',
            'Âº': 'º', 'Âª': 'ª'
        }
        for bad, good in replacements.items():
            text = text.replace(bad, good)
        return text

    def _format_results(self, question: str, results: pd.DataFrame) -> str:
        """Formata os resultados de uma consulta SQL em uma resposta de texto amigável."""
        # --- ALTERAÇÃO AQUI: Aplica a correção de encoding em todas as colunas de texto ---
        for col in results.select_dtypes(include=['object']).columns:
            results[col] = results[col].apply(self._fix_encoding)

        if len(results) == 1 and len(results.columns) == 1:
            value = results.iloc[0, 0]
            if isinstance(value, (int, float)):
                if "valor" in question.lower():
                    return f"O resultado da sua consulta é: **R$ {value:,.2f}**."
                else:
                    return f"O total encontrado é: **{int(value):,}**."
            else:
                return f"O resultado encontrado é: **{value}**"

        results.columns = [self._fix_encoding(col.replace('_', ' ').title()) for col in results.columns]
        return results.to_markdown(index=False)