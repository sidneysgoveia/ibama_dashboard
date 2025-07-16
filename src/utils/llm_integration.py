import os
import json
import re  # Importa o módulo de expressões regulares
from typing import Dict, Any, Optional, Literal
from openai import OpenAI
import google.generativeai as genai
import pandas as pd

import config
from src.utils.tools import search_internet

class LLMIntegration:
    def __init__(self, database=None):
        self.database = database
        self.groq_client = None
        self.gemini_model = None
        self.groq_model_name = "llama-3.1-70b-versatile"
        self.gemini_model_name = "gemini-1.5-pro"

        groq_api_key = config.get_secret('GROQ_API_KEY')
        if groq_api_key:
            self.groq_client = OpenAI(api_key=groq_api_key, base_url="https://api.groq.com/openai/v1")

        google_api_key = config.get_secret('GOOGLE_API_KEY')
        if google_api_key:
            genai.configure(api_key=google_api_key)
            self.gemini_model = genai.GenerativeModel(model_name=self.gemini_model_name)

    def _get_system_prompt(self) -> str:
        try:
            schema_df = self.database.get_table_info()
            schema_str = "\n".join([f'- "{row["name"]}" ({row["type"]})' for _, row in schema_df.iterrows()])
        except Exception:
            schema_str = "Não foi possível carregar o esquema da tabela."
        
        if self.database.is_cloud:
            sql_dialect_instructions = """
            Você gera código SQL para PostgreSQL.
            Regras IMPORTANTÍSSIMAS para PostgreSQL:
            1.  Sempre coloque os nomes das colunas entre aspas duplas (ex: "UF").
            2.  Para análises temporais, use: EXTRACT(YEAR FROM TO_TIMESTAMP("DAT_HORA_AUTO_INFRACAO", 'YYYY-MM-DD HH24:MI:SS')).
            3.  Para cálculos em "VAL_AUTO_INFRACAO", use: CAST(REPLACE("VAL_AUTO_INFRACAO", ',', '.') AS NUMERIC).
            """
        else:
            sql_dialect_instructions = """
            Você gera código SQL para DuckDB.
            Regras IMPORTANTÍSSIMAS para DuckDB:
            1.  Para análises temporais, use: EXTRACT(YEAR FROM TRY_CAST("DAT_HORA_AUTO_INFRACAO" AS TIMESTAMP)).
            2.  Para cálculos em "VAL_AUTO_INFRACAO", use: CAST(REPLACE("VAL_AUTO_INFRACAO", ',', '.') AS DOUBLE).
            """

        return f"""
        Você é um assistente especialista em dados do IBAMA. Sua função é gerar uma única consulta SQL para responder à pergunta.
        Retorne APENAS o código SQL, nada mais.

        {sql_dialect_instructions}

        Esquema da tabela `ibama_infracao`:
        {schema_str}
        """

    # --- NOVA FUNÇÃO DE LIMPEZA ---
    def _extract_sql_from_response(self, response_text: str) -> Optional[str]:
        """
        Extrai a primeira consulta SQL de um texto usando expressões regulares.
        Ignora qualquer texto antes do 'SELECT'.
        """
        # Procura por 'SELECT' (case-insensitive) e captura tudo até o final
        match = re.search(r"SELECT\s.*", response_text, re.IGNORECASE | re.DOTALL)
        if match:
            return match.group(0).strip()
        return None

    def query(self, question: str, provider: Literal['groq', 'gemini']) -> Dict[str, Any]:
        tool_choice = self._decide_tool(question)
        
        try:
            if tool_choice == 'internet':
                # ... (lógica da internet permanece a mesma)
                return {"answer": "Busca na internet ainda não implementada neste fluxo.", "source": "internet"}

            elif tool_choice == 'database':
                raw_sql_response = self.generate_sql(question, provider)
                
                # --- CORREÇÃO APLICADA AQUI ---
                # Limpa a resposta da IA antes de usar
                sql_query = self._extract_sql_from_response(raw_sql_response)
                
                if not sql_query:
                    return {"answer": "A IA não conseguiu gerar uma consulta SQL válida. Tente reformular sua pergunta.", "source": "error", "debug_info": {"raw_response": raw_sql_response}}
                
                db_results = self.database.execute_query(sql_query)
                
                if db_results.empty:
                    final_answer = "Não encontrei dados para sua consulta no banco de dados."
                else:
                    final_answer = self._format_results(question, db_results)
                
                return {"answer": final_answer, "source": "database", "debug_info": {"sql_query": sql_query}}

        except Exception as e:
            return {"answer": f"Ocorreu um erro inesperado: {str(e)}", "source": "error"}

    def generate_sql(self, question: str, provider: str) -> str:
        """Gera SQL a partir de uma pergunta. Retorna a resposta bruta da IA."""
        system_prompt = self._get_system_prompt()
        user_prompt = f"Pergunta: {question}"
        
        if provider == 'gemini' and self.gemini_model:
            response = self.gemini_model.generate_content(f"{system_prompt}\n\n{user_prompt}", generation_config={"temperature": 0.0})
            return response.text
        elif provider == 'groq' and self.groq_client:
            response = self.groq_client.chat.completions.create(
                model=self.groq_model_name,
                messages=[{"role": "system", "content": system_prompt}, {"role": "user", "content": user_prompt}],
                temperature=0.0, max_tokens=500
            )
            return response.choices[0].message.content
        return ""

    def _format_results(self, question: str, results: pd.DataFrame) -> str:
        if len(results) == 1 and len(results.columns) == 1:
            value = results.iloc[0, 0]
            if isinstance(value, (int, float)):
                return f"O resultado é: **{value:,.2f}**"
            else:
                return f"O resultado é: **{value}**"
        results.columns = [col.replace('_', ' ').title() for col in results.columns]
        return results.to_markdown(index=False)

    def _decide_tool(self, question: str) -> Literal['database', 'internet']:
        question_lower = question.lower()
        web_keywords = ["endereço", "o que é", "significado de", "site oficial", "telefone", "contato", "história", "quem é o presidente", "localização", "site"]
        if any(keyword in question_lower for keyword in web_keywords):
            return 'internet'
        return 'database'
