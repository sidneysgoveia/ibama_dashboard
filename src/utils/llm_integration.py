
import os
import json
from typing import Dict, Any, Optional, Literal
from openai import OpenAI
import google.generativeai as genai
import pandas as pd

# Importa o módulo de configuração da raiz e a ferramenta de busca
import config
from src.utils.tools import search_internet

class LLMIntegration:
    """
    Classe que funciona como um agente de IA, capaz de interagir com um banco de dados
    e realizar buscas na internet para responder perguntas. Suporta múltiplos
    provedores de LLM (Groq/Llama e Google/Gemini).
    """
    def __init__(self, database=None):
        """Inicializa o agente com acesso ao banco de dados e configura os clientes de LLM."""
        self.database = database
        self.groq_client = None
        self.gemini_model = None
        self.groq_model_name = "llama-3.1-70b-versatile"
        self.gemini_model_name = "gemini-1.5-pro"

        # Configura o cliente Groq (Llama)
        groq_api_key = config.get_secret('GROQ_API_KEY')
        if groq_api_key:
            self.groq_client = OpenAI(api_key=groq_api_key, base_url="https://api.groq.com/openai/v1")

        # Configura o cliente Google (Gemini)
        google_api_key = config.get_secret('GOOGLE_API_KEY')
        if google_api_key:
            genai.configure(api_key=google_api_key)
            self.gemini_model = genai.GenerativeModel(model_name=self.gemini_model_name)

    def _get_system_prompt(self) -> str:
        """Gera o prompt do sistema com o esquema do banco para dar contexto ao LLM."""
        try:
            schema_df = self.database.get_table_info()
            schema_str = "\n".join([f'- "{row["name"]}" ({row["type"]})' for _, row in schema_df.iterrows()])
        except Exception:
            schema_str = "Não foi possível carregar o esquema da tabela."
        
        # --- CORREÇÃO APLICADA AQUI ---
        # As instruções SQL agora dependem do ambiente (nuvem ou local)
        if self.database.is_cloud:
            # Instruções para PostgreSQL (Supabase)
            sql_dialect_instructions = """
            Você gera código SQL para **PostgreSQL**.
            Regras IMPORTANTÍSSIMAS e OBRIGATÓRIAS para PostgreSQL:
            1.  Sempre coloque os nomes das colunas entre aspas duplas (ex: "UF", "VAL_AUTO_INFRACAO").
            2.  Para análises temporais, use a função `TO_TIMESTAMP` para converter a data. Exemplo: `EXTRACT(YEAR FROM TO_TIMESTAMP("DAT_HORA_AUTO_INFRACAO", 'YYYY-MM-DD HH24:MI:SS'))`.
            3.  Para fazer qualquer cálculo (SUM, AVG, etc.) na coluna "VAL_AUTO_INFRACAO", use a expressão EXATA: `CAST(REPLACE("VAL_AUTO_INFRACAO", ',', '.') AS NUMERIC)`.
            """
        else:
            # Instruções para DuckDB (Local)
            sql_dialect_instructions = """
            Você gera código SQL para **DuckDB**.
            Regras IMPORTANTÍSSIMAS e OBRIGATÓRIAS para DuckDB:
            1.  Para análises temporais, use `EXTRACT(YEAR FROM TRY_CAST("DAT_HORA_AUTO_INFRACAO" AS TIMESTAMP)) AS ano`.
            2.  Para fazer qualquer cálculo (SUM, AVG, etc.) na coluna "VAL_AUTO_INFRACAO", use a expressão EXATA: `CAST(REPLACE("VAL_AUTO_INFRACAO", ',', '.') AS DOUBLE)`.
            """

        return f"""
        Você é um assistente especialista em análise de dados ambientais do IBAMA.
        Sua função é gerar uma única consulta SQL para responder à pergunta do usuário.
        Retorne APENAS o código SQL, nada mais.

        {sql_dialect_instructions}

        Esquema da tabela `ibama_infracao`:
        {schema_str}

        Regras Adicionais:
        - Sempre inclua "CPF_CNPJ_INFRATOR" junto com "NOME_INFRATOR" no SELECT.
        - Ao usar funções de agregação como SUM, AVG, COUNT, SEMPRE dê um apelido (alias) para a coluna (ex: `AS total_multas`).
        """

    def _decide_tool(self, question: str) -> Literal['database', 'internet']:
        """Decide qual ferramenta usar com base em uma heurística de palavras-chave."""
        question_lower = question.lower()
        web_keywords = ["endereço", "o que é", "significado de", "site oficial", "telefone", "contato", "história", "quem é o presidente", "localização", "site"]
        if any(keyword in question_lower for keyword in web_keywords):
            return 'internet'
        return 'database'

    def _generate_final_answer_from_text(self, context: str, question: str, provider: str) -> str:
        """Usa o LLM para gerar uma resposta final a partir de um contexto de texto."""
        prompt = f"Com base no seguinte contexto, responda à pergunta do usuário de forma clara e concisa.\n\nContexto:\n{context}\n\nPergunta do Usuário:\n{question}"
        
        if provider == 'gemini' and self.gemini_model:
            response = self.gemini_model.generate_content(prompt, generation_config={"temperature": 0.2})
            return response.text
        elif provider == 'groq' and self.groq_client:
            response = self.groq_client.chat.completions.create(
                model=self.groq_model_name,
                messages=[{"role": "system", "content": "Você é um assistente prestativo."}, {"role": "user", "content": prompt}],
                temperature=0.2
            )
            return response.choices[0].message.content
        return "Provedor de LLM não configurado."

    def query(self, question: str, provider: Literal['groq', 'gemini']) -> Dict[str, Any]:
        """Orquestra o processo completo: decide a ferramenta, a executa e formata a resposta."""
        if (provider == 'groq' and not self.groq_client) or (provider == 'gemini' and not self.gemini_model):
            return {"answer": f"Provedor '{provider}' não configurado. Verifique suas chaves de API.", "source": "error"}

        tool_choice = self._decide_tool(question)
        
        try:
            if tool_choice == 'internet':
                search_results = search_internet(question)
                final_answer = self._generate_final_answer_from_text(search_results, question, provider)
                return {"answer": final_answer, "source": "internet", "debug_info": {"search_query": question, "results": search_results}}
            
            elif tool_choice == 'database':
                sql_query = self.generate_sql(question, provider)
                if not sql_query or not sql_query.strip().lower().startswith('select'):
                    return {"answer": "Não consegui gerar uma consulta SQL válida para sua pergunta. Tente reformular.", "source": "error", "debug_info": {"generated_sql": sql_query}}
                
                db_results = self.database.execute_query(sql_query)
                
                if db_results.empty:
                    final_answer = "Não encontrei dados para sua consulta no banco de dados."
                else:
                    final_answer = self._format_results(question, db_results)
                
                return {"answer": final_answer, "source": "database", "debug_info": {"sql_query": sql_query}}

        except Exception as e:
            return {"answer": f"Ocorreu um erro inesperado ao processar sua solicitação: {str(e)}", "source": "error"}

    def generate_sql(self, question: str, provider: str) -> Optional[str]:
        """Gera SQL a partir de uma pergunta, usando o prompt robusto do sistema."""
        system_prompt = self._get_system_prompt()
        user_prompt = f"Pergunta: {question}"
        
        if provider == 'gemini' and self.gemini_model:
            response = self.gemini_model.generate_content(f"{system_prompt}\n\n{user_prompt}", generation_config={"temperature": 0.0})
            sql = response.text
        elif provider == 'groq' and self.groq_client:
            response = self.groq_client.chat.completions.create(
                model=self.groq_model_name,
                messages=[{"role": "system", "content": system_prompt}, {"role": "user", "content": user_prompt}],
                temperature=0.0, max_tokens=500
            )
            sql = response.choices[0].message.content
        else:
            return None

        sql = sql.strip().replace('```sql', '').replace('```', '').strip()
        return sql

    def _format_results(self, question: str, results: pd.DataFrame) -> str:
        """Formata os resultados de uma consulta SQL em uma resposta de texto amigável."""
        if len(results) == 1 and len(results.columns) == 1:
            value = results.iloc[0, 0]
            if isinstance(value, (int, float)):
                if "valor" in question.lower():
                    return f"O resultado da sua consulta é: **R$ {value:,.2f}**."
                else:
                    return f"O total encontrado é: **{int(value):,}**."
            else:
                return f"O resultado encontrado é: **{value}**"

        results.columns = [col.replace('_', ' ').title() for col in results.columns]
        return results.to_markdown(index=False)
