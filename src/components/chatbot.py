import streamlit as st
# --- ALTERAÇÃO AQUI: Adicionando a importação que faltava ---
import json
from typing import Dict, Any, List

class Chatbot:
    def __init__(self, llm_integration=None):
        """Initialize the chatbot component"""
        self.llm_integration = llm_integration
        
    def initialize_chat_state(self):
        """Initialize or reset the chat state in the session"""
        if "messages" not in st.session_state:
            st.session_state.messages = []
        
        if "debug_mode" not in st.session_state:
            st.session_state.debug_mode = False
            
        if "llm_provider" not in st.session_state:
            st.session_state.llm_provider = "gemini"

    def display_chat_interface(self):
        """Display the chat interface in Streamlit"""
        with st.sidebar.expander("⚙️ Configurações do Chat", expanded=True):
            st.session_state.llm_provider = st.radio(
                "Escolha o Modelo de IA:",
                options=['gemini', 'groq'],
                format_func=lambda x: "Google (Gemini 1.5 Pro)" if x == 'gemini' else "Groq (Llama 3.1 70B)",
                help="Gemini é melhor para buscas na web. Llama é mais rápido para consultas de dados."
            )
            
            st.session_state.debug_mode = st.checkbox(
                "Modo Debug", 
                value=st.session_state.debug_mode,
                help="Mostra o 'raciocínio' da IA, como a consulta SQL gerada."
            )
            
            if st.button("🗑️ Limpar Conversa"):
                st.session_state.messages = []
                st.rerun()
        
        st.subheader("Chat Analítico com IA")
        st.caption(f"Usando: **{st.session_state.llm_provider.upper()}** | Faça perguntas sobre os dados ou peça para buscar na web.")
        
        for message in st.session_state.messages:
            with st.chat_message(message["role"]):
                st.write(message["content"])
                
                if st.session_state.debug_mode and "source" in message:
                    st.caption(f"Fonte: {message['source']}")
                    if "debug_info" in message and message["debug_info"]:
                        st.code(f"Argumentos da Ferramenta:\n{json.dumps(message['debug_info'], indent=2, ensure_ascii=False)}", language="json")
        
        if prompt := st.chat_input("Pergunte sobre os dados ou busque na web..."):
            self._handle_user_input(prompt)
    
    def _handle_user_input(self, prompt: str):
        """Handle user input and generate response"""
        st.session_state.messages.append({"role": "user", "content": prompt})
        
        with st.chat_message("user"):
            st.write(prompt)
        
        with st.chat_message("assistant"):
            with st.spinner("A IA está pensando..."):
                response = self.llm_integration.query(
                    prompt, 
                    provider=st.session_state.llm_provider
                )
                st.write(response["answer"])
                
                if st.session_state.debug_mode and "debug_info" in response and response["debug_info"]:
                    st.caption(f"Fonte: {response.get('source', 'unknown')}")
                    st.code(f"Argumentos da Ferramenta:\n{json.dumps(response['debug_info'], indent=2, ensure_ascii=False)}", language="json")
            
            st.session_state.messages.append({
                "role": "assistant", 
                "content": response["answer"],
                "source": response.get("source", "unknown"),
                "debug_info": response.get("debug_info")
            })
    
    def display_sample_questions(self):
        """Display sample questions for users to try"""
        st.sidebar.subheader("Exemplos de perguntas")
        
        questions = [
            "Quais são os 5 estados com mais infrações?",
            "Qual o valor total de multas aplicadas no estado do Pará?",
            "Qual o endereço da sede do IBAMA em Brasília?",
            "Mostre 3 infrações relacionadas a 'fauna' com o CNPJ do infrator.",
            "Quais os tipos de infrações que existem?"
        ]
        
        for question in questions:
            if st.sidebar.button(question):
                self._handle_user_input(question)
                st.rerun()