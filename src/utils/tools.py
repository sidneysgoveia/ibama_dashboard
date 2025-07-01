import os
import json
import requests

# Importa o módulo de configuração que está na raiz do projeto
import config

def search_internet(query: str) -> str:
    """
    Realiza uma busca na internet usando a API da Serper (via requests) para encontrar informações.
    
    Args:
        query: A string de busca.

    Returns:
        Uma string formatada em JSON com os resultados da busca.
    """
    print(f"🔎 Realizando busca na internet (Serper/requests) por: '{query}'")
    
    # Lê a chave da API a partir do módulo de configuração
    serper_api_key = config.get_secret('SUPABASE_KEY', default=None)
    if not serper_api_key:
        print("❌ Erro: Chave da API da Serper não encontrada.")
        return json.dumps({"error": "A chave da API da Serper não está configurada."})

    # Endpoint da API da Serper
    url = "https://google.serper.dev/search"

    # Payload da requisição, incluindo a query e parâmetros de localização
    payload = json.dumps({
        "q": query,
        "gl": "br",
        "hl": "pt-br"
    })
    
    headers = {
        'X-API-KEY': serper_api_key,
        'Content-Type': 'application/json'
    }

    try:
        # Faz a requisição POST para a API
        response = requests.post(url, headers=headers, data=payload)
        response.raise_for_status()  # Lança um erro para status HTTP 4xx/5xx

        results = response.json()
        
        # Log da resposta completa para facilitar a depuração
        print(f"📄 Resposta da API Serper: {json.dumps(results, indent=2, ensure_ascii=False)}")

        # Processa os resultados para extrair as informações mais úteis
        if "organic" in results and results["organic"]:
            # Extrai os snippets dos 3 primeiros resultados orgânicos
            snippets = [
                {
                    "title": r.get("title"),
                    "snippet": r.get("snippet", "N/A"),
                    "link": r.get("link")
                }
                for r in results["organic"][:3]
            ]
            return json.dumps(snippets, ensure_ascii=False)
        elif "answerBox" in results:
             # Se houver uma "caixa de resposta" direta, usa essa informação
            answer_box = results["answerBox"]
            return json.dumps([{"title": answer_box.get("title"), "snippet": answer_box.get("snippet") or answer_box.get("answer")}], ensure_ascii=False)
        else:
            print("⚠️ Nenhum resultado 'organic' ou 'answerBox' encontrado na resposta.")
            return json.dumps({"error": "Nenhum resultado relevante encontrado."})
            
    except requests.exceptions.HTTPError as http_err:
        print(f"❌ Erro HTTP na busca (Serper): {http_err} - Resposta: {response.text}")
        return json.dumps({"error": f"Erro de comunicação com a API de busca: {http_err}"})
    except Exception as e:
        print(f"❌ Erro inesperado na busca (Serper): {e}")
        return json.dumps({"error": f"Falha ao buscar na internet: {str(e)}"})
