# IBAMA - Análise de Autos de Infração

Aplicativo Streamlit para análise de dados de autos de infração do Instituto Brasileiro do Meio Ambiente e dos Recursos Naturais Renováveis (IBAMA).

![IBAMA Logo](https://www.gov.br/ibama/pt-br/centrais-de-conteudo/imagens/logos/ibama-logo.png)

## Sobre

Este aplicativo baixa, processa e analisa dados de autos de infração do IBAMA, disponibilizados pelo [Portal de Dados Abertos](https://dadosabertos.ibama.gov.br/dataset/fiscalizacao-auto-de-infracao). O sistema oferece:

- Download automático dos dados a partir de arquivos CSV compactados
- Armazenamento em banco de dados local (DuckDB) e online (Supabase)
- Dashboard com visualizações interativas dos dados
- Chatbot inteligente para consultas em linguagem natural usando Llama 3.1
- Análise especializada de infrações relacionadas à biopirataria
- Geração robusta de SQL com LLMs e análise inteligente de resultados
- Agentes inteligentes para análises avançadas de dados
- Programação de atualizações diárias (10h, horário de Brasília)

## Requisitos

- Python 3.8+
- Conta na [Groq](https://groq.com/) para acesso aos modelos Llama 3.1
- Conta no [HuggingFace](https://huggingface.co/) para acesso aos modelos de embeddings
- Conta no [Supabase](https://supabase.com/) (opcional)

## Instalação

1. Clone o repositório:
```bash
git clone https://github.com/seu-usuario/ibama-analise.git
cd ibama-analise
```

2. Instale as dependências:
```bash
pip install -r requirements.txt
```

3. Configure as variáveis de ambiente:
```bash
# Copie o arquivo de exemplo
cp env.example .env

# Edite o arquivo .env com suas chaves de API
nano .env
```

## Configuração

Edite o arquivo `.env` com suas credenciais:

```ini
# Groq API Key para Llama 3.1
GROQ_API_KEY=your_groq_api_key_here

# HuggingFace API Key para modelo de embeddings
HUGGINGFACE_API_KEY=your_huggingface_api_key_here

# Credenciais do Supabase (opcional)
SUPABASE_URL=your_supabase_url_here
SUPABASE_KEY=your_supabase_anon_key_here

# URLs dos dados
IBAMA_DATASET_URL=https://dadosabertos.ibama.gov.br/dataset/fiscalizacao-auto-de-infracao/resource/b2aba344-95df-43c0-b2ba-f4353cfd9a00
IBAMA_ZIP_URL=https://dadosabertos.ibama.gov.br/dados/SIFISC/auto_infracao/auto_infracao/auto_infracao_csv.zip

# Configuração do banco de dados
DB_PATH=data/ibama_infracao.db
```

## Uso

Execute o aplicativo Streamlit:

```bash
streamlit run app.py
```

Acesse o aplicativo em seu navegador em `http://localhost:8501`.

## Funcionalidades

### Dashboard
Visualize métricas e gráficos sobre os autos de infração, incluindo:
- Total de autos e valores de multas
- Distribuição geográfica das infrações
- Evolução temporal das autuações
- Tipos mais comuns de infrações

### Análise de Biopirataria
Seção especializada para análise de infrações relacionadas à biopirataria e tráfico de espécies:
- Visualizações específicas por estado e tipo de infração
- Tendências temporais de crimes contra a fauna e flora
- Consultas especializadas para identificar padrões
- Exportação de dados para análise posterior

### Chatbot
Faça perguntas em linguagem natural sobre os dados, como:
- "Quais são os estados com mais autos de infração?"
- "Qual o valor médio das multas aplicadas?"
- "Como variou o número de autos de infração ao longo dos anos?"
- "Mostre dados sobre biopirataria no Amazonas"
- "Quais são as infrações de pesca no Ceará?"

### Explorador de Dados
Execute consultas SQL personalizadas e crie visualizações específicas.

### Ferramentas de Diagnóstico
Seção para desenvolvedores com ferramentas para:
- Verificar a estrutura do banco de dados
- Testar a geração de SQL com LLMs
- Testar o sistema de detecção de biopirataria
- Executar consultas pré-definidas por categoria

## Hospedagem no Streamlit Community Cloud

1. Faça o fork deste repositório para sua conta GitHub
2. Acesse [Streamlit Community Cloud](https://streamlit.io/cloud)
3. Conecte com sua conta GitHub
4. Selecione o repositório e configure:
   - Nome do aplicativo: `ibama-analise`
   - Arquivo principal: `app.py`
   - Adicione suas variáveis secretas em Advanced Settings

## Arquitetura

O aplicativo utiliza uma arquitetura moderna baseada em:

1. **Interface do Usuário**: Streamlit para criar a interface web interativa
2. **Armazenamento de Dados**: DuckDB (local) e Supabase (opcional, remoto)
3. **Processamento de Linguagem Natural**:
   - Llama 3.1 via Groq para geração de SQL e explicações
   - LiteLLM para compatibilidade com diferentes provedores de LLM
   - LangChain para busca de informações por similaridade
4. **Análise de Dados**:
   - Pandas e SQLAlchemy para manipulação de dados
   - Plotly e Matplotlib para visualizações
5. **Detecção Especializada**: 
   - Sistema personalizado para detecção de consultas sobre biopirataria
   - Tratamento diferenciado para terminologias específicas de crimes ambientais

## Tecnologias

- **Streamlit**: Interface do aplicativo
- **DuckDB**: Armazenamento local de dados
- **Supabase**: Banco de dados online (opcional)
- **Groq**: API para acesso aos modelos Llama 3.1
- **LangChain**: Framework para RAG (Retrieval Augmented Generation)
- **LiteLLM**: Camada de abstração para APIs de LLM
- **SmolagentS**: Framework para agentes de IA
- **Plotly**: Visualização interativa de dados
- **APScheduler**: Programação de tarefas automáticas
- **FAISS**: Busca eficiente por similaridade em embeddings

## Dados

Os dados utilizados são disponibilizados pelo IBAMA em formato CSV compactado, atualizados periodicamente:
- [Página do dataset](https://dadosabertos.ibama.gov.br/dataset/fiscalizacao-auto-de-infracao)
- [Link direto para o ZIP](https://dadosabertos.ibama.gov.br/dados/SIFISC/auto_infracao/auto_infracao/auto_infracao_csv.zip)

## Licença

Este projeto é distribuído sob a licença MIT. Veja o arquivo `LICENSE` para mais detalhes.

## Autor

Este projeto foi desenvolvido como demonstração de análise de dados públicos com tecnologias modernas de IA. 