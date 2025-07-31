# 🌳 IBAMA Dashboard - Análise de Infrações Ambientais

> **Status:** 🚧 **Versão Beta** - Em desenvolvimento ativo

Dashboard interativo para análise de dados de autos de infração do Instituto Brasileiro do Meio Ambiente e dos Recursos Naturais Renováveis (IBAMA), com recursos de Inteligência Artificial para consultas em linguagem natural.

[![Streamlit App](https://static.streamlit.io/badges/streamlit_badge_black_white.svg)](https://ibamadashboard.streamlit.app/)
[![GitHub](https://img.shields.io/badge/GitHub-Repository-blue)](https://github.com/reichaves/ibama_dashboard)
[![Python](https://img.shields.io/badge/Python-3.8%2B-blue)](https://python.org)
[![License](https://img.shields.io/badge/License-MIT-green)](LICENSE)

## 📊 Sobre o Projeto

Este aplicativo processa e analisa dados públicos de autos de infração do IBAMA, oferecendo uma interface moderna e intuitiva para exploração de dados ambientais brasileiros. Desenvolvido especificamente para **jornalistas**, **pesquisadores**, **acadêmicos** e **cidadãos interessados** em transparência ambiental.

### 🎯 Objetivos
- **Democratizar o acesso** aos dados ambientais brasileiros
- **Facilitar análises jornalísticas** de infrações ambientais
- **Apoiar pesquisas acadêmicas** com ferramentas modernas
- **Promover transparência** em fiscalizações ambientais
- **Combinar análise tradicional** com Inteligência Artificial

## ✨ Funcionalidades Principais

### 📈 **Dashboard Interativo**
- **Métricas em tempo real**: Total de infrações, valores de multas, municípios afetados
- **Visualizações geográficas**: Mapas de calor das infrações por região
- **Análises temporais**: Filtros avançados por ano e mês
- **Rankings especializados**: 
  - Top 10 pessoas físicas infratoras (CPF mascarado)
  - Top 10 empresas infratoras (CNPJ completo)
  - Estados e municípios com mais infrações
- **Distribuição por gravidade**: Baixa, Média, e infrações sem avaliação

### 🤖 **Chatbot com IA**
- **Perguntas em linguagem natural**: "Quais estados têm mais infrações de pesca?"
- **Dois modelos disponíveis**:
  - 🦙 **Llama 3.1 70B (Groq)**: Rápido, ideal para consultas simples
  - 💎 **Gemini 1.5 Pro (Google)**: Avançado, para análises complexas
- **Análise inteligente**: Combina dados locais com processamento de IA
- **Transparência**: Avisos claros sobre limitações da IA

### 🔍 **Explorador SQL**
- **Modo Manual**: Interface para consultas SQL diretas
- **Modo IA**: Geração automática de SQL a partir de linguagem natural
- **Análise automática**: Interpretação inteligente dos resultados
- **Exemplos prontos**: Consultas pré-definidas para início rápido

## 📰 Usos Jornalísticos

### **Para Jornalismo Investigativo:**
- **Identificação de padrões**: Empresas ou pessoas com histórico de infrações
- **Análises regionais**: Comparação entre estados e regiões
- **Séries temporais**: Evolução das infrações ao longo do tempo
- **Cruzamento de dados**: Correlação entre tipos de infração e localização

### **Para Reportagens:**
- **Dados verificáveis**: Todas as informações têm origem oficial (IBAMA)
- **Visualizações prontas**: Gráficos exportáveis para matérias
- **Consultas específicas**: Busca por casos particulares ou regiões
- **Contexto histórico**: Comparação com períodos anteriores

### **Exemplos de Pautas:**
- "As 10 empresas que mais receberam multas ambientais em 2024"
- "Municípios amazônicos lideram ranking de infrações contra fauna"
- "Crescimento de 30% nas multas por biopirataria no último ano"
- "Perfil das infrações ambientais no seu estado"

## 🔬 Aplicações em Pesquisa

### **Pesquisa Acadêmica:**
- **Análise quantitativa**: Dados estruturados para estudos estatísticos
- **Séries históricas**: Dados desde 2024 para análises temporais
- **Geolocalização**: Coordenadas para estudos espaciais
- **Categorização**: Tipos de infração para estudos temáticos

### **Áreas de Pesquisa Suportadas:**
- **Direito Ambiental**: Efetividade da fiscalização
- **Geografia**: Distribuição espacial de crimes ambientais
- **Economia**: Impacto econômico das multas ambientais
- **Ciências Sociais**: Perfil dos infratores ambientais
- **Políticas Públicas**: Avaliação de programas de fiscalização

### **Metodologia de Dados:**
- **Fonte primária**: Portal de Dados Abertos do IBAMA
- **Atualização**: Dados atualizados diariamente
- **Qualidade**: Validação automática e limpeza de dados
- **Transparência**: Código-fonte aberto para auditoria

## 🚨 Limitações e Avisos Importantes

### **⚠️ Limitações da IA**
- **Alucinações**: Modelos podem gerar informações incorretas
- **Vieses**: Podem refletir preconceitos dos dados de treinamento
- **Contexto limitado**: Não compreendem nuances políticas ou sociais
- **Verificação obrigatória**: **SEMPRE** confirme informações com fontes primárias

### **📊 Limitações dos Dados**
- **Período**: Dados disponíveis principalmente de 2024-2025
- **Completude**: Nem todas as infrações podem estar classificadas
- **Processamento**: Dados passam por limpeza automática que pode introduzir erros
- **Interpretação**: Correlação não implica causalidade

### **🔒 Privacidade e Ética**
- **CPF mascarado**: Pessoas físicas têm dados protegidos (XXX.***.***-XX)
- **CNPJ completo**: Empresas têm transparência total (dados públicos)
- **Responsabilidade**: Usuário responsável pelo uso ético das informações

## 🛠️ Tecnologias Utilizadas

### **Frontend e Interface**
- **[Streamlit](https://streamlit.io/)**: Framework web para aplicações de dados
- **[Plotly](https://plotly.com/)**: Visualizações interativas
- **[Pandas](https://pandas.pydata.org/)**: Manipulação e análise de dados

### **Inteligência Artificial**
- **[Groq](https://groq.com/)**: API para Llama 3.1 70B (processamento rápido)
- **[Google Gemini](https://ai.google.dev/)**: Modelo avançado para análises complexas
- **[OpenAI API](https://openai.com/)**: Interface compatível para LLMs

### **Banco de Dados**
- **[Supabase](https://supabase.com/)**: PostgreSQL na nuvem (produção)
- **[DuckDB](https://duckdb.org/)**: Banco analítico local (desenvolvimento)

### **Processamento de Dados**
- **[NumPy](https://numpy.org/)**: Computação numérica
- **[APScheduler](https://apscheduler.readthedocs.io/)**: Agendamento de tarefas
- **[Requests](https://requests.readthedocs.io/)**: Download de dados

### **Deploy e Infraestrutura**
- **[Streamlit Community Cloud](https://streamlit.io/cloud)**: Hospedagem gratuita
- **[GitHub](https://github.com/)**: Controle de versão e CI/CD
- **[Python 3.8+](https://python.org)**: Linguagem base

## 🚀 Como Usar

### **💻 Acesso Online (Recomendado)**
1. Acesse: **[ibamadashboard.streamlit.app](https://ibamadashboard.streamlit.app/)**
2. Use os filtros na barra lateral para explorar os dados
3. Navegue pelas 3 abas principais:
   - **📊 Dashboard**: Visualizações interativas
   - **💬 Chatbot**: Perguntas em linguagem natural
   - **🔍 SQL**: Consultas personalizadas

### **🏠 Instalação Local**

#### **Pré-requisitos:**
- Python 3.8 ou superior
- Git
- Chaves de API (opcional, para IA):
  - [Groq API Key](https://console.groq.com/) (gratuita)
  - [Google AI API Key](https://ai.google.dev/) (gratuita)

#### **Passos:**
```bash
# 1. Clone o repositório
git clone https://github.com/reichaves/ibama_dashboard.git
cd ibama_dashboard

# 2. Instale dependências
pip install -r requirements.txt

# 3. Configure variáveis de ambiente (opcional)
cp .env.example .env
# Edite .env com suas chaves de API

# 4. Execute o aplicativo
streamlit run app.py
```

#### **Variáveis de Ambiente (Opcionais):**
```bash
# Para funcionalidades de IA
GROQ_API_KEY=sua_chave_groq_aqui
GOOGLE_API_KEY=sua_chave_google_aqui

# Para banco de dados (se não configurado, usa dados locais)
SUPABASE_URL=sua_url_supabase
SUPABASE_KEY=sua_chave_supabase
```

## 📖 Guia Rápido de Uso

### **Para Jornalistas:**
1. **Comece pelo Dashboard** para ter visão geral
2. **Use o Chatbot** para perguntas específicas: "Maiores infratores no Pará"
3. **Exporte visualizações** clicando no ícone da câmera nos gráficos
4. **Sempre verifique** dados importantes com fontes primárias

### **Para Pesquisadores:**
1. **Use o Explorador SQL** para consultas complexas
2. **Aproveite os filtros avançados** por período e região
3. **Documente** suas consultas para reprodutibilidade
4. **Cite adequadamente** a fonte dos dados (Portal IBAMA)

### **Para Desenvolvedores:**
1. **Fork o repositório** para customizações
2. **Consulte a documentação** do código (comentários inline)
3. **Contribua** com melhorias via Pull Requests
4. **Reporte bugs** na seção Issues do GitHub

## 📊 Dados e Fontes

### **Fonte Oficial**
- **Origem**: [Portal de Dados Abertos do IBAMA](https://dadosabertos.ibama.gov.br/dataset/fiscalizacao-auto-de-infracao)
- **Formato**: CSV compactado, atualizado periodicamente
- **Licença**: Dados públicos, domínio público brasileiro

### **Estrutura dos Dados**
- **Período**: Principalmente 2024-2025
- **Granularidade**: Por auto de infração individual
- **Geolocalização**: Coordenadas quando disponíveis
- **Classificação**: Tipo, gravidade, status da infração

### **Processamento**
- **Limpeza automática**: Remoção de duplicatas e dados inválidos
- **Validação**: Verificação de formatos (CPF/CNPJ, datas, valores)
- **Enriquecimento**: Adição de análises geográficas e temporais

## 🤝 Como Contribuir

### **Para Usuários:**
- **Reporte bugs** ou problemas encontrados
- **Sugira melhorias** de funcionalidade
- **Compartilhe** casos de uso interessantes

### **Para Desenvolvedores:**
- **Fork** o repositório
- **Crie branch** para sua feature: `git checkout -b feature/nova-funcionalidade`
- **Commit** suas mudanças: `git commit -m 'Adiciona nova funcionalidade'`
- **Push** para branch: `git push origin feature/nova-funcionalidade`
- **Abra Pull Request** explicando as mudanças

### **Diretrizes de Contribuição:**
- Mantenha o código limpo e documentado
- Adicione testes quando possível
- Siga as convenções de estilo Python (PEP 8)
- Atualize documentação quando necessário

## 📝 Roadmap e Versões Futuras

### **🚧 Versão Atual (Beta)**
- ✅ Dashboard básico funcionando
- ✅ Chatbot com IA integrado
- ✅ Explorador SQL operacional
- ✅ Filtros avançados implementados

### **🎯 Próximas Versões**
- **v1.0** (Estável):
  - 📈 Análises preditivas com Machine Learning
  - 📋 Sistema de relatórios automatizados
  - 🔄 API REST para integração externa
  - 📱 Responsividade mobile aprimorada

- **v1.1** (Expansão):
  - 🌍 Integração com dados de outros órgãos ambientais
  - 📊 Dashboard para gestores públicos
  - 🔔 Sistema de alertas personalizados
  - 📈 Análise de tendências avançada

## ⚖️ Aspectos Legais e Éticos

### **Uso Responsável**
- **Presunção de inocência**: Multas não significam culpa confirmada
- **Contexto necessário**: Dados isolados podem ser enganosos
- **Verificação**: Sempre confirme informações importantes
- **Privacidade**: Respeite dados pessoais mascarados

### **Transparência**
- **Código aberto**: Algoritmos auditáveis publicamente
- **Metodologia clara**: Processamento de dados documentado
- **Limitações explícitas**: Avisos sobre restrições e vieses

## 📞 Suporte e Contato

### **Desenvolvedor**
- **Nome**: Reinaldo Chaves
- **GitHub**: [@reichaves](https://github.com/reichaves)
- **Projeto**: [github.com/reichaves/ibama_dashboard](https://github.com/reichaves/ibama_dashboard)

### **Suporte**
- **Issues**: Use o GitHub Issues para bugs e sugestões
- **Documentação**: README e comentários no código
- **Comunidade**: Streamlit Community para dúvidas técnicas

### **Citação Acadêmica**
```
Chaves, R. (2024). IBAMA Dashboard - Análise de Infrações Ambientais. 
Disponível em: https://github.com/reichaves/ibama_dashboard
```

## 📄 Licença

Este projeto está sob a licença MIT. Veja o arquivo [LICENSE](LICENSE) para detalhes completos.

---

**⚠️ Aviso Legal**: Este projeto é uma iniciativa independente para democratização de dados públicos. Não possui vinculação oficial com o IBAMA ou governo brasileiro. Use as informações com responsabilidade e sempre verifique dados importantes nas fontes oficiais.

**🔍 Transparência**: Todo o código é aberto e auditável. Contribuições e melhorias são bem-vindas da comunidade.

---

*Última atualização: Julho 2025 | Versão: Beta 0.9*
