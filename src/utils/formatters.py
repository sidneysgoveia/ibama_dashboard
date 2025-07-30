# src/utils/formatters.py
import locale
import pandas as pd
import numpy as np

# Configuração para formatação brasileira
try:
    locale.setlocale(locale.LC_ALL, 'pt_BR.UTF-8')
except:
    # Fallback para sistemas que não têm locale brasileiro
    try:
        locale.setlocale(locale.LC_ALL, 'pt_BR')
    except:
        # Ultimo fallback para sistemas sem suporte a locale brasileiro
        pass

def format_currency_brazilian(value):
    """
    Formata um valor numérico como moeda brasileira.
    
    Args:
        value: Valor numérico (int, float) ou string
        
    Returns:
        str: Valor formatado como moeda brasileira (R$ X.XXX.XXX,XX)
    """
    if pd.isna(value) or value is None:
        return "R$ 0,00"
    
    try:
        # Converte para float se necessário
        if isinstance(value, str):
            # Remove caracteres não numéricos exceto pontos e vírgulas
            clean_value = value.replace('R$', '').replace(' ', '').strip()
            # Substitui vírgula por ponto para conversão
            clean_value = clean_value.replace(',', '.')
            numeric_value = float(clean_value)
        else:
            numeric_value = float(value)
        
        # Verifica se é NaN
        if np.isnan(numeric_value):
            return "R$ 0,00"
        
        # Formatação inteligente baseada no valor
        if numeric_value >= 1_000_000_000:
            # Bilhões
            return f"R$ {numeric_value/1_000_000_000:,.1f} bi".replace(',', 'X').replace('.', ',').replace('X', '.')
        elif numeric_value >= 1_000_000:
            # Milhões
            return f"R$ {numeric_value/1_000_000:,.1f} mi".replace(',', 'X').replace('.', ',').replace('X', '.')
        elif numeric_value >= 1_000:
            # Milhares
            return f"R$ {numeric_value:,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.')
        else:
            # Valores menores
            return f"R$ {numeric_value:.2f}".replace('.', ',')
            
    except (ValueError, TypeError):
        return "R$ 0,00"

def format_number_brazilian(value):
    """
    Formata um número usando o padrão brasileiro (pontos para milhares, vírgulas para decimais).
    
    Args:
        value: Valor numérico (int, float) ou string
        
    Returns:
        str: Número formatado no padrão brasileiro
    """
    if pd.isna(value) or value is None:
        return "0"
    
    try:
        # Converte para número se necessário
        if isinstance(value, str):
            clean_value = value.replace(',', '.').strip()
            numeric_value = float(clean_value)
        else:
            numeric_value = float(value)
        
        # Verifica se é NaN
        if np.isnan(numeric_value):
            return "0"
        
        # Se é um número inteiro, não mostra decimais
        if numeric_value == int(numeric_value):
            numeric_value = int(numeric_value)
            return f"{numeric_value:,}".replace(',', '.')
        else:
            # Para números com decimais
            return f"{numeric_value:,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.')
            
    except (ValueError, TypeError):
        return "0"

def format_percentage_brazilian(value, decimals=1):
    """
    Formata um valor como percentual brasileiro.
    
    Args:
        value: Valor numérico entre 0 e 1 (ou 0 e 100)
        decimals: Número de casas decimais (padrão: 1)
        
    Returns:
        str: Percentual formatado (XX,X%)
    """
    if pd.isna(value) or value is None:
        return "0,0%"
    
    try:
        numeric_value = float(value)
        
        if np.isnan(numeric_value):
            return "0,0%"
        
        # Se o valor é menor que 1, assume que já está em formato decimal (0.15 = 15%)
        if numeric_value <= 1:
            percentage = numeric_value * 100
        else:
            # Se maior que 1, assume que já está em formato percentual (15 = 15%)
            percentage = numeric_value
        
        return f"{percentage:.{decimals}f}%".replace('.', ',')
        
    except (ValueError, TypeError):
        return "0,0%"

def format_large_number(value):
    """
    Formata números grandes com sufixos (K, M, B).
    
    Args:
        value: Valor numérico
        
    Returns:
        str: Número formatado com sufixo apropriado
    """
    if pd.isna(value) or value is None:
        return "0"
    
    try:
        numeric_value = float(value)
        
        if np.isnan(numeric_value):
            return "0"
        
        if abs(numeric_value) >= 1_000_000_000:
            return f"{numeric_value/1_000_000_000:.1f}B".replace('.', ',')
        elif abs(numeric_value) >= 1_000_000:
            return f"{numeric_value/1_000_000:.1f}M".replace('.', ',')
        elif abs(numeric_value) >= 1_000:
            return f"{numeric_value/1_000:.1f}K".replace('.', ',')
        else:
            return format_number_brazilian(numeric_value)
            
    except (ValueError, TypeError):
        return "0"

def format_date_brazilian(date_value):
    """
    Formata uma data no padrão brasileiro (DD/MM/AAAA).
    
    Args:
        date_value: Data (datetime, string, etc.)
        
    Returns:
        str: Data formatada no padrão brasileiro
    """
    if pd.isna(date_value) or date_value is None:
        return "-"
    
    try:
        # Converte para datetime se necessário
        if isinstance(date_value, str):
            date_obj = pd.to_datetime(date_value, errors='coerce')
        else:
            date_obj = pd.to_datetime(date_value, errors='coerce')
        
        if pd.isna(date_obj):
            return "-"
        
        return date_obj.strftime('%d/%m/%Y')
        
    except:
        return "-"

def format_datetime_brazilian(datetime_value):
    """
    Formata uma data e hora no padrão brasileiro (DD/MM/AAAA HH:MM).
    
    Args:
        datetime_value: Data e hora (datetime, string, etc.)
        
    Returns:
        str: Data e hora formatada no padrão brasileiro
    """
    if pd.isna(datetime_value) or datetime_value is None:
        return "-"
    
    try:
        # Converte para datetime se necessário
        if isinstance(datetime_value, str):
            datetime_obj = pd.to_datetime(datetime_value, errors='coerce')
        else:
            datetime_obj = pd.to_datetime(datetime_value, errors='coerce')
        
        if pd.isna(datetime_obj):
            return "-"
        
        return datetime_obj.strftime('%d/%m/%Y %H:%M')
        
    except:
        return "-"

def truncate_text(text, max_length=50, suffix="..."):
    """
    Trunca texto longo para exibição.
    
    Args:
        text: Texto a ser truncado
        max_length: Comprimento máximo (padrão: 50)
        suffix: Sufixo para indicar truncamento (padrão: "...")
        
    Returns:
        str: Texto truncado se necessário
    """
    if pd.isna(text) or text is None:
        return "-"
    
    text_str = str(text).strip()
    
    if len(text_str) <= max_length:
        return text_str
    else:
        return text_str[:max_length - len(suffix)] + suffix

def clean_numeric_string(value):
    """
    Limpa uma string numérica para conversão.
    
    Args:
        value: String com valor numérico
        
    Returns:
        float: Valor numérico limpo
    """
    if pd.isna(value) or value is None:
        return 0.0
    
    try:
        # Remove caracteres não numéricos exceto pontos, vírgulas e sinais
        clean_str = str(value).strip()
        
        # Remove prefixos comuns (R$, $, etc.)
        prefixes = ['R, ', '€', '£']
        for prefix in prefixes:
            clean_str = clean_str.replace(prefix, '')
        
        # Remove espaços
        clean_str = clean_str.replace(' ', '')
        
        # Se tem vírgula e ponto, assume formato brasileiro (1.234.567,89)
        if ',' in clean_str and '.' in clean_str:
            # Remove pontos (separadores de milhares) e substitui vírgula por ponto
            clean_str = clean_str.replace('.', '').replace(',', '.')
        elif ',' in clean_str:
            # Só vírgula - pode ser decimal brasileiro ou separador de milhares
            parts = clean_str.split(',')
            if len(parts) == 2 and len(parts[1]) <= 2:
                # Provavelmente decimal (123,45)
                clean_str = clean_str.replace(',', '.')
            else:
                # Provavelmente separador de milhares (1,234,567)
                clean_str = clean_str.replace(',', '')
        
        return float(clean_str)
        
    except (ValueError, TypeError):
        return 0.0

def format_compact_currency(value):
    """
    Formata moeda de forma compacta para gráficos.
    
    Args:
        value: Valor numérico
        
    Returns:
        str: Valor formatado de forma compacta (R$ 1,2M)
    """
    if pd.isna(value) or value is None:
        return "R$ 0"
    
    try:
        numeric_value = float(value)
        
        if np.isnan(numeric_value):
            return "R$ 0"
        
        if abs(numeric_value) >= 1_000_000_000:
            return f"R$ {numeric_value/1_000_000_000:.1f}B".replace('.', ',')
        elif abs(numeric_value) >= 1_000_000:
            return f"R$ {numeric_value/1_000_000:.1f}M".replace('.', ',')
        elif abs(numeric_value) >= 1_000:
            return f"R$ {numeric_value/1_000:.0f}K"
        else:
            return f"R$ {numeric_value:.0f}"
            
    except (ValueError, TypeError):
        return "R$ 0"

def format_month_name(month_number):
    """
    Converte número do mês para nome em português.
    
    Args:
        month_number: Número do mês (1-12)
        
    Returns:
        str: Nome do mês em português
    """
    months = {
        1: "Janeiro", 2: "Fevereiro", 3: "Março", 4: "Abril",
        5: "Maio", 6: "Junho", 7: "Julho", 8: "Agosto",
        9: "Setembro", 10: "Outubro", 11: "Novembro", 12: "Dezembro"
    }
    
    try:
        month_int = int(month_number)
        return months.get(month_int, f"Mês {month_int}")
    except (ValueError, TypeError):
        return "Mês inválido"

def format_month_name_short(month_number):
    """
    Converte número do mês para nome abreviado em português.
    
    Args:
        month_number: Número do mês (1-12)
        
    Returns:
        str: Nome abreviado do mês em português
    """
    months_short = {
        1: "Jan", 2: "Fev", 3: "Mar", 4: "Abr",
        5: "Mai", 6: "Jun", 7: "Jul", 8: "Ago",
        9: "Set", 10: "Out", 11: "Nov", 12: "Dez"
    }
    
    try:
        month_int = int(month_number)
        return months_short.get(month_int, f"M{month_int}")
    except (ValueError, TypeError):
        return "M??"

def format_data_size(size_bytes):
    """
    Formata tamanho de dados em bytes para formato legível.
    
    Args:
        size_bytes: Tamanho em bytes
        
    Returns:
        str: Tamanho formatado (KB, MB, GB, etc.)
    """
    if pd.isna(size_bytes) or size_bytes is None:
        return "0 B"
    
    try:
        size = float(size_bytes)
        
        if size < 1024:
            return f"{size:.0f} B"
        elif size < 1024**2:
            return f"{size/1024:.1f} KB".replace('.', ',')
        elif size < 1024**3:
            return f"{size/(1024**2):.1f} MB".replace('.', ',')
        elif size < 1024**4:
            return f"{size/(1024**3):.1f} GB".replace('.', ',')
        else:
            return f"{size/(1024**4):.1f} TB".replace('.', ',')
            
    except (ValueError, TypeError):
        return "0 B"

# Funções de validação e limpeza para dados do IBAMA

def clean_uf_name(uf_value):
    """
    Limpa e valida nome de UF.
    
    Args:
        uf_value: Valor da UF
        
    Returns:
        str: UF limpa e validada
    """
    if pd.isna(uf_value) or uf_value is None:
        return "N/A"
    
    uf_clean = str(uf_value).strip().upper()
    
    # Lista de UFs válidas
    valid_ufs = [
        'AC', 'AL', 'AP', 'AM', 'BA', 'CE', 'DF', 'ES', 'GO',
        'MA', 'MT', 'MS', 'MG', 'PA', 'PB', 'PR', 'PE', 'PI',
        'RJ', 'RN', 'RS', 'RO', 'RR', 'SC', 'SP', 'SE', 'TO'
    ]
    
    if uf_clean in valid_ufs:
        return uf_clean
    else:
        return "N/A"

def clean_municipality_name(municipality_value):
    """
    Limpa nome de município.
    
    Args:
        municipality_value: Nome do município
        
    Returns:
        str: Nome do município limpo
    """
    if pd.isna(municipality_value) or municipality_value is None:
        return "N/A"
    
    # Remove espaços extras e capitaliza adequadamente
    municipality_clean = str(municipality_value).strip()
    
    if municipality_clean:
        # Capitaliza cada palavra, exceto preposições comuns
        prepositions = ['de', 'da', 'do', 'das', 'dos', 'e']
        words = municipality_clean.lower().split()
        
        capitalized_words = []
        for i, word in enumerate(words):
            if i == 0 or word not in prepositions:
                capitalized_words.append(word.capitalize())
            else:
                capitalized_words.append(word)
        
        return ' '.join(capitalized_words)
    else:
        return "N/A"

def clean_infractor_name(name_value):
    """
    Limpa nome do infrator.
    
    Args:
        name_value: Nome do infrator
        
    Returns:
        str: Nome limpo
    """
    if pd.isna(name_value) or name_value is None:
        return "N/A"
    
    name_clean = str(name_value).strip()
    
    if name_clean:
        return name_clean.title()
    else:
        return "N/A"
