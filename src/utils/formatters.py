# src/utils/formatters.py

# Adiciona a importação do módulo 'numbers'
import numbers

def format_number_brazilian(value) -> str:
    """
    Formata um número inteiro no padrão brasileiro (ex: 17.804).
    """
    # --- ALTERAÇÃO AQUI: Verificação de tipo mais robusta ---
    if not isinstance(value, numbers.Number):
        return "N/A"
    
    # Garante que estamos lidando com um inteiro para a formatação
    int_value = int(value)
    return f"{int_value:,}".replace(",", ".")

def format_currency_brazilian(value) -> str:
    """
    Formata um número grande em uma string concisa no padrão brasileiro (bi, mi, mil).
    """
    # --- ALTERAÇÃO AQUI: Verificação de tipo mais robusta ---
    if not isinstance(value, numbers.Number):
        return "N/A"

    if abs(value) >= 1_000_000_000:
        num_str = f"{value / 1_000_000_000:.1f}".replace('.', ',')
        return f"R$ {num_str} bi"
    elif abs(value) >= 1_000_000:
        num_str = f"{value / 1_000_000:.1f}".replace('.', ',')
        return f"R$ {num_str} mi"
    elif abs(value) >= 1_000:
        num_str = f"{value / 1_000:.1f}".replace('.', ',')
        return f"R$ {num_str} mil"
    else:
        num_str = f"{value:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        return f"R$ {num_str}"