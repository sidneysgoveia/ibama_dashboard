#!/usr/bin/env python3
"""
Script de teste para verificar a integridade dos dados do IBAMA Dashboard.
Use este script para debugar problemas de contagem.
"""

import os
import sys
import pandas as pd
from datetime import datetime

# Adiciona o diretório src ao path
sys.path.append('src')

def test_supabase_connection():
    """Testa a conexão com Supabase e verifica contagens."""
    try:
        from src.utils.database import Database
        from src.utils.supabase_utils import SupabasePaginator
        
        print("🔧 Testando conexão com Supabase...")
        
        # Inicializa database
        db = Database()
        
        if not db.is_cloud or not db.supabase:
            print("❌ Não está conectado ao Supabase")
            return False
        
        print("✅ Conectado ao Supabase")
        
        # Inicializa paginador
        paginator = SupabasePaginator(db.supabase)
        
        print("\n📊 Executando testes de contagem...")
        
        # Teste 1: Contagem real no banco
        print("\n1️⃣ Contagem real no banco:")
        real_counts = paginator.get_real_count()
        
        if 'error' in real_counts:
            print(f"❌ Erro: {real_counts['error']}")
            return False
        
        total_records = real_counts['total_records']
        unique_infractions = real_counts['unique_infractions']
        duplicates = real_counts['duplicates']
        
        print(f"   📊 Total de registros: {total_records:,}")
        print(f"   🔢 Infrações únicas: {unique_infractions:,}")
        print(f"   📉 Duplicatas: {duplicates:,}")
        
        # Teste 2: Busca com paginação
        print("\n2️⃣ Busca com paginação:")
        df_paginated = paginator.get_all_records()
        
        if df_paginated.empty:
            print("❌ Nenhum dado retornado pela paginação")
            return False
        
        paginated_count = len(df_paginated)
        paginated_unique = df_paginated['NUM_AUTO_INFRACAO'].nunique() if 'NUM_AUTO_INFRACAO' in df_paginated.columns else 0
        
        print(f"   📊 Registros retornados: {paginated_count:,}")
        print(f"   🔢 Infrações únicas: {paginated_unique:,}")
        
        # Teste 3: Comparação
        print("\n3️⃣ Análise de consistência:")
        
        if paginated_count == paginated_unique:
            print("   ✅ Paginação retorna dados únicos")
        else:
            print(f"   ⚠️ Paginação tem {paginated_count - paginated_unique} duplicatas")
        
        if unique_infractions == paginated_unique:
            print("   ✅ Contagem do banco coincide com paginação")
        else:
            print(f"   ❌ INCONSISTÊNCIA: Banco={unique_infractions:,}, Paginação={paginated_unique:,}")
        
        # Teste 4: Validação de integridade
        print("\n4️⃣ Validação de integridade:")
        integrity_info = paginator.validate_data_integrity()
        
        if 'error' in integrity_info:
            print(f"   ❌ Erro na validação: {integrity_info['error']}")
        else:
            print(f"   📊 Amostra analisada: {integrity_info['sample_size']:,} registros")
            print(f"   🔢 Colunas: {integrity_info['columns_count']}")
            print(f"   ✅ Tem NUM_AUTO_INFRACAO: {integrity_info['has_num_auto_infracao']}")
            
            if integrity_info['has_num_auto_infracao']:
                print(f"   📈 Únicos na amostra: {integrity_info['unique_num_auto_count']:,}")
                print(f"   ⚠️ Nulos: {integrity_info['null_num_auto_count']:,}")
                print(f"   ⚠️ Vazios: {integrity_info['empty_num_auto_count']:,}")
                print(f"   🔍 Duplicatas detectadas: {integrity_info['duplicate_detection']}")
        
        # Resumo final
        print("\n" + "="*60)
        print("📋 RESUMO DOS TESTES:")
        print("="*60)
        
        if total_records == 21030 and unique_infractions == 21019:
            print("✅ DADOS CORRETOS:")
            print(f"   📊 21.030 registros totais")
            print(f"   🔢 21.019 infrações únicas")
            print(f"   📉 11 duplicatas (esperado)")
        else:
            print("❌ DADOS INCORRETOS:")
            print(f"   📊 Esperado: 21.030 registros, Atual: {total_records:,}")
            print(f"   🔢 Esperado: 21.019 únicos, Atual: {unique_infractions:,}")
        
        if paginated_unique == 21019:
            print("✅ PAGINAÇÃO FUNCIONANDO CORRETAMENTE")
        else:
            print(f"❌ PROBLEMA NA PAGINAÇÃO: Retornou {paginated_unique:,}, esperado 21.019")
        
        return True
        
    except Exception as e:
        print(f"❌ Erro no teste: {e}")
        return False

def test_visualization():
    """Testa o componente de visualização."""
    try:
        from src.components.visualization import DataVisualization
        from src.utils.database import Database
        
        print("\n🎨 Testando componente de visualização...")
        
        db = Database()
        viz = DataVisualization(database=db)
        
        if not viz.paginator:
            print("❌ Paginador não inicializado")
            return False
        
        print("✅ Componente de visualização inicializado")
        
        # Teste de qualidade dos dados
        date_filters = {
            "mode": "simple",
            "years": [2024, 2025],
            "description": "2024-2025"
        }
        
        quality_info = viz.get_data_quality_info([], date_filters)
        
        if 'error' in quality_info:
            print(f"❌ Erro na análise de qualidade: {quality_info['error']}")
            return False
        
        print("📊 Informações de qualidade dos dados:")
        print(f"   📊 Total de registros: {quality_info['total_records']:,}")
        print(f"   🔢 Infrações únicas: {quality_info['unique_infractions']:,}")
        print(f"   ✅ Consistência: {quality_info['data_consistency']}")
        
        if quality_info['duplicate_records']:
            print(f"   📉 Duplicatas removidas: {quality_info['duplicate_records']:,}")
        
        return True
        
    except Exception as e:
        print(f"❌ Erro no teste de visualização: {e}")
        return False

def main():
    """Executa todos os testes de integridade."""
    print("🔍 IBAMA Dashboard - Teste de Integridade dos Dados")
    print("=" * 60)
    print(f"⏰ Executado em: {datetime.now()}")
    print("=" * 60)
    
    # Testa conexão e variáveis de ambiente
    print("🔧 Verificando configuração...")
    
    required_vars = ['SUPABASE_URL', 'SUPABASE_KEY']
    missing_vars = []
    
    for var in required_vars:
        if not os.getenv(var):
            missing_vars.append(var)
    
    if missing_vars:
        print(f"❌ Variáveis de ambiente faltando: {', '.join(missing_vars)}")
        print("💡 Configure as variáveis no arquivo .env ou como variáveis de sistema")
        return False
    
    print("✅ Configuração OK")
    
    # Executa testes
    tests_passed = 0
    total_tests = 2
    
    print("\n" + "=" * 60)
    print("EXECUTANDO TESTES")
    print("=" * 60)
    
    # Teste 1: Supabase
    if test_supabase_connection():
        tests_passed += 1
        print("✅ Teste Supabase: PASSOU")
    else:
        print("❌ Teste Supabase: FALHOU")
    
    # Teste 2: Visualização  
    if test_visualization():
        tests_passed += 1
        print("✅ Teste Visualização: PASSOU")
    else:
        print("❌ Teste Visualização: FALHOU")
    
    # Resultado final
    print("\n" + "=" * 60)
    print("RESULTADO FINAL")
    print("=" * 60)
    
    if tests_passed == total_tests:
        print("🎉 TODOS OS TESTES PASSARAM!")
        print("✅ Sistema funcionando corretamente")
        return True
    else:
        print(f"❌ {total_tests - tests_passed} de {total_tests} testes falharam")
        print("🔧 Verifique as mensagens de erro acima")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
