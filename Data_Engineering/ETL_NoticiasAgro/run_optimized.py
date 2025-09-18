#!/usr/bin/env python3
"""
Script de exemplo para testar as otimizações de performance
"""

import sys
import os
from datetime import datetime, timedelta

# Adicionar src ao path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from main import ETLPipeline

def test_optimizations():
    """Testa as otimizações implementadas"""
    
    print("🚀 Testando otimizações do pipeline ETL")
    print("=" * 50)
    
    # Criar pipeline otimizado
    pipeline = ETLPipeline(
        output_dir="output",
        max_workers=6,  # Mais workers
        stop_on_error=False  # Continuar em caso de erro
    )
    
    # Testar período recente (últimos 10 dias úteis)
    end_date = datetime.now()
    start_date = end_date - timedelta(days=15)  # 15 dias para garantir 10 dias úteis
    
    start_str = start_date.strftime('%Y-%m-%d')
    end_str = end_date.strftime('%Y-%m-%d')
    
    print(f"📅 Período de teste: {start_str} até {end_str}")
    print(f"⚡ Modo paralelo com 6 workers")
    print(f"🎯 Pula fins de semana e feriados automaticamente")
    print(f"🔄 Continua processamento mesmo com erros")
    print()
    
    # Executar processamento otimizado
    start_time = datetime.now()
    
    try:
        pipeline.collect_date_range_parallel(start_str, end_str, skip_existing=True)
        
        end_time = datetime.now()
        duration = end_time - start_time
        
        print()
        print("=" * 50)
        print(f"⏱️  Tempo total: {duration}")
        print(f"📊 Registros processados: {pipeline.total_records_processed}")
        print("✅ Teste de otimização concluído!")
        
    except Exception as e:
        print(f"❌ Erro durante teste: {e}")
        return False
    
    return True

if __name__ == "__main__":
    success = test_optimizations()
    
    if success:
        print("\n🎉 Otimizações funcionando corretamente!")
        print("\nPara usar o pipeline otimizado:")
        print("  python main.py range-fast --start-date 2025-01-01 --end-date 2025-01-10")
        print("  python main.py range-parallel --start-date 2025-01-01 --end-date 2025-01-10 --workers 8")
    else:
        print("\n❌ Problemas encontrados durante o teste")