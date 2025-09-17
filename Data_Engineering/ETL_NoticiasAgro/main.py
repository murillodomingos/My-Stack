#!/usr/bin/env python3
"""
Script principal para coleta e processamento de dados de cotações
"""

import sys
import os
from pathlib import Path
from datetime import datetime, timedelta
import argparse
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import random

# Adicionar src ao path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from src import scrap_ox_data, CotacaoDataProcessor

class ETLPipeline:
    """Pipeline ETL para dados de cotações"""
    
    def __init__(self, output_dir: str = "output", max_workers: int = 3):
        self.processor = CotacaoDataProcessor(output_dir)
        self.max_workers = max_workers
        self.logs = []
    
    def collect_single_date(self, date_str: str) -> bool:
        """Coleta e processa dados de uma data específica"""
        try:
            print(f"Processando {date_str}...")
            
            # Coletar dados
            raw_data = scrap_ox_data(date_str)
            
            if 'error' in raw_data:
                self.logs.append(f"ERRO {date_str}: {raw_data['error']}")
                return False
            
            # Processar e salvar
            processed_dfs = self.processor.process_raw_data(raw_data)
            self.processor.save_to_parquet(processed_dfs, date_str)
            
            total_records = sum(len(df) for df in processed_dfs.values())
            self.logs.append(f"SUCESSO {date_str}: {len(processed_dfs)} tabelas, {total_records} registros")
            
            # Delay para não sobrecarregar o servidor
            time.sleep(random.uniform(1, 3))
            
            return True
            
        except Exception as e:
            self.logs.append(f"ERRO {date_str}: {str(e)}")
            return False
    
    def collect_date_range(self, start_date: str, end_date: str, skip_existing: bool = True) -> None:
        """Coleta dados para um intervalo de datas com processamento paralelo"""
        
        # Gerar lista de datas
        start = datetime.strptime(start_date, '%Y-%m-%d')
        end = datetime.strptime(end_date, '%Y-%m-%d')
        dates = []
        
        current = start
        while current <= end:
            date_str = current.strftime('%Y-%m-%d')
            
            # Verificar se já existe
            if skip_existing and self._file_exists(date_str):
                print(f"Pulando {date_str} (já existe)")
                current += timedelta(days=1)
                continue
            
            dates.append(date_str)
            current += timedelta(days=1)
        
        if not dates:
            print("Nenhuma data nova para processar.")
            return
        
        print(f"Processando {len(dates)} datas...")
        
        # Processar em paralelo
        success_count = 0
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submeter tarefas
            future_to_date = {
                executor.submit(self.collect_single_date, date): date 
                for date in dates
            }
            
            # Processar resultados
            for future in as_completed(future_to_date):
                date = future_to_date[future]
                try:
                    success = future.result()
                    if success:
                        success_count += 1
                except Exception as e:
                    self.logs.append(f"ERRO {date}: {str(e)}")
        
        # Relatório final
        print(f"\nProcessamento concluído:")
        print(f"- Sucessos: {success_count}/{len(dates)}")
        print(f"- Erros: {len(dates) - success_count}")
        
        self._save_logs()
        self._print_stats()
    
    def _file_exists(self, date_str: str) -> bool:
        """Verifica se já existem dados para uma data"""
        year = date_str[:4]
        month = date_str[5:7]
        
        # Verificar pelo menos uma tabela
        for table_type in self.processor.schemas.keys():
            file_path = self.processor.parquet_dir / table_type / year / month / f"cotacoes_{date_str}.parquet"
            if file_path.exists():
                return True
        return False
    
    def _save_logs(self):
        """Salva logs em arquivo"""
        log_file = self.processor.base_output_dir / "etl_logs.txt"
        with open(log_file, 'a', encoding='utf-8') as f:
            f.write(f"\n=== ETL Execution: {datetime.now()} ===\n")
            for log in self.logs:
                f.write(f"{log}\n")
    
    def _print_stats(self):
        """Imprime estatísticas de armazenamento"""
        stats = self.processor.get_storage_stats()
        print(f"\nEstatísticas de Armazenamento:")
        total_size = 0
        total_files = 0
        
        for table_type, info in stats.items():
            print(f"  {table_type}: {info['files']} arquivos, {info['size_mb']} MB")
            total_size += info['size_mb']
            total_files += info['files']
        
        print(f"\nTotal: {total_files} arquivos, {total_size:.2f} MB")

def main():
    parser = argparse.ArgumentParser(description='ETL Pipeline para cotações de boi gordo')
    parser.add_argument('command', choices=['single', 'range', 'backfill', 'stats'], 
                       help='Comando a executar')
    
    parser.add_argument('--date', help='Data única (YYYY-MM-DD)')
    parser.add_argument('--start-date', help='Data inicial para range (YYYY-MM-DD)')
    parser.add_argument('--end-date', help='Data final para range (YYYY-MM-DD)')
    parser.add_argument('--output-dir', default='output', help='Diretório de saída')
    parser.add_argument('--workers', type=int, default=3, help='Número de workers paralelos')
    parser.add_argument('--force', action='store_true', help='Reprocessar arquivos existentes')
    
    args = parser.parse_args()
    
    # Criar pipeline
    pipeline = ETLPipeline(args.output_dir, args.workers)
    
    if args.command == 'single':
        if not args.date:
            print("ERRO: --date é obrigatório para comando 'single'")
            return
        
        pipeline.collect_single_date(args.date)
    
    elif args.command == 'range':
        if not args.start_date or not args.end_date:
            print("ERRO: --start-date e --end-date são obrigatórios para comando 'range'")
            return
        
        pipeline.collect_date_range(args.start_date, args.end_date, not args.force)
    
    elif args.command == 'backfill':
        # Backfill dos últimos 30 dias por padrão
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        
        if args.start_date:
            start_date = args.start_date
        if args.end_date:
            end_date = args.end_date
        
        print(f"Backfill de {start_date} até {end_date}")
        pipeline.collect_date_range(start_date, end_date, not args.force)
    
    elif args.command == 'stats':
        stats = pipeline.processor.get_storage_stats()
        print("Estatísticas de Armazenamento:")
        
        total_size = 0
        total_files = 0
        
        for table_type, info in stats.items():
            print(f"  {table_type}:")
            print(f"    Arquivos: {info['files']}")
            print(f"    Tamanho: {info['size_mb']} MB")
            total_size += info['size_mb']
            total_files += info['files']
        
        print(f"\nTotal: {total_files} arquivos, {total_size:.2f} MB")

if __name__ == "__main__":
    main()
