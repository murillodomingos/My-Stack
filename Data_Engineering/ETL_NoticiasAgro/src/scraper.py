from bs4 import BeautifulSoup
import requests
import pandas as pd
from datetime import datetime
import json
from typing import Dict, List, Any

NA_SCRAPER_URL = "https://www.noticiasagricolas.com.br/cotacoes/boi-gordo/{}"

def scrap_ox_data(date_str: str) -> Dict[str, Any]:
    """
    Get all ox tables for a specific date

    Args:
        date_str (str): YYYY-MM-DD (ex: '2025-08-20')
    
    Returns:
        Dict[str, Any]: Data dictionary with all extracted tables
    """
    try:
        # Make request
        url = NA_SCRAPER_URL.format(date_str)
        response = requests.get(url)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')

        # Dictionary to store all extracted tables
        data = {
            'date': date_str,
            'url': url,
            'collected_at': datetime.now().isoformat(),
            'tables': {}
        }
        
        # Search all tables
        sections = soup.find_all('h2', class_=lambda x: x and ('cotacao' in str(x).lower() or 'indicador' in str(x).lower()))
        if not sections:
            # Fallback
            sections = soup.find_all('h2')
          
        for section in sections:
            section_title = section.get_text(strip=True)
            if not section_title:
                continue
                
            # Buscar tabela próxima à seção
            table = section.find_next('table')
            if not table:
                continue
            
            # Extrair dados da tabela
            table_data = extract_table_data(table, section_title)
            if table_data:
                data['tables'][section_title] = table_data
        
        return data
        
    except Exception as e:
        return {
            'date': date_str,
            'error': str(e),
            'collected_at': datetime.now().isoformat()
        }

def extract_table_data(table, section_title: str) -> List[Dict[str, Any]]:
    """
    Extrai dados de uma tabela HTML
    
    Args:
        table: Elemento BeautifulSoup da tabela
        section_title (str): Título da seção
    
    Returns:
        List[Dict[str, Any]]: Lista de dicionários com os dados da tabela
    """
    try:
        rows = table.find_all('tr')
        if not rows:
            return []
        
        data = []
        headers = []
        
        for i, row in enumerate(rows):
            cells = row.find_all(['th', 'td'])
            if not cells:
                continue
            
            cell_texts = [cell.get_text(strip=True) for cell in cells]
            
            # Primeira linha não vazia pode ser cabeçalho
            if i == 0 or not headers:
                # Verificar se parece com cabeçalho
                if any(text.lower() in ['data', 'preço', 'variação', 'estado', 'região'] for text in cell_texts):
                    headers = cell_texts
                    continue
            
            # Pular linhas vazias
            if not any(cell_texts):
                continue
            
            # Criar dicionário da linha
            if headers:
                row_dict = {}
                for j, value in enumerate(cell_texts):
                    header = headers[j] if j < len(headers) else f'column_{j}'
                    row_dict[header] = value
            else:
                # Sem cabeçalho identificado, usar índices
                row_dict = {f'column_{j}': value for j, value in enumerate(cell_texts)}
            
            row_dict['section'] = section_title
            data.append(row_dict)
        
        return data
        
    except Exception as e:
        return [{'error': str(e), 'section': section_title}]

def scrap_multiple_dates(start_date: str, end_date: str) -> List[Dict[str, Any]]:
    """
    Coleta dados para múltiplas datas
    
    Args:
        start_date (str): Data inicial no formato YYYY-MM-DD
        end_date (str): Data final no formato YYYY-MM-DD
    
    Returns:
        List[Dict[str, Any]]: Lista com dados de todas as datas
    """
    from datetime import datetime, timedelta
    
    start = datetime.strptime(start_date, '%Y-%m-%d')
    end = datetime.strptime(end_date, '%Y-%m-%d')
    
    all_data = []
    current = start
    
    while current <= end:
        date_str = current.strftime('%Y-%m-%d')
        print(f"Coletando dados para {date_str}...")
        
        data = scrap_ox_data(date_str)
        all_data.append(data)
        
        current += timedelta(days=1)
    
    return all_data

def save_data_to_csv(data: List[Dict[str, Any]], output_file: str):
    """
    Salva os dados coletados em arquivo CSV
    
    Args:
        data (List[Dict[str, Any]]): Dados coletados
        output_file (str): Caminho do arquivo de saída
    """
    all_rows = []
    
    for day_data in data:
        if 'error' in day_data:
            continue
            
        date = day_data.get('date', '')
        
        for section_name, table_data in day_data.get('tables', {}).items():
            for row in table_data:
                row['date'] = date
                row['section_name'] = section_name
                all_rows.append(row)
    
    if all_rows:
        df = pd.DataFrame(all_rows)
        df.to_csv(output_file, index=False, encoding='utf-8')
        print(f"Dados salvos em: {output_file}")
    else:
        print("Nenhum dado encontrado para salvar.")

# Exemplo de uso
if __name__ == "__main__":
    # Coletar dados de uma data específica
    single_date_data = scrap_ox_data("2025-08-20")
    print(json.dumps(single_date_data, indent=2, ensure_ascii=False))
    
    # Ou coletar dados de múltiplas datas
    # multiple_dates_data = scrap_multiple_dates("2025-08-15", "2025-08-20")
    # save_data_to_csv(multiple_dates_data, "cotacoes_boi_gordo.csv")

