import requests
import pandas as pd
import os
from datetime import datetime, timedelta
from io import StringIO

# B3 API URL
B3_API_URL = "https://arquivos.b3.com.br/bdi/table/export/csv?lang=pt-BR"

def download_and_process_bdi(date_from, date_to, product_name):
    """
    Downloads and processes B3 data for a specific period
    
    Args:
        date_from (str): Start date in YYYY-MM-DD format
        date_to (str): End date in YYYY-MM-DD format  
        product_name (str): Product name (ex: FatOx)
    
    Returns:
        pd.DataFrame or None: DataFrame with data or None if no data available
    """
    payload = {
        "Name": product_name,
        "Date": date_from,
        "FinalDate": date_to,
        "ClientId": "",
        "Filters": {}
    }
    
    try:
        response = requests.post(B3_API_URL, json=payload)
        if response.status_code == 200 and len(response.content) > 0:
            # Process CSV directly from memory
            csv_content = response.content.decode('latin1')
            
            # Check if there's useful content
            if len(csv_content.strip()) < 10:  # CSV too small
                return None
                
            try:
                df = pd.read_csv(StringIO(csv_content), sep=";", skiprows=2)
                
                # Check if DataFrame has useful data
                if df.empty or len(df.columns) == 0:
                    return None
                    
                return df
            except pd.errors.EmptyDataError:
                # Empty or malformed CSV
                return None
            except Exception as parse_error:
                print(f"{date_from}: Error parsing CSV - {parse_error}")
                return None
        else:
            return None
    except requests.exceptions.RequestException as req_error:
        print(f"{date_from}: Network error - {req_error}")
        return None
    except Exception as e:
        print(f"{date_from}: Unexpected error - {e}")
        return None


