#!/usr/bin/env python3
"""
Main Pipeline - B3 Data Processing
Extracts, transforms and loads B3 data to Google Sheets
"""

import os
import sys
import pandas as pd
from datetime import datetime, timedelta
import json
import argparse

# Add the src directory to the path to import modules
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

# Import pipeline modules
from src import download_and_process_bdi, clean_table, filter_empty_rows, B3DataLoader

class B3Pipeline:
    def __init__(self, config_path="config/config.json"):
        """Initialize pipeline with configurations"""
        self.config = self.load_config(config_path)
        self.output_folder = self.config.get('output_folder', 'output')
        os.makedirs(self.output_folder, exist_ok=True)
    
    def load_config(self, config_path):
        """Load configurations from JSON file"""
        if os.path.exists(config_path):
            with open(config_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        else:
            # Configuração padrão
            default_config = {
                "product_name": "FatOx",
                "start_date": "2025-08-15",
                "end_date": "2025-08-30",    
                "output_folder": "output",
                "google_credentials": "config/credentials.json",
                "spreadsheet_id": "",
                "sheet_name": "B3_Data"
            }
            # Create default config file
            with open(config_path, 'w', encoding='utf-8') as f:
                json.dump(default_config, f, indent=2, ensure_ascii=False)
            print(f"Configuration file created: {config_path}")
            return default_config
    
    def extract_data(self):
        """Phase 1: Extraction - Collects data from B3 API"""
        print("=== PHASE 1: EXTRACTION ===")
        
        start = datetime.strptime(self.config['start_date'], "%Y-%m-%d")
        end = datetime.strptime(self.config['end_date'], "%Y-%m-%d")
        
        # Validação do período
        hoje = datetime.now()
        if start > hoje:
            print(f"⚠ WARNING: Start date ({start.date()}) is in the future!")
        if end > hoje:
            print(f"⚠ WARNING: End date ({end.date()}) is in the future!")

        # Verifica se o período não é muito longo
        dias_total = (end - start).days + 1
        if dias_total > 365:
            print(f"⚠ WARNING: Period too long ({dias_total} days). Consider shorter periods.")

        print(f"Period: {start.date()} to {end.date()} ({dias_total} days)")

        adjustments_table = {}
        all_expirations = set()
        
        current = start
        while current <= end:
            date_str = current.strftime("%Y-%m-%d")
            
            print(f"Extracting data for {date_str}...")
            df = download_and_process_bdi(date_str, date_str, self.config['product_name'])
            
            if df is not None:
                try:
                    if 'Vencimento' in df.columns and 'Ajuste' in df.columns:
                        day_adjustments = {}
                        
                        for _, row in df.iterrows():
                            expiration = str(row['Vencimento']).strip()
                            adjustment_raw = str(row['Ajuste']).strip()
                            
                            if adjustment_raw and adjustment_raw != 'nan' and adjustment_raw != '-':
                                adjustment_clean = adjustment_raw.replace('↓', '').replace('↑', '').replace(',', '.').strip()
                                try:
                                    adjustment_value = float(adjustment_clean)
                                    day_adjustments[expiration] = adjustment_value
                                    all_expirations.add(expiration)
                                except:
                                    day_adjustments[expiration] = None
                                    all_expirations.add(expiration)
                        
                        # Only add the day if there's at least one valid adjustment
                        if day_adjustments:
                            adjustments_table[date_str] = day_adjustments
                            print(f"  ✓ {len(day_adjustments)} expirations processed")
                        else:
                            print(f"  ⚠ No valid adjustments found")
                    else:
                        print(f"  ⚠ Columns not found")

                except Exception as e:
                    print(f"  ✗ Error processing {date_str}: {e}")
            else:
                print(f"  ⚠ No data for {date_str} (probably weekend/holiday)")
            
            current += timedelta(days=1)
        
        # Create transposed DataFrame
        if adjustments_table:
            ordered_expirations = sorted(list(all_expirations))
            records = []
            
            for day in sorted(adjustments_table.keys()):
                record = {'day': day}
                for exp in ordered_expirations:
                    record[exp] = adjustments_table[day].get(exp, None)
                records.append(record)
            
            transposed_df = pd.DataFrame(records)
            transposed_df.set_index('day', inplace=True)
            
            print(f"✓ Extraction completed: {transposed_df.shape[0]} days x {transposed_df.shape[1]} expirations")
            return transposed_df
        
        return None
    
    def transform_data(self, df):
        """Phase 2: Transformation - Applies treatments to data"""
        print("\\n=== PHASE 2: TRANSFORMATION ===")
        
        if df is None:
            print("✗ No data to transform")
            return None
        
        print(f"Original data: {df.shape}")
        
        # Reset index to treat as normal column
        df_reset = df.reset_index()
        
        # Apply basic transformations
        df_treated = clean_table(df_reset)
        
        # Remove rows where ALL data columns (except 'day') are empty
        df_treated = filter_empty_rows(df_treated, exclude_columns=['day'])
        
        # Return index to 'day'
        df_treated.set_index('day', inplace=True)
        
        print(f"✓ Transformation completed: {df_treated.shape}")
        return df_treated
    
    def load_data(self, df):
        """Phase 3: Loading - Saves locally and/or to Google Sheets"""
        print("\\n=== PHASE 3: LOADING ===")
        
        if df is None:
            print("✗ No data to load")
            return False

        # Save local file
        filename = f"ajustes_transpostos_{self.config['product_name']}_{self.config['start_date']}_{self.config['end_date']}.csv"
        filepath = os.path.join(self.output_folder, filename)
        df.to_csv(filepath, sep=";", encoding="utf-8")
        print(f"✓ File saved: {filepath}")

        # Load to Google Sheets (if configured)
        if (self.config.get('spreadsheet_id') and 
            os.path.exists(self.config.get('google_credentials', ''))):
            
            try:
                print("Loading to Google Sheets...")
                loader = B3DataLoader(self.config['google_credentials'])

                # Reset index to include 'day' column
                df_for_sheets = df.reset_index()
                
                result = loader.load_to_sheets(
                    df_for_sheets,
                    self.config['spreadsheet_id'],
                    self.config['sheet_name']
                )
                print(f"✓ Loading to Google Sheets: {result.get('updatedCells', 0)} cells")
                
            except Exception as e:
                print(f"⚠ Error loading to Google Sheets: {e}")
                print("Data saved locally only")

        return True
    
    def run(self):
        """Runs the complete pipeline"""
        print("🚀 STARTING B3 DATA PROCESSING PIPELINE")
        print(f"Product: {self.config['product_name']}")
        print(f"Period: {self.config['start_date']} to {self.config['end_date']}")
        print("-" * 50)
        
        try:
            # Phase 1: Extraction
            df_extracted = self.extract_data()

            # Phase 2: Transformation
            df_transformed = self.transform_data(df_extracted)

            # Phase 3: Loading
            success = self.load_data(df_transformed)
            
            if success:
                print("\\n🎉 PIPELINE EXECUTED SUCCESSFULLY!")
            else:
                print("\\n❌ PIPELINE FAILED")

        except Exception as e:
            print(f"\\n💥 CRITICAL ERROR: {e}")
            import traceback
            traceback.print_exc()

def main():
    """Main function"""
    parser = argparse.ArgumentParser(description='B3 Data Pipeline')
    parser.add_argument('--config', default='config/config.json', help='Configuration file')
    parser.add_argument('--start-date', help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end-date', help='End date (YYYY-MM-DD)')
    parser.add_argument('--product', help='Product name')
    
    args = parser.parse_args()

    # Initialize pipeline
    pipeline = B3Pipeline(args.config)

    # Override configurations via arguments
    if args.start_date:
        pipeline.config['start_date'] = args.start_date
    if args.end_date:
        pipeline.config['end_date'] = args.end_date
    if args.product:
        pipeline.config['product_name'] = args.product

    # Execute pipeline
    pipeline.run()

if __name__ == "__main__":
    main()
