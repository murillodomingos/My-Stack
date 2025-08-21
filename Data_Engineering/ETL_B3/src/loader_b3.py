import pandas as pd
from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials
from googleapiclient.errors import HttpError
import os
from datetime import datetime

class B3DataLoader:
    def __init__(self, credentials_path):
        """
        Initialize loader for Google Sheets
        
        Args:
            credentials_path: Path to JSON credentials file
        """
        self.credentials_path = credentials_path
        self.service = self._authenticate()
    
    def _authenticate(self):
        """Authenticate with Google Sheets API"""
        try:
            if not os.path.exists(self.credentials_path):
                raise FileNotFoundError(f"Credentials file not found: {self.credentials_path}")
            
            scopes = ['https://www.googleapis.com/auth/spreadsheets']
            credentials = Credentials.from_service_account_file(
                self.credentials_path, scopes=scopes
            )
            service = build('sheets', 'v4', credentials=credentials)
            print(f"✅ Google Sheets authentication successful")
            return service
        except Exception as e:
            print(f"❌ Authentication error: {e}")
            print(f"💡 Make sure that:")
            print(f"   1. The file {self.credentials_path} exists")
            print(f"   2. It's a valid Service Account JSON file")
            print(f"   3. Google Sheets API is enabled in the project")
            raise
    
    def check_sheet_exists(self, spreadsheet_id, sheet_name):
        """Check if sheet tab exists in spreadsheet"""
        try:
            spreadsheet = self.service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
            sheet_names = [sheet['properties']['title'] for sheet in spreadsheet['sheets']]
            return sheet_name in sheet_names
        except HttpError as e:
            print(f"❌ Error checking spreadsheet: {e}")
            return False
    
    def create_sheet(self, spreadsheet_id, sheet_name):
        """Create a new sheet tab in spreadsheet"""
        try:
            request = {
                'requests': [{
                    'addSheet': {
                        'properties': {
                            'title': sheet_name
                        }
                    }
                }]
            }
            
            result = self.service.spreadsheets().batchUpdate(
                spreadsheetId=spreadsheet_id,
                body=request
            ).execute()
            
            print(f"✅ Sheet '{sheet_name}' created successfully")
            return True
        except HttpError as e:
            print(f"❌ Error creating sheet '{sheet_name}': {e}")
            return False
    
    def load_to_sheets(self, df, spreadsheet_id, sheet_name='B3_Data', clear_existing=True):
        """
        Load DataFrame to Google Sheets
        
        Args:
            df: DataFrame with data
            spreadsheet_id: Google spreadsheet ID
            sheet_name: Sheet tab name
            clear_existing: Whether to clear existing data
        """
        try:
            print(f"📤 Loading data to Google Sheets...")
            print(f"   Spreadsheet ID: {spreadsheet_id}")
            print(f"   Sheet: {sheet_name}")
            print(f"   Data: {df.shape[0]} rows x {df.shape[1]} columns")
            
            # Check if sheet exists, if not, create it
            if not self.check_sheet_exists(spreadsheet_id, sheet_name):
                print(f"⚠ Sheet '{sheet_name}' doesn't exist. Creating...")
                if not self.create_sheet(spreadsheet_id, sheet_name):
                    raise Exception(f"Could not create sheet '{sheet_name}'")
            
            # Prepare data - convert everything to string to avoid issues
            df_copy = df.copy()
            
            # Convert values to appropriate format for Google Sheets
            for col in df_copy.columns:
                df_copy[col] = df_copy[col].astype(str).replace('nan', '')
            
            values = [df_copy.columns.tolist()] + df_copy.values.tolist()
            
            # Clear existing data if requested
            if clear_existing:
                print(f"🧹 Clearing existing data...")
                try:
                    self.service.spreadsheets().values().clear(
                        spreadsheetId=spreadsheet_id,
                        range=f'{sheet_name}!A:Z'
                    ).execute()
                except HttpError as e:
                    print(f"⚠ Warning while clearing data: {e}")
            
            # Load new data
            print(f"📊 Sending data...")
            body = {
                'values': values,
                'majorDimension': 'ROWS'
            }
            
            result = self.service.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id,
                range=f'{sheet_name}!A1',
                valueInputOption='USER_ENTERED',  # Allows automatic formatting
                body=body
            ).execute()
            
            updated_cells = result.get('updatedCells', 0)
            updated_rows = result.get('updatedRows', 0)
            
            print(f"✅ Loading completed!")
            print(f"   📊 {updated_rows} rows updated")
            print(f"   📝 {updated_cells} cells updated")
            print(f"   🔗 Link: https://docs.google.com/spreadsheets/d/{spreadsheet_id}")
            
            # Add timestamp to sheet
            self._add_timestamp(spreadsheet_id, sheet_name, df.shape)
            
            return result
            
        except HttpError as http_error:
            error_details = http_error.error_details if hasattr(http_error, 'error_details') else []
            print(f"❌ Google Sheets HTTP error: {http_error}")
            
            if http_error.resp.status == 403:
                print(f"💡 Possible solutions:")
                print(f"   1. Share spreadsheet with service account email")
                print(f"   2. Check if Google Sheets API is enabled")
                print(f"   3. Confirm service account permissions")
            elif http_error.resp.status == 404:
                print(f"💡 Spreadsheet not found. Check ID: {spreadsheet_id}")
            
            raise
        except Exception as e:
            print(f"❌ Unexpected error: {e}")
            raise
    
    def _add_timestamp(self, spreadsheet_id, sheet_name, data_shape):
        """Add last update timestamp"""
        try:
            timestamp = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
            info_text = f"Last update: {timestamp} | Data: {data_shape[0]} rows x {data_shape[1]} columns"
            
            # Add on row after data
            row_position = data_shape[0] + 3  # +1 for header +2 for space
            
            self.service.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id,
                range=f'{sheet_name}!A{row_position}',
                valueInputOption='RAW',
                body={'values': [[info_text]]}
            ).execute()
            
        except Exception as e:
            print(f"⚠ Could not add timestamp: {e}")

    def test_connection(self, spreadsheet_id):
        """Test connection with specific spreadsheet"""
        try:
            print(f"🔍 Testing connection with spreadsheet...")
            spreadsheet = self.service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
            
            title = spreadsheet.get('properties', {}).get('title', 'Untitled')
            sheets = [sheet['properties']['title'] for sheet in spreadsheet.get('sheets', [])]
            
            print(f"✅ Connection successful!")
            print(f"   📋 Title: {title}")
            print(f"   📑 Available sheets: {', '.join(sheets)}")
            
            return True
        except HttpError as e:
            print(f"❌ Connection error: {e}")
            return False
        except Exception as e:
            print(f"❌ Unexpected error: {e}")
            return False

# Usage example
if __name__ == "__main__":
    # Settings
    CREDENTIALS_PATH = "path/to/your/credentials.json"
    SPREADSHEET_ID = "your_spreadsheet_id"
    
    # Load your transformed data
    df = pd.read_csv("transformed_data.csv")  # or your data source
    
    # Initialize loader
    loader = B3DataLoader(CREDENTIALS_PATH)
    
    # Load to Google Sheets
    loader.load_to_sheets(df, SPREADSHEET_ID, "B3_Data")