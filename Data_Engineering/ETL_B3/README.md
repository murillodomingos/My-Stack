# 📊 B3 Data Pipeline

An automated ETL (Extract, Transform, Load) pipeline for B3 (Brasil Bolsa Balcão) financial data processing. This pipeline extracts adjustment data from B3's API, transforms it into a transposed format, and loads it into CSV files and Google Sheets.

## 🚀 Features

- **🔽 Data Extraction**: Collects adjustment data from B3 API for specified date ranges
- **⚙️ Data Transformation**: Cleans data and creates transposed tables (days × expirations)
- **📤 Data Loading**: Saves locally as CSV and optionally uploads to Google Sheets
- **🛠️ Configurable Pipeline**: JSON-based configuration for flexible execution
- **📅 Date Range Processing**: Handles multiple dates with automatic weekend/holiday detection
- **🔍 Data Validation**: Built-in validation for B3 data structure and content
- **📊 Progress Tracking**: Detailed logging throughout the pipeline execution

## 📁 Project Structure

```
teste_agro/
├── main.py                    # 🎯 Main pipeline orchestrator
├── config/                    # ⚙️ Configuration files
│   ├── config.json           # Main configuration
│   └── credentials.json      # Google Sheets credentials (optional)
├── src/                       # 📁 Source code modules
│   ├── __init__.py           # Package initialization
│   ├── extractor_b3.py       # 📥 B3 API data extraction
│   ├── transformer_b3.py     # 🔄 Data cleaning and transformation
│   └── loader_b3.py          # 📤 Google Sheets integration
├── output/                    # 📂 Generated CSV files
├── logs/                      # 📋 Log files (if configured)
├── requirements.txt          # 📦 Core dependencies
├── requirements-dev.txt      # 🛠️ Development dependencies
└── README.md                 # 📖 This documentation
```

## ⚙️ Installation & Setup

### 1. Install Dependencies

**Core dependencies:**
```bash
pip install -r requirements.txt
```

**Development environment (optional):**
```bash
pip install -r requirements-dev.txt
```

### 2. Configuration

Edit the `config/config.json` file:
```json
{
  "product_name": "FatOx",
  "start_date": "2025-08-01", 
  "end_date": "2025-08-21",
  "output_folder": "output",
  "google_credentials": "config/credentials.json",
  "spreadsheet_id": "your_spreadsheet_id_here",
  "sheet_name": "B3_Data"
}
```

### 3. Google Sheets Integration (Optional)

To enable Google Sheets upload:

1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Create a new project or select existing one
3. Enable the "Google Sheets API"
4. Create a Service Account credential
5. Download the JSON file and save as `config/credentials.json`
6. Share your target spreadsheet with the service account email

## 🏃‍♂️ Usage

### Basic Execution
```bash
python main.py
```

### Custom Parameters
```bash
# Specify date range and product
python main.py --start-date 2025-08-01 --end-date 2025-08-21 --product FatOx

# Use custom configuration file
python main.py --config config/custom_config.json
```

### Available Command Line Arguments

| Argument | Description | Example |
|----------|-------------|---------|
| `--config` | Configuration file path | `--config config/config.json` |
| `--start-date` | Start date (YYYY-MM-DD) | `--start-date 2025-08-01` |
| `--end-date` | End date (YYYY-MM-DD) | `--end-date 2025-08-21` |
| `--product` | Product name | `--product FatOx` |

## 📊 Pipeline Phases

### Phase 1: Extraction
- Connects to B3 API endpoint
- Downloads CSV data for each day in the specified range
- Handles network errors and retries
- Validates data structure (Vencimento, Ajuste columns)

### Phase 2: Transformation
- Cleans raw data (removes empty rows/columns)
- Filters out invalid adjustments
- Creates transposed format (days as rows, expirations as columns)
- Applies data validation rules

### Phase 3: Loading
- Saves processed data as CSV file locally
- Optionally uploads to Google Sheets
- Adds metadata and timestamps
- Provides execution summary

## 📈 Output Format

The pipeline generates a transposed table with:
- **Rows**: Trading days
- **Columns**: Contract expirations
- **Values**: Daily adjustment values

### Example CSV Output:
```csv
day,F26,G26,K26,Q25,U25,V25,X25,Z25
2025-08-01,333.25,327.25,327.25,316.4,323.95,332.55,334.85,333.35
2025-08-05,339.15,335.7,333.15,319.0,328.6,337.9,339.8,339.05
2025-08-06,340.9,337.8,335.15,320.55,330.1,339.5,341.4,340.7
```

## 🔧 Module API

### Using Individual Modules

```python
from src.extractor_b3 import download_and_process_bdi
from src.transformer_b3 import clean_table, filter_empty_rows
from src.loader_b3 import B3DataLoader

# Extract data for a specific date
df = download_and_process_bdi("2025-08-21", "2025-08-21", "FatOx")

# Clean and transform data
df_clean = clean_table(df)
df_filtered = filter_empty_rows(df_clean, exclude_columns=['day'])

# Load to Google Sheets
loader = B3DataLoader("config/credentials.json")
result = loader.load_to_sheets(df_filtered, "spreadsheet_id", "Sheet1")
```

### Core Functions

**extractor_b3.py**
- `download_and_process_bdi(date_from, date_to, product_name)`: Downloads B3 data

**transformer_b3.py**
- `clean_table(df)`: Cleans raw data
- `filter_empty_rows(df, exclude_columns)`: Removes empty data rows
- `validate_b3_data(df)`: Validates data structure

**loader_b3.py**
- `B3DataLoader(credentials_path)`: Google Sheets integration class
- `load_to_sheets(df, spreadsheet_id, sheet_name)`: Uploads data to sheets

## 🐛 Troubleshooting

### Common Issues

**Import Errors**
```bash
# Make sure to run from the project root directory
cd teste_agro
python main.py
```

**Google Sheets Authentication Failed**
- Verify `config/credentials.json` exists and is valid
- Check if the spreadsheet ID is correct in config.json
- Ensure the service account has edit access to the spreadsheet

**No Data Retrieved**
- Weekends and holidays have no trading data
- Verify the product name exists on B3 for the specified period
- Check internet connection for API access

**Date Format Errors**
- Ensure dates are in YYYY-MM-DD format
- Start date must be before or equal to end date
- Avoid dates too far in the future

### Debug Mode

For detailed debugging information:
```python
# Enable verbose logging in your code
import logging
logging.basicConfig(level=logging.DEBUG)
```

## 🧪 Development

### Running Tests
```bash
# Install development dependencies first
pip install -r requirements-dev.txt

# Run tests
python -m pytest tests/ -v

# Run with coverage
python -m pytest tests/ -v --cov=src --cov-report=html
```

### Code Quality
```bash
# Format code
black src/ tests/ main.py

# Sort imports
isort src/ tests/ main.py

# Lint code
flake8 src/ tests/ main.py

# Type checking
mypy src/
```

### Project Standards
- **Code Style**: Black formatter (88 characters)
- **Import Sorting**: isort with Black profile
- **Linting**: flake8 for code quality
- **Testing**: pytest for unit tests
- **Type Hints**: mypy for static type checking

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Make your changes following the coding standards
4. Add tests for new functionality
5. Run the test suite and ensure all tests pass
6. Commit your changes (`git commit -m 'Add amazing feature'`)
7. Push to the branch (`git push origin feature/amazing-feature`)
8. Open a Pull Request

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- [B3 - Brasil Bolsa Balcão](https://www.b3.com.br/) for providing the financial data API
- [Google Sheets API](https://developers.google.com/sheets/api) for spreadsheet integration
- [Pandas](https://pandas.pydata.org/) for data manipulation capabilities

---

**Made with ❤️ for financial data processing**
