"""
B3 data transformation module
Responsible for cleaning and processing extracted data
"""

import pandas as pd
import numpy as np

def clean_table(df):
    """
    Applies cleaning treatments to B3 data table
    
    Args:
        df (pd.DataFrame): DataFrame with raw data
        
    Returns:
        pd.DataFrame: Clean and processed DataFrame
        
    Applied treatments:
    - Removes completely empty rows
    - Removes rows with only empty strings/whitespace
    - Removes completely empty columns
    - Removes duplicate records
    - Cleans whitespace from text columns
    - Converts 'nan' strings to NaN
    - Resets index
    """
    if df is None or df.empty:
        print("⚠ Empty or None DataFrame received")
        return df
    
    initial_shape = df.shape
    print(f"Applying transformations to DataFrame {initial_shape}...")
    
    # Remove completely empty rows
    clean_df = df.dropna(how='all')
    
    # Remove rows where all columns are empty strings or whitespace
    clean_df = clean_df.loc[~(clean_df.astype(str).apply(lambda x: x.str.strip()) == '').all(axis=1)]
    
    # Remove completely empty columns
    clean_df = clean_df.dropna(axis=1, how='all')
    
    # Remove duplicates
    clean_df = clean_df.drop_duplicates()
    
    # Clean whitespace from text columns
    for col in clean_df.select_dtypes(include=['object']).columns:
        clean_df[col] = clean_df[col].astype(str).str.strip()
        clean_df[col] = clean_df[col].replace('nan', np.nan)
    
    # Reset index
    clean_df = clean_df.reset_index(drop=True)
    
    final_shape = clean_df.shape
    print(f"✓ Transformation completed: {initial_shape} → {final_shape}")
    
    return clean_df

def filter_empty_rows(df, exclude_columns=['day']):
    """
    Remove rows where all data columns (except specified ones) are empty
    
    Args:
        df (pd.DataFrame): DataFrame to filter
        exclude_columns (list): Columns to exclude from verification (ex: ['day', 'date'])
        
    Returns:
        pd.DataFrame: Filtered DataFrame without empty rows
    """
    if df is None or df.empty:
        return df
    
    print(f"Filtering empty rows...")
    initial_shape = df.shape
    
    # Identify data columns (excluding specified ones)
    data_columns = [col for col in df.columns if col not in exclude_columns]
    
    if not data_columns:
        print("⚠ No data columns found to filter")
        return df
    
    # Remove rows where ALL data columns are NaN/empty
    filtered_df = df.dropna(subset=data_columns, how='all')
    
    # Also remove rows where all data columns are empty strings
    empty_mask = filtered_df[data_columns].astype(str).apply(
        lambda row: all(val.strip() in ['', 'nan', 'None'] for val in row), axis=1
    )
    filtered_df = filtered_df[~empty_mask]
    
    final_shape = filtered_df.shape
    removed_rows = initial_shape[0] - final_shape[0]
    
    if removed_rows > 0:
        print(f"✓ {removed_rows} empty rows removed: {initial_shape} → {final_shape}")
    else:
        print(f"✓ No empty rows found")
    
    return filtered_df

def validate_b3_data(df):
    """
    Validates if DataFrame contains expected B3 columns
    
    Args:
        df (pd.DataFrame): DataFrame to validate
        
    Returns:
        dict: Validation result with status and messages
    """
    result = {
        'valid': True,
        'messages': [],
        'found_columns': list(df.columns) if df is not None else []
    }
    
    if df is None:
        result['valid'] = False
        result['messages'].append("DataFrame is None")
        return result
    
    if df.empty:
        result['valid'] = False
        result['messages'].append("DataFrame is empty")
        return result
    
    # Expected B3 columns
    expected_columns = ['Vencimento', 'Ajuste']
    missing_columns = [col for col in expected_columns if col not in df.columns]
    
    if missing_columns:
        result['valid'] = False
        result['messages'].append(f"Missing columns: {missing_columns}")
    else:
        result['messages'].append("All required columns are present")
    
    return result