# Test to connect with the Work Deal Funnel Data

import os
import requests
import json
import numpy as np
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

api_token = os.getenv("MONDAY_API_TOKEN")
api_url = "https://api.monday.com/v2"
# Replace with your actual Deals Board ID
board_id = "5026870326" 

headers = {
    "Authorization": api_token,
    "Content-Type": "application/json"
}

def clean_deals_data(df):
    """
    Cleans and normalizes the raw Deals DataFrame fetched from monday.com.
    """
    # Create a copy to avoid SettingWithCopyWarning
    clean_df = df.copy()

    # 1. Normalize Currency & Numbers
    # Strip symbols (like $, commas, or text) from 'masked deal value' and convert to float
    if 'masked deal value' in clean_df.columns:
        clean_df['masked deal value'] = clean_df['masked deal value'].astype(str).str.replace(r'[^\d.]', '', regex=True)
        clean_df['masked deal value'] = pd.to_numeric(clean_df['masked deal value'], errors='coerce').fillna(0)
        
    # Remove '%' signs from 'closure probability' and convert to float
    if 'closure probability' in clean_df.columns:
        clean_df['closure probability'] = clean_df['closure probability'].astype(str).str.replace('%', '')
        clean_df['closure probability'] = pd.to_numeric(clean_df['closure probability'], errors='coerce').fillna(0)

    # 2. Standardize Dates
    # Convert various date string formats into standard Pandas datetime objects
    date_columns = ['tentative close data', 'created date']
    for col in date_columns:
        if col in clean_df.columns:
            clean_df[col] = pd.to_datetime(clean_df[col], errors='coerce')

    # 3. Clean Text & Categorical Statuses
    # Standardize casing and strip whitespace to ensure exact matches for AI filtering
    categorical_cols = ['deal status', 'deal stage', 'sector/service', 'product deal']
    for col in categorical_cols:
        if col in clean_df.columns:
            # Handle missing/null categorical values
            clean_df[col] = clean_df[col].fillna('Unknown').astype(str).str.strip().str.title()
            
    # 4. Handle General Missing Values
    # Fill any remaining completely blank text cells with 'Unknown'
    clean_df.replace({'': 'Unknown', 'None': 'Unknown', 'nan': 'Unknown'}, inplace=True)

    return clean_df

# This GraphQL query fetches the names and column values of items on the board
query = f"""
{{
  boards(ids: {board_id}) {{
    name
    items_page(limit: 1) {{
      items {{
        name
        column_values {{
          id
          value
          text
        }}
      }}
    }}
  }}
}}
"""

response = requests.post(api_url, headers=headers, json={"query": query})

if response.status_code == 200:
    data = response.json()
    items = data['data']['boards'][0]['items_page']['items']
    
    parsed_data = []
    for item in items:
        # Start the row with the main item name (e.g., the Deal Name)
        row = {'Deal Name': item['name']} 
        
        # Loop through the rest of the columns
        for col in item['column_values']:
            # Using col['text'] gets the human-readable version of the value
            row[col['id']] = col['text'] 
            
        parsed_data.append(row)

    # Convert to a Pandas DataFrame
    df = pd.DataFrame(parsed_data)
    clean_df = clean_deals_data(df)
    print("Cleaned Data Ready for AI:")
    print(clean_df.head())
    
else:
    print(f"Error: {response.status_code}")