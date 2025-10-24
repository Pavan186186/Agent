import pandas as pd
import sqlite3
import json
import os

DB_NAME='finance.db'
TRANSACTIONS_FILE='transactions_data.csv'
FRAUD_FILE='creditcard_2023.csv'
MCC_FILE='mcc_codes.json'

def load_data_to_sqlite():
    """
    Loads data from CSV and JSON files into a central SQLite database.
    """
    # 1. Createing a connection to the SQLite database
    conn=sqlite3.connect(DB_NAME)
    print(f"Database '{DB_NAME}' created/connected.")
    
    # 2. Loading and processing the main transactions file
    try:
        print(f"Loading '{TRANSACTIONS_FILE}'...")
        trans_df = pd.read_csv(TRANSACTIONS_FILE)
        # trans_df = pd.read_csv(TRANSACTIONS_FILE, nrows=200000)
        
        if 'timestamp' in trans_df.columns:
            trans_df['timestamp']=pd.to_datetime(trans_df['timestamp'])
        
        # Save to SQL
        trans_df.to_sql('transactions', conn, if_exists='replace', index=False)
        print(f"Successfully loaded {len(trans_df)} rows into 'transactions' table.")

    except FileNotFoundError:
        print(f"ERROR:'{TRANSACTIONS_FILE}' not found.")
        return
    except Exception as e:
        print(f"Error:{e}")
        return

    # 3. Load and process the fraud data
    try:
        print(f"Loading '{FRAUD_FILE}'...")
        fraud_df=pd.read_csv(FRAUD_FILE)
        
        # Save to SQL
        fraud_df.to_sql('fraud_data', conn, if_exists='replace', index=False)
        print(f"Successfully loaded {len(fraud_df)} rows into 'fraud_data' table.")
    
    except FileNotFoundError:
        print(f"ERROR:'{FRAUD_FILE}' not found.")
    except Exception as e:
        print(f"Error:{e}")

    # 4. Load and process the MCC (Merchant Category Codes) JSON
    try:
        print(f"Loading '{MCC_FILE}'...")
        with open(MCC_FILE, 'r') as f:
            mcc_data=json.load(f)
        
        # Convert the JSON (dictionary) into a DataFrame for normalization
        mcc_df=pd.DataFrame(mcc_data.items(), columns=['mcc','category_description'])
        
        # Save to SQL
        mcc_df.to_sql('mcc_codes', conn, if_exists='replace', index=False)
        print(f"Successfully loaded {len(mcc_df)} categories into 'mcc_codes' table.")
    
    except FileNotFoundError:
        print(f"ERROR:'{MCC_FILE}' not found.")
    except Exception as e:
        print(f"Error{e}")

    # 5. Close the database connection
    conn.close()
    print(f"All data loaded. Database connection closed.")

if __name__ == "__main__":
    load_data_to_sqlite()