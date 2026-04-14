import sqlite3
import pandas as pd
import os

# 1. Path Resolution
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(SCRIPT_DIR, '../../local_db/bank_database.sqlite')
USERS_CSV = os.path.join(SCRIPT_DIR, '../demo_users.csv')
TXN_CSV = os.path.join(SCRIPT_DIR, '../demo_transactions.csv')

# Ensure the local_db directory exists
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

def initialize_database():
    print(f"Connecting to SQLite database at {os.path.abspath(DB_PATH)}...")
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # 2. Create Tables
    print("Creating database schema...")
    
    # Users Table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS users (
            user_id TEXT PRIMARY KEY,
            name TEXT,
            username TEXT UNIQUE,
            routing_number TEXT,
            account_number TEXT,
            card_number TEXT,
            balance REAL,
            pin_hash TEXT
        )
    ''')

    # Transactions Table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS transactions (
            transaction_id TEXT PRIMARY KEY,
            card_number TEXT,
            timestamp TEXT,
            merchant_category TEXT,
            merchant_type TEXT,
            merchant TEXT,
            amount REAL,
            currency TEXT,
            country TEXT,
            city TEXT,
            city_size TEXT,
            card_type TEXT,
            card_present TEXT,
            device TEXT,
            channel TEXT,
            device_fingerprint TEXT,
            ip_address TEXT,
            distance_from_home REAL,
            high_risk_merchant TEXT,
            transaction_hour INTEGER,
            weekend_transaction TEXT,
            velocity_last_hour TEXT,
            is_fraud TEXT,
            user_id TEXT,
            account_number TEXT
        )
    ''')

    # Beneficiaries Table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS beneficiaries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id TEXT,
            payee_name TEXT,
            account_number TEXT,
            routing_number TEXT
        )
    ''')

    # 3. Load Data from CSVs
    print("Loading data from CSVs...")
    users_df = pd.read_csv(USERS_CSV)
    txn_df = pd.read_csv(TXN_CSV)

    # 4. Insert Data into Tables
    print("Populating tables...")
    
    # Insert entire dataframes directly
    users_df.to_sql('users', conn, if_exists='replace', index=False)
    txn_df.to_sql('transactions', conn, if_exists='replace', index=False)

    conn.commit()
    conn.close()
    print("Database initialization complete! The Vault is secured.")

if __name__ == "__main__":
    initialize_database()
