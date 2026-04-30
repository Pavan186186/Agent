from fastapi import FastAPI
import sqlite3
import os

app = FastAPI()

# Point to the local_db folder
DB_PATH = os.path.join(os.path.dirname(__file__), "../../../local_db/bank_database.sqlite")

def get_db_connection():
    # Ensure the directory exists
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # 1. Build the expanded Accounts table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS accounts (
            user_id TEXT,
            owner_name TEXT,
            username TEXT,
            routing_number TEXT,
            account_number TEXT PRIMARY KEY,
            card_number TEXT,
            balance REAL,
            pin_hash TEXT
        )
    ''')

    # 2. Build the new Transactions table for fraud detection
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS transactions (
            tx_id INTEGER PRIMARY KEY AUTOINCREMENT,
            account_number TEXT,
            merchant TEXT,
            amount REAL,
            status TEXT,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    # 3. Seed the 5 target accounts if the table is empty
    cursor.execute("SELECT count(*) FROM accounts")
    if cursor.fetchone()[0] == 0:
        users = [
            ('U001', 'PAVAN TEJA TALLAPALLI', 'pavan_tallapalli', '111000111', '1000000001', '4111222233334441', 8500.5, '03ac674216f3e15c761ee1a5e255f067953623c8b388b4459e13f978d7c846f4'),
            ('U002', 'NITHIN KUMAR SURINENI', 'nithin_surineni', '111000111', '1000000002', '4111222233334442', 45000.0, '38083c7ee9121e17401883566a148aa5c2e2d55dc53bc4a94a026517dbff3c6b'),
            ('U003', 'SAHITHI KATOORI', 'sahithi_katoori', '222000222', '2000000001', '5111222233334443', 12400.75, 'ceaa28bba4caba687dc31b1bbe79eca3c70c33f871f1ce8f528cf9ab5cfd76dd'),
            ('U004', 'NIKHILESH GOUD', 'nikhilesh_goud', '333000333', '3000000001', '4555222233334444', 850.2, 'db2e7f1bd5ab9968ae76199b7cc74795ca7404d5a08d78567715ce532f9d2669'),
            ('U005', 'UDAY REDDY', 'uday_reddy', '111000111', '1000000003', '6011222233334445', 105000.0, 'f8638b979b2f4f793ddb6dbd197e0ee25a7a6ea32b0ae22f5e3c5d119d839e75')
        ]
        cursor.executemany("INSERT INTO accounts VALUES (?, ?, ?, ?, ?, ?, ?, ?)", users)

        # 4. Seed the dummy transactions
        txs = [
            ('1000000001', 'Sonic Drive-In', 14.50, 'Approved'),
            ('1000000001', 'Dutch Bros. Coffee', 6.50, 'Approved'),
            ('1000000001', 'UNKNOWN WIRE TRANSFER TO RUSSIA', 500.00, 'FLAGGED AS SUSPICIOUS'),
            ('1000000002', 'Waymo Ride', 18.00, 'Approved'),
            ('1000000002', 'Uber', 22.50, 'Approved'),
            ('2000000001', 'Arizona State University', 1200.00, 'Approved'),
            ('3000000001', 'Discord Nitro', 9.99, 'Approved'),
            ('1000000003', 'Royal Enfield Rental', 150.00, 'Approved')
        ]
        cursor.executemany("INSERT INTO transactions (account_number, merchant, amount, status) VALUES (?, ?, ?, ?)", txs)
        
        conn.commit()
        
    return conn

@app.get("/")
def read_root():
    """Health check endpoint to ensure the FastAPI server is running."""
    # Triggers database connection and seeding on boot if necessary
    get_db_connection()
    return {"status": "Secure Bank Vault is Online", "database": "Connected"}