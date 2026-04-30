from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import sqlite3
import os
import hashlib

app = FastAPI()

# --- FOOLPROOF PATHING ---
# Navigates up 3 levels to the root AGENT folder, ensures local_db exists
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DB_DIR = os.path.join(BASE_DIR, "local_db")
os.makedirs(DB_DIR, exist_ok=True)  # Fixes the SQLite crash!
DB_PATH = os.path.join(DB_DIR, "bank_database.sqlite")

def get_db_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    
    # Auto-initialize table if it doesn't exist
    conn.execute('''
        CREATE TABLE IF NOT EXISTS accounts (
            account_number TEXT PRIMARY KEY,
            owner_name TEXT NOT NULL,
            balance REAL NOT NULL,
            pin_hash TEXT NOT NULL
        )
    ''')
    
    # Seed Pavan's demo account if the database is empty
    cursor = conn.cursor()
    cursor.execute("SELECT count(*) FROM accounts")
    if cursor.fetchone()[0] == 0:
        # Inserting Pavan's account with pin_hash for '1234'
        conn.execute("INSERT INTO accounts VALUES ('1000000001', 'PAVAN TEJA TALLAPALLI', 8500.50, '03ac674216f3e15c761ee1a5e255f067953623c8b388b4459e13f978d7c846f4')")
        conn.commit()
        
    return conn

def hash_password(password: str) -> str:
    """Hashes the plain text pin/password into SHA-256"""
    return hashlib.sha256(password.encode()).hexdigest()

# --- Pydantic Models ---
class AuthRequest(BaseModel):
    account_number: str
    password: str

class TransferRequest(BaseModel):
    from_account: str
    password: str
    to_account: str
    amount: float

class CreateAccountRequest(BaseModel):
    account_number: str
    owner_name: str
    initial_balance: float
    password: str

# --- Secure Endpoints ---
@app.post("/accounts/balance")
def get_balance(req: AuthRequest):
    conn = get_db_connection()
    hashed_pin = hash_password(req.password)
    
    account = conn.execute('SELECT * FROM accounts WHERE account_number = ? AND pin_hash = ?', 
                           (req.account_number, hashed_pin)).fetchone()
    conn.close()
    
    if account is None:
        raise HTTPException(status_code=401, detail="Authentication failed. Invalid account number or PIN.")
    
    return {"account_number": req.account_number, "balance": account['balance']}

@app.post("/transactions/transfer")
def transfer_funds(req: TransferRequest):
    conn = get_db_connection()
    hashed_pin = hash_password(req.password)
    
    sender = conn.execute('SELECT * FROM accounts WHERE account_number = ? AND pin_hash = ?', 
                          (req.from_account, hashed_pin)).fetchone()
    if sender is None:
        conn.close()
        raise HTTPException(status_code=401, detail="Authentication failed. Invalid PIN.")
        
    if sender['balance'] < req.amount:
        conn.close()
        raise HTTPException(status_code=400, detail="Insufficient funds.")

    receiver = conn.execute('SELECT * FROM accounts WHERE account_number = ?', (req.to_account,)).fetchone()
    if receiver is None:
        conn.close()
        raise HTTPException(status_code=404, detail="Destination account not found.")

    try:
        conn.execute('UPDATE accounts SET balance = balance - ? WHERE account_number = ?', (req.amount, req.from_account))
        conn.execute('UPDATE accounts SET balance = balance + ? WHERE account_number = ?', (req.amount, req.to_account))
        conn.commit()
    except Exception:
        conn.rollback()
        raise HTTPException(status_code=500, detail="Transaction failed.")
    finally:
        conn.close()

    return {"message": f"Successfully transferred ${req.amount}."}

@app.post("/accounts/create")
def create_account(req: CreateAccountRequest):
    conn = get_db_connection()
    hashed_pin = hash_password(req.password)
    try:
        conn.execute('INSERT INTO accounts (account_number, owner_name, balance, pin_hash) VALUES (?, ?, ?, ?)',
                     (req.account_number, req.owner_name, req.initial_balance, hashed_pin))
        conn.commit()
    except sqlite3.IntegrityError:
        conn.close()
        raise HTTPException(status_code=400, detail="Account number already exists.")
    conn.close()
    return {"message": f"Account {req.account_number} created successfully."}