from fastapi import FastAPI, HTTPException, Depends
from pydantic import BaseModel
import sqlite3
import hashlib
import os
from typing import List, Optional

app = FastAPI(title="Core Bank API", description="Simulated Bank Server for MCP Agent")

# Robust path to your database
DB_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../local_db/bank_database.sqlite"))

def get_db_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row  # Returns rows as dictionaries
    return conn

# --- Pydantic Models for API Requests/Responses ---
class LoginRequest(BaseModel):
    username: str
    pin: str

class TransferRequest(BaseModel):
    from_account: str
    to_account: str
    amount: float

# --- Core API Endpoints ---

@app.get("/")
def health_check():
    return {"status": "Bank Server is Online", "database_connected": os.path.exists(DB_PATH)}

@app.post("/auth/verify")
def verify_user(request: LoginRequest):
    """Verifies user credentials and returns user details (acting as a session token)."""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Hash the incoming PIN to compare with the database
    hashed_pin = hashlib.sha256(request.pin.encode()).hexdigest()
    
    cursor.execute("SELECT user_id, name, account_number FROM users WHERE username = ? AND pin_hash = ?", (request.username, hashed_pin))
    user = cursor.fetchone()
    conn.close()
    
    if not user:
        raise HTTPException(status_code=401, detail="Invalid username or PIN")
    
    # In a real app, you'd return a JWT here. For our demo, returning the user info acts as our "token".
    return {"status": "success", "user": dict(user)}

@app.get("/accounts/{account_number}/balance")
def get_balance(account_number: str):
    """Fetches the current balance for an account."""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT balance FROM accounts WHERE account_number = ?", (account_number,))
    result = cursor.fetchone()
    conn.close()
    
    if not result:
        raise HTTPException(status_code=404, detail="Account not found")
    
    return {"account_number": account_number, "balance": result["balance"]}

@app.post("/transactions/transfer")
def transfer_funds(request: TransferRequest):
    """Executes a strict SQL Transaction to move money between accounts."""
    if request.amount <= 0:
        raise HTTPException(status_code=400, detail="Transfer amount must be greater than zero")

    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # BEGIN TRANSACTION (SQLite does this automatically, but we explicitly handle errors)
        
        # 1. Check sender's balance
        cursor.execute("SELECT balance FROM accounts WHERE account_number = ?", (request.from_account,))
        sender = cursor.fetchone()
        if not sender:
            raise HTTPException(status_code=404, detail="Sender account not found")
        if sender["balance"] < request.amount:
            raise HTTPException(status_code=400, detail="Insufficient funds")
            
        # 2. Check receiver exists
        cursor.execute("SELECT account_number FROM accounts WHERE account_number = ?", (request.to_account,))
        if not cursor.fetchone():
            raise HTTPException(status_code=404, detail="Recipient account not found")

        # 3. Deduct from sender
        cursor.execute("UPDATE accounts SET balance = balance - ? WHERE account_number = ?", (request.amount, request.from_account))
        
        # 4. Add to receiver
        cursor.execute("UPDATE accounts SET balance = balance + ? WHERE account_number = ?", (request.amount, request.to_account))
        
        # 5. COMMIT
        conn.commit()
        return {"status": "success", "message": f"Successfully transferred ${request.amount} to {request.to_account}"}
        
    except sqlite3.Error as e:
        conn.rollback() # If ANYTHING fails, roll back the changes!
        raise HTTPException(status_code=500, detail=f"Database transaction failed: {str(e)}")
    finally:
        conn.close()