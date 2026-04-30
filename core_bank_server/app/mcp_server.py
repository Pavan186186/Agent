from fastmcp import FastMCP
import hashlib
import sqlite3
from main import get_db_connection

mcp = FastMCP("Banking Agent Server")

@mcp.tool()
def get_account_balance(account_number: str, password: str) -> str:
    """Retrieves the current balance. Requires account number and password/PIN."""
    conn = get_db_connection()
    cursor = conn.cursor()
    hashed_password = hashlib.sha256(password.encode()).hexdigest()
    
    cursor.execute("SELECT balance FROM accounts WHERE account_number = ? AND pin_hash = ?", (account_number, hashed_password))
    result = cursor.fetchone()
    conn.close()
    
    if result:
        return f"The current balance for account {account_number} is ${result[0]:.2f}."
    return "Authentication Failed: Invalid account number or PIN."

@mcp.tool()
def transfer_funds(from_account: str, password: str, to_account: str, amount: float) -> str:
    """Executes a secure transfer between accounts and logs the transaction."""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # 1. HEURISTIC FRAUD DETECTION LAYER
    if amount > 2000.00:
        cursor.execute("INSERT INTO transactions (account_number, merchant, amount, status) VALUES (?, ?, ?, ?)", 
                       (from_account, f"BLOCKED Transfer to {to_account}", amount, "FLAGGED AS SUSPICIOUS"))
        conn.commit()
        conn.close()
        return "ERROR_FRAUD_FLAG: Transaction exceeds the $2,000 automated safety limit. The transfer has been blocked and the account is under review."

    hashed_password = hashlib.sha256(password.encode()).hexdigest()

    # 2. Verify Sender PIN
    cursor.execute("SELECT balance FROM accounts WHERE account_number = ? AND pin_hash = ?", (from_account, hashed_password))
    sender = cursor.fetchone()
    if not sender:
        conn.close()
        return "Authentication Failed: Invalid sender account number or PIN."

    # 3. Check Sufficient Funds
    if sender[0] < amount:
        conn.close()
        return "Transaction Failed: Insufficient funds."

    # 4. Verify Receiver Exists
    cursor.execute("SELECT balance FROM accounts WHERE account_number = ?", (to_account,))
    receiver = cursor.fetchone()
    if not receiver:
        conn.close()
        return "Transaction Failed: Destination account not found."

    # 5. Execute Transfer
    cursor.execute("UPDATE accounts SET balance = balance - ? WHERE account_number = ?", (amount, from_account))
    cursor.execute("UPDATE accounts SET balance = balance + ? WHERE account_number = ?", (amount, to_account))

    # 6. Log the Transactions in History
    cursor.execute("INSERT INTO transactions (account_number, merchant, amount, status) VALUES (?, ?, ?, ?)", 
                   (from_account, f"Wire Transfer to {to_account}", amount, "Approved"))
    cursor.execute("INSERT INTO transactions (account_number, merchant, amount, status) VALUES (?, ?, ?, ?)", 
                   (to_account, f"Wire Transfer from {from_account}", amount, "Approved"))

    conn.commit()
    
    cursor.execute("SELECT balance FROM accounts WHERE account_number = ?", (from_account,))
    new_balance = cursor.fetchone()[0]
    conn.close()

    return f"Success! ${amount:.2f} transferred to {to_account}. Your new balance is ${new_balance:.2f}."

@mcp.tool()
def get_recent_transactions(account_number: str, password: str) -> str:
    """Retrieves the most recent transactions for an account to check for unauthorized activity. Requires PIN."""
    conn = get_db_connection()
    cursor = conn.cursor()
    hashed_password = hashlib.sha256(password.encode()).hexdigest()
    
    cursor.execute("SELECT owner_name FROM accounts WHERE account_number = ? AND pin_hash = ?", (account_number, hashed_password))
    user = cursor.fetchone()
    
    if not user:
        conn.close()
        return "Authentication Failed: Invalid account number or PIN."
        
    cursor.execute("SELECT merchant, amount, status, timestamp FROM transactions WHERE account_number = ? ORDER BY tx_id DESC LIMIT 5", (account_number,))
    transactions = cursor.fetchall()
    conn.close()
    
    if not transactions:
        return "No recent transactions found for this account."
        
    result = f"Recent activity for {account_number} ({user[0]}):\n"
    for tx in transactions:
        result += f"- {tx[0]}: ${tx[1]:.2f} [{tx[2]}]\n"
        
    return result

@mcp.tool()
def create_new_account(account_number: str, owner_name: str, initial_balance: float, password: str) -> str:
    """Creates a new banking account."""
    conn = get_db_connection()
    cursor = conn.cursor()
    hashed_password = hashlib.sha256(password.encode()).hexdigest()
    
    try:
        # Fills in the new schema requirements with dummy values so the LLM doesn't break the database
        cursor.execute("INSERT INTO accounts (user_id, owner_name, username, routing_number, account_number, card_number, balance, pin_hash) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", 
                       ("NEW_USER", owner_name, "new_user", "000000000", account_number, "0000000000000000", initial_balance, hashed_password))
        conn.commit()
        conn.close()
        return f"Success: Account {account_number} created for {owner_name} with starting balance ${initial_balance:.2f}."
    except sqlite3.IntegrityError:
        conn.close()
        return f"Error: Account {account_number} already exists."

@mcp.tool()
def query_bank_policy(query: str) -> str:
    """Searches the bank's official policy documents. Use for rules, fees, and limits."""
    # Fallback heuristic if ChromaDB is offline
    if "transfer" in query.lower() or "limit" in query.lower() or "fraud" in query.lower():
        return "Bank Policy: All transfers over $2,000 are subject to automated fraud review and will be blocked. Standard wire transfer limits apply."
    return "Bank Policy: Please refer to the Master Funds Transfer Agreement for detailed rules."

if __name__ == "__main__":
    mcp.run()