from fastmcp import FastMCP
import requests

mcp = FastMCP("Banking Agent Server")
API_BASE = "http://127.0.0.1:8000"

@mcp.tool()
async def get_account_balance(account_number: str, password: str) -> str:
    """Retrieves account balance. Requires password/PIN."""
    try:
        response = requests.post(f"{API_BASE}/accounts/balance", json={
            "account_number": account_number,
            "password": password
        })
        if response.status_code == 200:
            data = response.json()
            return f"Balance for {account_number}: ${data['balance']}"
        return f"Error: {response.json().get('detail', 'Authentication Failed')}"
    except Exception as e:
        return f"API Connection Error: {str(e)}"

@mcp.tool()
async def transfer_funds(from_account: str, password: str, to_account: str, amount: float) -> str:
    """Transfers funds. Requires sender's password/PIN."""
    try:
        response = requests.post(f"{API_BASE}/transactions/transfer", json={
            "from_account": from_account,
            "password": password,
            "to_account": to_account,
            "amount": amount
        })
        if response.status_code == 200:
            return response.json()["message"]
        return f"Error: {response.json().get('detail', 'Transfer Failed')}"
    except Exception as e:
        return f"API Connection Error: {str(e)}"

@mcp.tool()
async def create_new_account(account_number: str, owner_name: str, initial_balance: float, password: str) -> str:
    """Creates a new bank account."""
    try:
        response = requests.post(f"{API_BASE}/accounts/create", json={
            "account_number": account_number,
            "owner_name": owner_name,
            "initial_balance": initial_balance,
            "password": password
        })
        if response.status_code == 200:
            return response.json()["message"]
        return f"Error: {response.json().get('detail', 'Creation Failed')}"
    except Exception as e:
        return f"API Connection Error: {str(e)}"

if __name__ == "__main__":
    mcp.run(transport='stdio')