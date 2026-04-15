from fastmcp import FastMCP
import requests
import chromadb
import os

# Initialize the MCP Server
mcp = FastMCP("Banking Agent Server")

# --- Configuration ---
# Point to our running FastAPI Bank Server
BANK_API_URL = "http://127.0.0.1:8000"

# Point to our local ChromaDB (FIXED PATH: Only going up 2 levels)
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CHROMA_PATH = os.path.join(SCRIPT_DIR, '../../local_db/chroma_db_storage')

# Connect to the Vector DB directly in the MCP server
chroma_client = chromadb.PersistentClient(path=CHROMA_PATH)
knowledge_base = chroma_client.get_collection(name="bank_knowledge_base")

# ==========================================
# TOOL 1: Information Retrieval (RAG)
# ==========================================
@mcp.tool()
def query_bank_policy(query: str) -> str:
    """
    Searches the bank's official policy documents (Terms of Service, Fee Schedules, Limits).
    Use this tool whenever the user asks a question about bank rules, fees, limits, or procedures.
    """
    results = knowledge_base.query(
        query_texts=[query],
        n_results=2
    )
    
    if not results['documents'][0]:
        return "No relevant policy found for this query."
    
    # Combine the top 2 chunks of text to give the LLM full context
    combined_context = "\n\n".join(results['documents'][0])
    return f"Bank Policy Excerpt:\n{combined_context}"

# ==========================================
# TOOL 2: Read Tabular Data (Balance)
# ==========================================
@mcp.tool()
def get_account_balance(account_number: str) -> str:
    """
    Retrieves the current balance for a specific bank account number.
    """
    try:
        response = requests.get(f"{BANK_API_URL}/accounts/{account_number}/balance")
        if response.status_code == 200:
            data = response.json()
            return f"The current balance for account {account_number} is ${data['balance']:.2f}"
        else:
            return f"Error fetching balance: {response.json().get('detail', 'Unknown error')}"
    except Exception as e:
        return f"Server connection failed: {str(e)}"

# ==========================================
# TOOL 3: Write Tabular Data (Transfers)
# ==========================================
@mcp.tool()
def transfer_funds(from_account: str, to_account: str, amount: float) -> str:
    """
    Executes a secure transfer of funds from one account to another.
    """
    try:
        payload = {
            "from_account": from_account,
            "to_account": to_account,
            "amount": amount
        }
        response = requests.post(f"{BANK_API_URL}/transactions/transfer", json=payload)
        
        if response.status_code == 200:
            return response.json()['message']
        else:
            return f"Transfer failed: {response.json().get('detail', 'Unknown error')}"
    except Exception as e:
        return f"Server connection failed: {str(e)}"

if __name__ == "__main__":
    # This starts the MCP server over standard input/output (stdio), 
    # which is how MCP clients natively communicate with MCP servers.
    print("Starting Banking MCP Server...")
    mcp.run()