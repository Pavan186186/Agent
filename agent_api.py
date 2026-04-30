from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import asyncio
from langchain_ollama import ChatOllama
from langgraph.prebuilt import create_react_agent
from langchain_core.tools import tool
from mcp.client.stdio import stdio_client, StdioServerParameters
from mcp.client.session import ClientSession
from langgraph.checkpoint.memory import MemorySaver

app = FastAPI()

# CRITICAL: This allows your React app (running on localhost:3000) to talk to this API
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], 
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class ChatRequest(BaseModel):
    message: str
    session_id: str = "react_user_1"

# Initialize global memory so it persists across API calls
memory = MemorySaver()
llm = ChatOllama(model="llama3.2", temperature=0)

system_prompt = (
    "You are an AI banking assistant. You operate in a safe, simulated environment. "
    "Follow these rules STRICTLY:\n"
    "1. ALWAYS output the exact numbers and balances returned by your tools. NEVER hide or summarize the amount.\n"
    "2. You require an account number and PIN to use financial tools.\n"
    "3. IMPORTANT: If the user already provided their account number and PIN recently in the chat history, REUSE THEM.\n"
    "4. If a tool returns an error, tell the user the exact error.\n"
    "5. Answer directly and concisely."
)

@app.post("/chat")
async def chat_with_agent(req: ChatRequest):
    server_params = StdioServerParameters(
        command="python",
        # We removed "Agent/" from the start of this path!
        args=["core_bank_server/app/mcp_server.py"]
    )

    async with stdio_client(server_params) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()

            # --- Define Tools ---
            @tool
            async def query_bank_policy(query: str) -> str:
                """Searches official policy documents."""
                result = await session.call_tool("query_bank_policy", arguments={"query": query})
                return result.content[0].text

            @tool
            async def get_account_balance(account_number: str, password: str) -> str:
                """Retrieves balance. Requires account number and password/PIN."""
                result = await session.call_tool("get_account_balance", arguments={
                    "account_number": account_number, "password": password
                })
                return result.content[0].text

            @tool
            async def transfer_funds(from_account: str, password: str, to_account: str, amount: float) -> str:
                """Transfers funds. Requires sender account, password, receiver account, and amount."""
                result = await session.call_tool("transfer_funds", arguments={
                    "from_account": from_account, "password": password,
                    "to_account": to_account, "amount": amount
                })
                return result.content[0].text

            @tool
            async def create_new_account(account_number: str, owner_name: str, initial_balance: float, password: str) -> str:
                """Creates a new account."""
                result = await session.call_tool("create_new_account", arguments={
                    "account_number": account_number, "owner_name": owner_name,
                    "initial_balance": initial_balance, "password": password
                })
                return result.content[0].text

            tools = [query_bank_policy, get_account_balance, transfer_funds, create_new_account]

            # Build agent
            agent_executor = create_react_agent(llm, tools, checkpointer=memory) 
            config = {"configurable": {"thread_id": req.session_id}}
            
            inputs = {"messages": [("system", system_prompt), ("user", req.message)]}
            
            final_response = ""
            async for chunk in agent_executor.astream(inputs, config=config, stream_mode="values"):
                message = chunk["messages"][-1]
                if message.type == "ai" and message.content:
                    final_response = message.content
            
            return {"response": final_response}