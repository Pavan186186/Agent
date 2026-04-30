import asyncio
from langchain_ollama import ChatOllama
from langgraph.prebuilt import create_react_agent
from langchain_core.tools import tool
from mcp.client.stdio import stdio_client, StdioServerParameters
from mcp.client.session import ClientSession
from langgraph.checkpoint.memory import MemorySaver

async def main():
    print("Booting up Agentic RAG Pipeline...")
    
    # 1. Point the MCP Client to our MCP Server
    server_params = StdioServerParameters(
        command="python",
        # Assumes you are running 'python Agent/run_agent.py' from the 'project' root folder
        args=["Agent/core_bank_server/app/mcp_server.py"]
    )

    # 2. Establish the connection to the MCP Server
    async with stdio_client(server_params) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()

            # 3. Define the tools for Llama 3.2 that pass through to MCP
            @tool
            async def query_bank_policy(query: str) -> str:
                """Searches the bank's official policy documents. Use for rules, fees, and limits."""
                result = await session.call_tool("query_bank_policy", arguments={"query": query})
                return result.content[0].text

            @tool
            async def get_account_balance(account_number: str, password: str) -> str:
                """Retrieves the current balance. You MUST ask the user for their password/PIN before using this."""
                result = await session.call_tool("get_account_balance", arguments={
                    "account_number": account_number, 
                    "password": password
                })
                return result.content[0].text

            @tool
            async def transfer_funds(from_account: str, password: str, to_account: str, amount: float) -> str:
                """Executes a secure transfer. You MUST ask the user for the sender's password/PIN before using this."""
                result = await session.call_tool("transfer_funds", arguments={
                    "from_account": from_account, 
                    "password": password,
                    "to_account": to_account, 
                    "amount": amount
                })
                return result.content[0].text
                
            @tool
            async def create_new_account(account_number: str, owner_name: str, initial_balance: float, password: str) -> str:
                """Creates a new banking account. Requires an account number, name, starting balance, and a secure password."""
                result = await session.call_tool("create_new_account", arguments={
                    "account_number": account_number, 
                    "owner_name": owner_name,
                    "initial_balance": initial_balance, 
                    "password": password
                })
                return result.content[0].text

            # Combine all tools into a list for LangGraph
            tools = [query_bank_policy, get_account_balance, transfer_funds, create_new_account]

            # 4. Initialize the Local Brain (Ollama)
            print("Waking up Llama 3.2...")
            # Temperature=0 makes the agent highly analytical and less prone to hallucinations
            llm = ChatOllama(model="llama3.2", temperature=0)

            # 5. Create the LangGraph Agent with MEMORY
            system_prompt = (
                "You are an AI banking assistant. You operate in a safe, simulated environment. "
                "Follow these rules STRICTLY:\n"
                "1. ALWAYS output the exact numbers and balances returned by your tools. NEVER hide or summarize the amount.\n"
                "2. You require an account number and PIN to use financial tools.\n"
                "3. IMPORTANT: If the user already provided their account number and PIN recently in the chat history, REUSE THEM. Do not ask for them again.\n"
                "4. If a tool returns an error, tell the user the exact error.\n"
                "5. Answer directly and concisely."
            )
            
            # Initialize the short-term memory vault
            memory = MemorySaver()
            
            # Simple initialization (no modifier arguments!)
            agent_executor = create_react_agent(
                llm, 
                tools, 
                checkpointer=memory
            ) 

            print("\n✅ Banking Agent is Online and Ready!")
            print("Type 'exit' to quit.\n")

            # A configuration object to track this specific user's chat session
            config = {"configurable": {"thread_id": "user_session_1"}}

            # 6. The Chat Loop
            while True:
                user_input = input("You: ")
                if user_input.lower() in ['exit', 'quit']:
                    break

                # Send BOTH the system rules and the user message directly to the inputs
                inputs = {
                    "messages": [
                        ("system", system_prompt),
                        ("user", user_input)
                    ]
                }
                
                # Stream the agent's thought process using the session config
                async for chunk in agent_executor.astream(inputs, config=config, stream_mode="values"):
                    message = chunk["messages"][-1]
                    
                    if message.type == "ai":
                        if hasattr(message, 'tool_calls') and message.tool_calls:
                            print(f"   [Agent is using tool: {message.tool_calls[0]['name']}...]")
                        elif message.content:
                            print(f"🤖 Agent: {message.content}\n")
                    elif message.type == "tool":
                        print(f"   [Vault response received...]")
                        
if __name__ == "__main__":
    # Required for running async Python scripts
    asyncio.run(main())