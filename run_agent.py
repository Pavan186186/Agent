import asyncio
from langchain_ollama import ChatOllama
from langgraph.prebuilt import create_react_agent
from langchain_core.tools import tool
from mcp.client.stdio import stdio_client, StdioServerParameters
from mcp.client.session import ClientSession

async def main():
    print("Booting up Agentic RAG Pipeline...")
    
    # 1. Point the MCP Client to our MCP Server
    server_params = StdioServerParameters(
        command="python",
        # Make sure this path points correctly to your mcp_server.py
        args=["Agent/core_bank_server/app/mcp_server.py"]
    )

    # 2. Establish the connection to the MCP Server
    async with stdio_client(server_params) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()

            # 3. Define the tools for Llama 3.1 that pass through to MCP
            # We use these wrappers so LangGraph knows exactly what inputs the tools require
            @tool
            async def query_bank_policy(query: str) -> str:
                """Searches the bank's official policy documents. Use for rules, fees, and limits."""
                result = await session.call_tool("query_bank_policy", arguments={"query": query})
                return result.content[0].text

            @tool
            async def get_account_balance(account_number: str) -> str:
                """Retrieves the current balance for a specific bank account number."""
                result = await session.call_tool("get_account_balance", arguments={"account_number": account_number})
                return result.content[0].text

            @tool
            async def transfer_funds(from_account: str, to_account: str, amount: float) -> str:
                """Executes a secure transfer of funds from one account to another."""
                result = await session.call_tool("transfer_funds", arguments={
                    "from_account": from_account, 
                    "to_account": to_account, 
                    "amount": amount
                })
                return result.content[0].text

            tools = [query_bank_policy, get_account_balance, transfer_funds]

            # 4. Initialize the Local Brain (Ollama)
            print("Waking up Llama 3.1...")
            # Temperature=0 makes the agent highly analytical and less prone to hallucinations
            llm = ChatOllama(model="llama3.1", temperature=0)

            # 5. Create the LangGraph Agent (Version-Proof!)
            system_prompt = (
                "You are an elite, highly secure AI banking assistant. "
                "You have access to tools for checking balances, executing transfers, and reading bank policies. "
                "Always use your tools before answering if you do not know the exact numbers. "
                "Be concise and professional."
            )
            
            # We remove the modifier parameter completely to avoid version conflicts
            agent_executor = create_react_agent(llm, tools) 

            print("\n✅ Banking Agent is Online and Ready!")
            print("Type 'exit' to quit.\n")

            # 6. The Chat Loop
            while True:
                user_input = input("You: ")
                if user_input.lower() in ['exit', 'quit']:
                    break

                # We inject the system prompt directly into the message array instead
                inputs = {
                    "messages": [
                        ("system", system_prompt),
                        ("user", user_input)
                    ]
                }
                
                # Stream the agent's thought process
                async for chunk in agent_executor.astream(inputs, stream_mode="values"):
                    message = chunk["messages"][-1]
                    
                    if message.type == "ai":
                        if message.tool_calls:
                            print(f"   [Agent is using tool: {message.tool_calls[0]['name']}...]")
                        elif message.content:
                            print(f"🤖 Agent: {message.content}\n")
                    elif message.type == "tool":
                        print(f"   [Vault response received...]")
                        
if __name__ == "__main__":
    # Required for running async Python scripts
    asyncio.run(main())