import chromadb
import os
from langchain_text_splitters import RecursiveCharacterTextSplitter

# 1. Robust Path Resolution
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CHROMA_PATH = os.path.join(SCRIPT_DIR, '../../local_db/chroma_db_storage')
POLICIES_DIR = os.path.join(SCRIPT_DIR, '../bank_policies')

os.makedirs(CHROMA_PATH, exist_ok=True)

def build_vector_database():
    print(f"Initializing local ChromaDB at {os.path.abspath(CHROMA_PATH)}...")
    
    client = chromadb.PersistentClient(path=CHROMA_PATH)
    
    # We use get_or_create to allow overwriting if you run this multiple times
    collection = client.get_or_create_collection(name="bank_knowledge_base")

    # 2. Configure the Text Splitter (The Chunking Engine)
    text_splitter = RecursiveCharacterTextSplitter(
        chunk_size=400,       # Max characters per chunk
        chunk_overlap=50,     # Overlap to maintain context between chunks
        length_function=len,
        is_separator_regex=False,
    )

    documents = []
    metadatas = []
    ids = []

    print("Reading and chunking policy documents...")
    if not os.path.exists(POLICIES_DIR):
        print(f"Error: Could not find the policies folder at {POLICIES_DIR}")
        return

    # 3. Process each file
    for filename in os.listdir(POLICIES_DIR):
        if filename.endswith(".txt"):
            filepath = os.path.join(POLICIES_DIR, filename)
            with open(filepath, 'r', encoding='utf-8') as file:
                content = file.read()
                
                # Split the large text into smaller chunks
                chunks = text_splitter.split_text(content)
                
                for i, chunk in enumerate(chunks):
                    documents.append(chunk)
                    # We store the source file AND the chunk index so we know exactly where it came from
                    metadatas.append({"source": filename, "chunk_index": i})
                    ids.append(f"{filename}_chunk_{i}")

    if not documents:
        print("No .txt files found in the bank_policies folder!")
        return

    # 4. Embed and Store
    print(f"Embedding and storing {len(documents)} chunks into ChromaDB...")
    collection.add(
        documents=documents,
        metadatas=metadatas,
        ids=ids
    )

    print("Success! The Vector Database is fully loaded with chunked data.")

    # 5. Quick Test Query
    print("\n--- Testing Retrieval Accuracy ---")
    query = "How much does an international wire transfer cost?"
    print(f"Query: '{query}'")
    
    results = collection.query(
        query_texts=[query],
        n_results=2 # Fetch the top 2 most relevant chunks
    )
    
    print("\nTop Match Found In:", results['metadatas'][0][0]['source'], f"(Chunk {results['metadatas'][0][0]['chunk_index']})")
    print("Content:", results['documents'][0][0])

if __name__ == "__main__":
    build_vector_database()