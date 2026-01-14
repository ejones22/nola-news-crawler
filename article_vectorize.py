"""
Vectorize local article .md files and save to ChromaDB
Modeled after NOCouncil-ETF
"""
import chromadb
from chromadb import PersistentClient
from chromadb.config import Settings
from chromadb.utils.embedding_functions.sentence_transformer_embedding_function \
    import SentenceTransformerEmbeddingFunction
from dotenv import load_dotenv
import glob
import os
from tqdm import tqdm

load_dotenv()

def make_vector_db(collection, md_files):
    """Vectorize markdown files from local directory"""
    print(f'Embedding {len(md_files)} articles...')
    
    for md_file in tqdm(md_files):
        # Read markdown file
        with open(md_file, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Split frontmatter and content
        parts = content.split('---')
        if len(parts) < 3:
            continue
            
        # Parse frontmatter
        frontmatter = {}
        for line in parts[1].strip().split('\n'):
            if ':' in line:
                key, val = line.split(':', 1)
                frontmatter[key.strip()] = val.strip()
        
        # Get article content
        article_text = parts[2].strip()
        
        # Create ID from filename
        article_id = os.path.basename(md_file).replace('.md', '').split('_')[1]
        
        # Add to collection
        collection.add(
            documents=[article_text],
            metadatas=[frontmatter],
            ids=[article_id]
        )
    
    print('...done')
    return collection


if __name__ == "__main__":
    load_dotenv()
    
    DB_DIR = os.getenv("CHROMA_DB_DIR", 'chroma_db')
    ARTICLES_DIR = os.getenv("ARTICLES_DIR", 'out')
    
    # Initialize ChromaDB
    chroma_client = PersistentClient(
        path=DB_DIR,
        settings=Settings(anonymized_telemetry=False)
    )
    
    embed_fn = SentenceTransformerEmbeddingFunction(
        model_name="all-MiniLM-L6-v2",
        device="cpu",
    )
    
    collection = chroma_client.get_or_create_collection(
        name="nola_articles",
        embedding_function=embed_fn,
        metadata={"hnsw:space": "cosine", "hnsw:num_threads": 1}
    )
    
    # Get all markdown files
    md_files = glob.glob(f"{ARTICLES_DIR}/*.md")
    print(f"Found {len(md_files)} markdown files in {ARTICLES_DIR}/")
    
    make_vector_db(collection, md_files)
