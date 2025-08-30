from sentence_transformers import SentenceTransformer
import numpy as np
from typing import List

class EmbeddingService:
    def __init__(self):
        self.model = SentenceTransformer('all-MiniLM-L6-v2')
    
    async def embed(self, text: str) -> np.ndarray:
        """Async single text embedding"""
        return self.model.encode(text)
    
    async def embed(self, texts: List[str]) -> List[np.ndarray]:
        """Async batch embeddings"""
        return self.model.encode(texts)