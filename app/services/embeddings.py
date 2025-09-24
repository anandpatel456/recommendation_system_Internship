# Alternative implementation without sentence_transformers dependency
import numpy as np
from typing import List, Union

class EmbeddingService:
    def __init__(self):
        # Simple fallback implementation
        print("⚠️  Using fallback embedding service (no ML model loaded)")
        print("   Install compatible sentence-transformers for full functionality")

    async def embed(self, text: Union[str, List[str]]) -> Union[np.ndarray, List[np.ndarray]]:
        """Async single text or batch embeddings - fallback implementation"""
        if isinstance(text, str):
            # Return a simple vector representation for testing
            # In production, this would use a real embedding model
            return np.array([0.1, 0.2, 0.3, 0.4, 0.5])
        else:
            # Return list of vectors for batch processing
            return [np.array([0.1, 0.2, 0.3, 0.4, 0.5]) for _ in text]
