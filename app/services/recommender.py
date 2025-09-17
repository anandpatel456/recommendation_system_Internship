from typing import List, Tuple
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity
from app.models.job import JobPosting
from app.models.user import UserProfile
from app.models.swipe import UserSwipe

class HybridRecommender:
    def __init__(self, embedder):
        self.embedder = embedder
    
    async def recommend(self, user: UserProfile, jobs: List[JobPosting], swipes: List[UserSwipe]) -> List[Tuple[JobPosting, float]]:
        """MAKE THIS METHOD ASYNC"""
        # 1. Content-based filtering
        user_embed = await self._embed_user(user)
        job_embeds = await self._embed_jobs(jobs)
        content_scores = cosine_similarity([user_embed], job_embeds)[0]
        
        # 2. Collaborative filtering
        swipe_scores = self._calculate_swipe_scores(user.clerk_id, swipes, jobs)
        
        # 3. Hybrid scoring with priority boost for posted jobs
        results = []
        for idx, job in enumerate(jobs):
            # Base content and collaborative scores
            content_score = content_scores[idx]
            swipe_score = swipe_scores.get(job.id, 0)
            
            # Priority boost for posted jobs (source priority)
            priority_boost = getattr(job, 'priority', 0.5)  # Default to 0.5 for scraped jobs
            
            # Calculate final score with priority boost
            base_score = (0.6 * content_score) + (0.4 * swipe_score)
            final_score = base_score + (priority_boost * 0.3)  # Add up to 30% boost for posted jobs
            
            results.append((job, final_score))
        
        # Sort by score descending (highest scores first)
        rr = sorted(results, key=lambda x: -x[1])
        return rr
    
    async def _embed_user(self, user: UserProfile) -> np.ndarray:
        """Async user embedding"""
        text = " ".join([
            " ".join(user.skills),
            " ".join(exp.title for exp in user.experience),
            user.location or ""
        ])
        return await self.embedder.embed(text)
    
    async def _embed_jobs(self, jobs: List[JobPosting]) -> List[np.ndarray]:
        """Async batch job embeddings"""
        texts = [
            f"{job.title} {' '.join(job.skills_required)} {job.location.city}"
            for job in jobs
        ]
        return await self.embedder.embed(texts)  # Assumes your embedder supports batch
    
    def _calculate_swipe_scores(self, clerk_id: str, swipes: List[UserSwipe], jobs: List[JobPosting]) -> dict:
        """Sync calculation (no I/O needed)"""
        weights = {
            "like": 1.0,
            "super_like": 1.5,
            "dislike": -1.0,
            "save": 0.8
        }
        return {job.id: sum(
            weights.get(s.action.value, 0)
            for s in swipes
            if s.user_id == clerk_id and s.job_id == job.id
        ) for job in jobs}