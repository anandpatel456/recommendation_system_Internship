from fastapi import APIRouter, Depends, HTTPException
from app.core.db import db
from app.services.recommender import HybridRecommender
from app.services.embeddings import EmbeddingService
from app.models.user import UserProfile
from app.models.job import JobPosting, JobRecommendation
from app.models.swipe import UserSwipe
from typing import List

from app.utils.converter import convert_mongo_doc

router = APIRouter()


def get_recommender():
    return HybridRecommender(EmbeddingService())


@router.get("/{clerk_id}")
async def get_recommendations(
    clerk_id: str,
    limit: int = 10,
    recommender: HybridRecommender = Depends(get_recommender),
):
    try:
        # 1. Fetch user data
        user = await db.get_user_by_clerk_id(clerk_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")

        # 2. Convert user document
        user_converted = convert_mongo_doc(user)
        user_model = UserProfile(**user_converted)

        # 3. Fetch active jobs with required fields
        jobs = await db.get_active_jobs()
        if not jobs:
            raise HTTPException(status_code=404, detail="No active jobs found")

        # 4. Convert job documents
        job_models = []
        for job in jobs:
            try:
                converted = convert_mongo_doc(job)
                job_models.append(JobPosting(**converted))
            except Exception as e:
                print(f"Skipping invalid job {job.get('_id')}: {str(e)}")
                continue

        # 5. Fetch user swipes
        swipes = await db.get_user_swipes(clerk_id)
        swipe_models = [UserSwipe(**convert_mongo_doc(swipe)) for swipe in swipes]

        # 6. Generate recommendations
        recommendations = await recommender.recommend(
            user_model, job_models, swipe_models
        )
        print(recommendations[0])
        # 7. Return top N recommendations
        return [
            JobRecommendation(job=job, match_score=score)
            for job, score in recommendations[:limit]
        ]
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Recommendation failed: {str(e)}")
