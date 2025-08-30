from typing import List, Optional
from bson import ObjectId
from pymongo import MongoClient
import os
from dotenv import load_dotenv
from motor.motor_asyncio import AsyncIOMotorClient


load_dotenv()
mongo_uri = os.getenv("MONGO_URI")

class Database:
    def __init__(self):
        self.client = AsyncIOMotorClient(mongo_uri)
        self.db = self.client.jobswipe_prod
    
    async def get_user_by_clerk_id(self, clerk_id: str) -> Optional[dict]:
        """Get user by Clerk ID (case-sensitive exact match)"""
        return await self.db.users.find_one({"clerk_id": clerk_id})
    
    async def get_active_jobs(self, limit: int = 1000) -> List[dict]:
        """Get all active job postings with required fields"""
        return await self.db.jobs.find(
            {"is_active": True},
            {
                "_id": 1,
                "employer_id": 1,
                "title": 1,
                "description": 1,
                "employment_type": 1,
                "salary": 1,
                "location": 1,
                "skills_required": 1,
                "requirements": 1,
                "responsibilities": 1,
                "benefits": 1
            }
        ).to_list(limit)
    
    async def get_user_swipes(self, clerk_id: str) -> List[dict]:
        """Get all swipes for a user (both likes and dislikes)"""
        return await self.db.swipes.find(
            {"user_id": clerk_id, "undone": False},
            {"job_id": 1, "action": 1}
        ).to_list(1000)
    
    async def get_job_by_id(self, job_id: str) -> Optional[dict]:
        """Get job details by ID"""
        if not ObjectId.is_valid(job_id):
            return None
        return await self.db.jobs.find_one({"_id": ObjectId(job_id)})

# Singleton instance
db = Database()