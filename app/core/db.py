from typing import List, Optional
from bson import ObjectId
import os
import json
import redis.asyncio as redis
from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv


load_dotenv()
redis_uri = os.getenv("REDIS_URI", "redis://default:liiSkjZQkhWPULcAcQ2dV0MZzy82wj2B@redis-13364.c56.east-us.azure.redns.redis-cloud.com:13364/0")
mongo_uri = os.getenv("MONGO_URI", "mongodb+srv://Backend-1:Backend-1@cluster0.q71be.mongodb.net/Tinder_Job?retryWrites=true&w=majority")

class Database:
    def __init__(self):
        self.redis_client = redis.from_url(redis_uri, decode_responses=True)
        # MongoDB for user profiles and user-posted jobs
        self.mongo_client = AsyncIOMotorClient(mongo_uri)
        self.mongo_db = self.mongo_client.Tinder_Job
        print(f"🔗 Database connections initialized:")
        print(f"   - Redis: {redis_uri.split('@')[1] if '@' in redis_uri else 'localhost'}")
        print(f"   - MongoDB: {mongo_uri.split('@')[1].split('/')[0] if '@' in mongo_uri else 'localhost'}")
    
    async def get_user_by_clerk_id(self, clerk_id: str) -> Optional[dict]:
        """Get user by Clerk ID from MongoDB"""
        try:
            print(f"🔍 Searching for user with clerk_id: {clerk_id}")
            
            # First, let's check if the users collection exists and has any data
            user_count = await self.mongo_db.users.count_documents({})
            print(f"📊 Total users in MongoDB: {user_count}")
            
            if user_count == 0:
                print("⚠️  No users found in MongoDB users collection")
                return None
            
            # List all users to see what's available
            all_users = await self.mongo_db.users.find({}, {"clerk_id": 1, "email": 1, "first_name": 1}).to_list(10)
            print(f"👥 Available users:")
            for user in all_users:
                print(f"   - clerk_id: {user.get('clerk_id', 'N/A')}, email: {user.get('email', 'N/A')}, name: {user.get('first_name', 'N/A')}")
            
            user_data = await self.mongo_db.users.find_one({"clerk_id": clerk_id})
            if user_data:
                print(f"✅ User found: {user_data.get('first_name', 'N/A')} {user_data.get('last_name', 'N/A')}")
                # Convert ObjectId to string for JSON serialization
                user_data["_id"] = str(user_data["_id"])
                return user_data
            else:
                print(f"❌ User with clerk_id '{clerk_id}' not found in MongoDB")
                return None
        except Exception as e:
            print(f"💥 Error fetching user from MongoDB: {e}")
            return None
    
    async def create_user(self, user_data: dict) -> Optional[str]:
        """Create a new user in MongoDB"""
        try:
            result = await self.mongo_db.users.insert_one(user_data)
            return str(result.inserted_id)
        except Exception as e:
            print(f"Error creating user in MongoDB: {e}")
            return None
    
    async def update_user(self, clerk_id: str, update_data: dict) -> bool:
        """Update user data in MongoDB"""
        try:
            result = await self.mongo_db.users.update_one(
                {"clerk_id": clerk_id},
                {"$set": update_data}
            )
            return result.modified_count > 0
        except Exception as e:
            print(f"Error updating user in MongoDB: {e}")
            return False
    
    async def get_active_jobs(self, limit: int = 1000) -> List[dict]:
        """Get all active job postings with priority: posted jobs first, then scraped jobs"""
        all_jobs = []
        
        # 1. First priority: Get posted jobs from MongoDB
        try:
            posted_jobs = await self.mongo_db.jobs.find(
                {"is_verified": True},  # Use is_verified instead of is_active
                {
                    "_id": 1,
                    "employer_id_1": 1,
                    "title": 1,
                    "description_text": 1,
                    "job_type": 1,
                    "location_1": 1,
                    "skills": 1,
                    "requirements": 1,
                    "category": 1,
                    "source": 1,
                    "created_at": 1,
                    "posted_time": 1
                }
            ).to_list(100)  # Limit posted jobs to 100
            
            print(f"=== MONGODB JOBS FETCHED ===")
            print(f"Total jobs found in MongoDB: {len(posted_jobs)}")
            
            for i, job in enumerate(posted_jobs):
                print(f"\n--- Job {i+1} from MongoDB ---")
                print(f"ID: {job.get('_id')}")
                print(f"Title: {job.get('title', 'N/A')}")
                print(f"Company: {job.get('employer_id_1', 'N/A')}")
                print(f"Location: {job.get('location_1', 'N/A')}")
                print(f"Job Type: {job.get('job_type', 'N/A')}")
                print(f"Category: {job.get('category', 'N/A')}")
                print(f"Skills: {job.get('skills', 'N/A')}")
                print(f"Created: {job.get('created_at', 'N/A')}")
                print(f"Description: {job.get('description_text', 'N/A')[:100]}...")
                
                # Convert MongoDB job to our format
                converted_job = self._convert_mongo_job(job)
                if converted_job:
                    converted_job["source"] = "posted"  # Mark as posted job
                    converted_job["priority"] = 1.0  # Highest priority
                    all_jobs.append(converted_job)
                    print(f"✅ Successfully converted and added to recommendations")
                else:
                    print(f"❌ Failed to convert job")
            
            print(f"\n=== MONGODB SUMMARY ===")
            print(f"Successfully processed: {len([j for j in all_jobs if j.get('source') == 'posted'])} jobs")
        except Exception as e:
            print(f"Error fetching posted jobs from MongoDB: {e}")
        
        # 2. Second priority: Get scraped jobs from Redis
        try:
            job_keys = await self.redis_client.keys("job-scraping:*")
            scraped_count = 0
            
            for key in job_keys[:limit - len(all_jobs)]:  # Fill remaining slots
                job_data = await self.redis_client.hgetall(key)
                if job_data:
                    converted_job = self._convert_scraped_job(job_data)
                    if converted_job:
                        converted_job["source"] = "scraped"  # Mark as scraped job
                        converted_job["priority"] = 0.5  # Lower priority
                        all_jobs.append(converted_job)
                        scraped_count += 1
            
            print(f"Found {scraped_count} scraped jobs from Redis")
        except Exception as e:
            print(f"Error fetching scraped jobs from Redis: {e}")
        
        print(f"\n=== FINAL JOB SUMMARY ===")
        posted_count = len([j for j in all_jobs if j.get('source') == 'posted'])
        scraped_count = len([j for j in all_jobs if j.get('source') == 'scraped'])
        print(f"Total jobs available: {len(all_jobs)}")
        print(f"  - Posted jobs (MongoDB): {posted_count}")
        print(f"  - Scraped jobs (Redis): {scraped_count}")
        print(f"=====================================")
        
        return all_jobs
    
    def _convert_mongo_job(self, job_data: dict) -> dict:
        """Convert MongoDB job data to our expected format"""
        try:
            # Parse skills from string to list
            skills = []
            if job_data.get('skills'):
                skills = [skill.strip() for skill in job_data['skills'].split(',') if skill.strip()]
            
            # Parse location
            location_str = job_data.get('location_1', '')
            location_parts = location_str.split(',')
            location = {
                "city": location_parts[0].strip() if location_parts else "",
                "state": location_parts[1].strip() if len(location_parts) > 1 else None,
                "country": location_parts[2].strip() if len(location_parts) > 2 else "USA",
                "remote": False,  # Default to False, can be enhanced later
                "coordinates": None
            }
            
            # Convert job type
            job_type = job_data.get('job_type', 'Full-time').lower()
            if 'full' in job_type:
                employment_type = 'full_time'
            elif 'part' in job_type:
                employment_type = 'part_time'
            elif 'contract' in job_type:
                employment_type = 'contract'
            elif 'intern' in job_type:
                employment_type = 'internship'
            else:
                employment_type = 'full_time'
            
            return {
                "id": str(job_data.get("_id", "")),
                "employer_id": job_data.get("employer_id_1", ""),
                "title": job_data.get("title", ""),
                "description": job_data.get("description_text", ""),
                "requirements": [job_data.get("requirements", "")] if job_data.get("requirements") else [],
                "responsibilities": [],  # Not available in your schema
                "employment_type": employment_type,
                "salary": {"min": 0, "max": 0, "currency": "USD", "is_public": False},  # Not available in your schema
                "location": location,
                "skills_required": skills,
                "benefits": [],  # Not available in your schema
                "is_active": True,
                "posted_at": job_data.get("created_at", "2024-01-01"),
                "expires_at": "2024-12-31",  # Default expiry
                # Additional fields from your schema
                "category": job_data.get("category", ""),
                "source": job_data.get("source", "Manual"),
                "job_link": job_data.get("job_link", ""),
                "posted_time": job_data.get("posted_time", "")
            }
        except Exception as e:
            print(f"Error converting MongoDB job: {e}")
            return None
    
    def _convert_scraped_job(self, job_data: dict) -> dict:
        """Convert scraped job data to our expected format"""
        try:
            # Parse skills and requirements from strings to lists
            skills = []
            if job_data.get('skills'):
                try:
                    skills = json.loads(job_data['skills']) if isinstance(job_data['skills'], str) else job_data['skills']
                except:
                    skills = job_data['skills'].split(',') if isinstance(job_data['skills'], str) else []
            
            requirements = []
            if job_data.get('requirements'):
                try:
                    requirements = json.loads(job_data['requirements']) if isinstance(job_data['requirements'], str) else job_data['requirements']
                except:
                    requirements = [job_data['requirements']] if job_data['requirements'] else []
            
            responsibilities = []
            if job_data.get('responsibilities'):
                try:
                    responsibilities = json.loads(job_data['responsibilities']) if isinstance(job_data['responsibilities'], str) else job_data['responsibilities']
                except:
                    responsibilities = [job_data['responsibilities']] if job_data['responsibilities'] else []
            
            # Parse salary if available
            salary = {"min": 0, "max": 0, "currency": "USD", "is_public": False}
            if job_data.get('salary') and job_data['salary'].strip():
                # Try to extract salary range from text
                salary_text = job_data['salary'].lower()
                if 'lakh' in salary_text or 'lpa' in salary_text:
                    # Indian salary format
                    salary["currency"] = "INR"
                # For now, set as not public since we can't parse the exact range
                salary["is_public"] = True
            
            # Parse location
            location_parts = job_data.get('location', '').split(',')
            location = {
                "city": location_parts[0].strip() if location_parts else "",
                "state": location_parts[1].strip() if len(location_parts) > 1 else None,
                "country": location_parts[2].strip() if len(location_parts) > 2 else "India",
                "remote": job_data.get('remote', 'No').lower() in ['yes', 'true', 'remote', 'hybrid'],
                "coordinates": None
            }
            
            # Convert employment type
            employment_type = job_data.get('employment_type', 'full_time').lower()
            if employment_type not in ['full_time', 'part_time', 'contract', 'internship']:
                employment_type = 'full_time'
            
            return {
                "id": job_data.get('job_id', ''),
                "employer_id": job_data.get('company', ''),
                "title": job_data.get('title', ''),
                "description": job_data.get('responsibilities', ''),
                "requirements": requirements,
                "responsibilities": responsibilities,
                "employment_type": employment_type,
                "salary": salary,
                "location": location,
                "skills_required": skills,
                "benefits": [],  # Not available in scraped data
                "is_active": True,
                "posted_at": job_data.get('posted_date', '2024-01-01'),
                "expires_at": job_data.get('expires_at', '2024-12-31'),
                # Additional fields from scraping
                "company": job_data.get('company', ''),
                "url": job_data.get('url', ''),
                "experience_level": job_data.get('experience_level', ''),
                "category": job_data.get('category', ''),
                "is_trusted_company": job_data.get('is_trusted_company', False)
            }
        except Exception as e:
            print(f"Error converting job data: {e}")
            return None
    
    async def get_user_swipes(self, clerk_id: str) -> List[dict]:
        """Get all swipes for a user from Redis"""
        swipe_keys = await self.redis_client.keys(f"swipe:{clerk_id}:*")
        swipes = []
        
        for key in swipe_keys:
            swipe_data = await self.redis_client.get(key)
            if swipe_data:
                swipe = json.loads(swipe_data)
                if not swipe.get("undone", False):
                    swipes.append(swipe)
        
        return swipes
    
    async def get_job_by_id(self, job_id: str) -> Optional[dict]:
        """Get job details by ID from Redis"""
        job_data = await self.redis_client.get(f"job:{job_id}")
        if job_data:
            return json.loads(job_data)
            return None

# Singleton instance
db = Database()