from typing import List, Optional
from bson import ObjectId
import os
import json
import redis.asyncio as redis
from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

load_dotenv()
redis_uri = os.getenv("REDIS_URI", "redis://default:liiSkjZQkhWPULcAcQ2dV0MZzy82wj2B@redis-13364.c56.east-us.azure.redns.redis-cloud.com:13364/0")
mongo_uri = os.getenv("MONGO_URI", "mongodb+srv://Backend-1:Backend-1@cluster0.q71be.mongodb.net/Tinder_Job?retryWrites=true&w=majority")

class Database:
    def __init__(self):
        self.redis_client = redis.from_url(redis_uri, decode_responses=True)
        # MongoDB for user profiles and user-posted jobs
        self.mongo_client = AsyncIOMotorClient(mongo_uri)
        self.mongo_db = self.mongo_client.Tinder_Job
        
        # Initialize web scraper (will be imported when needed)
        self.scraper = None
        
        print(f"🔗 Database connections initialized:")
        print(f"   - Redis: {redis_uri.split('@')[1] if '@' in redis_uri else 'localhost'}")
        print(f"   - MongoDB: {mongo_uri.split('@')[1].split('/')[0] if '@' in mongo_uri else 'localhost'}")
    
    def _initialize_scraper(self):
        """Initialize the web scraper only when needed"""
        if self.scraper is None:
            try:
                # Import your RedisCachedJobScraper class
                from job_scraper import RedisCachedJobScraper
                
                # Extract Redis connection details from URI
                redis_parts = redis_uri.split('@')
                if len(redis_parts) == 2:
                    # Has authentication  
                    auth_part = redis_parts[0].split('//')[1]
                    host_part = redis_parts[1]
                    password = auth_part.split(':')[1] if ':' in auth_part else None
                    host = host_part.split(':')[0]
                    port = int(host_part.split(':')[1].split('/')[0])
                else:
                    # No authentication
                    host = 'localhost'
                    port = 6379
                    password = None
                
                self.scraper = RedisCachedJobScraper(
                    redis_host=host,
                    redis_port=port, 
                    redis_password=password,
                    cache_duration_hours=72
                )
                logger.info("Web scraper initialized successfully")
                
            except Exception as e:
                logger.error(f"Failed to initialize web scraper: {e}")
                self.scraper = None
    
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
    
    async def get_active_jobs(self, limit: int = 1000, 
                            keywords: str = "software engineer", 
                            location: str = "United States",
                            job_type_filter: str = None, 
                            category_filter: str = None,
                            trusted_only: bool = True,
                            force_scrape: bool = False) -> List[dict]:
        """
        Get all active job postings with 3-step priority:
        1st: Posted jobs from MongoDB
        2nd: Scraped jobs from Redis cache  
        3rd: Fresh web scraping if not enough jobs found
        
        Args:
            limit: Maximum number of jobs to return
            keywords: Job search keywords for scraping
            location: Location for job search
            job_type_filter: Filter by job type
            category_filter: Filter by category
            trusted_only: Only trusted companies
            force_scrape: Skip cache and force fresh scraping
        """
        all_jobs = []
        
        print(f"🎯 Starting 3-step job search (limit: {limit})")
        print(f"   Keywords: {keywords}, Location: {location}")
        print(f"   Filters: job_type={job_type_filter}, category={category_filter}, trusted={trusted_only}")
        
        # ============= STEP 1: GET POSTED JOBS FROM MONGODB =============
        try:
            print(f"\n📋 STEP 1: Fetching posted jobs from MongoDB...")
            posted_jobs = await self.mongo_db.jobs.find(
                {"is_verified": True},
                {
                    "_id": 1, "employer_id_1": 1, "title": 1, "description_text": 1,
                    "job_type": 1, "location_1": 1, "skills": 1, "requirements": 1,
                    "category": 1, "source": 1, "created_at": 1, "posted_time": 1
                }
            ).to_list(100)  # Limit MongoDB jobs to 100
            
            print(f"✅ Found {len(posted_jobs)} posted jobs in MongoDB")
            
            for i, job in enumerate(posted_jobs):
                converted_job = self._convert_mongo_job(job)
                if converted_job:
                    converted_job["source"] = "posted"
                    converted_job["priority"] = 1.0  # Highest priority
                    all_jobs.append(converted_job)
                    
            print(f"✅ Successfully processed {len([j for j in all_jobs if j.get('source') == 'posted'])} MongoDB jobs")
            
        except Exception as e:
            print(f"❌ Error fetching posted jobs from MongoDB: {e}")
        
        # ============= STEP 2: GET CACHED JOBS FROM REDIS =============
        remaining_needed = limit - len(all_jobs)
        if remaining_needed > 0:
            try:
                print(f"\n🔄 STEP 2: Fetching cached jobs from Redis (need {remaining_needed} more)...")
                
                # Get scraped jobs from Redis cache
                job_keys = await self.redis_client.keys("job-scraping:*")
                scraped_count = 0
                
                print(f"📊 Found {len(job_keys)} cached job keys in Redis")
                
                for key in job_keys[:remaining_needed * 2]:  # Get more to allow filtering
                    job_data = await self.redis_client.hgetall(key)
                    if job_data:
                        # Filter based on search criteria
                        if self._matches_search_criteria(job_data, keywords, location, job_type_filter, category_filter, trusted_only):
                            converted_job = self._convert_scraped_job(job_data)
                            if converted_job:
                                converted_job["source"] = "scraped"
                                converted_job["priority"] = 0.7  # Medium priority
                                all_jobs.append(converted_job)
                                scraped_count += 1
                                
                                if scraped_count >= remaining_needed:
                                    break
                
                print(f"✅ Successfully processed {scraped_count} cached Redis jobs")
                
            except Exception as e:
                print(f"❌ Error fetching cached jobs from Redis: {e}")
        
        # ============= STEP 3: WEB SCRAPING IF NEEDED =============
        remaining_needed = limit - len(all_jobs)
        if remaining_needed > 0 and not force_scrape:
            try:
                print(f"\n🕷️  STEP 3: Web scraping needed (need {remaining_needed} more jobs)...")
                
                # Initialize scraper if needed
                self._initialize_scraper()
                
                if self.scraper:
                    print(f"🔍 Starting fresh web scraping...")
                    print(f"   Search params: keywords='{keywords}', location='{location}'")
                    
                    # Use the cached scraper which will handle Redis caching internally
                    scraped_jobs = self.scraper.get_jobs(
                        keywords=keywords,
                        location=location,
                        max_jobs=remaining_needed,
                        job_type_filter=job_type_filter,
                        category_filter=category_filter,
                        trusted_only=trusted_only,
                        force_refresh=force_scrape
                    )
                    
                    print(f"✅ Web scraping returned {len(scraped_jobs)} fresh jobs")
                    
                    # Convert and add scraped jobs
                    fresh_jobs_added = 0
                    for job in scraped_jobs:
                        # Convert from scraper format to our format
                        converted_job = self._convert_scraped_job_format(job)
                        if converted_job:
                            converted_job["source"] = "fresh_scraped"
                            converted_job["priority"] = 0.5  # Lower priority
                            all_jobs.append(converted_job)
                            fresh_jobs_added += 1
                    
                    print(f"✅ Added {fresh_jobs_added} freshly scraped jobs")
                    
                else:
                    print("❌ Web scraper not available")
                    
            except Exception as e:
                print(f"❌ Error during web scraping: {e}")
        
        # ============= FINAL RESULTS =============
        print(f"\n📊 === FINAL JOB SUMMARY ===")
        posted_count = len([j for j in all_jobs if j.get('source') == 'posted'])
        cached_count = len([j for j in all_jobs if j.get('source') == 'scraped'])
        fresh_count = len([j for j in all_jobs if j.get('source') == 'fresh_scraped'])
        
        print(f"Total jobs found: {len(all_jobs)}")
        print(f"  🏢 Posted jobs (MongoDB): {posted_count}")
        print(f"  💾 Cached jobs (Redis): {cached_count}")
        print(f"  🌐 Fresh scraped jobs: {fresh_count}")
        print(f"=============================")
        
        # Sort by priority and return limited results
        all_jobs.sort(key=lambda x: x.get('priority', 0), reverse=True)
        return all_jobs[:limit]
    
    def _matches_search_criteria(self, job_data: dict, keywords: str, location: str, 
                               job_type_filter: str, category_filter: str, trusted_only: bool) -> bool:
        """Check if a Redis job matches the search criteria"""
        try:
            # Keywords match (check title and description)
            if keywords:
                keywords_lower = keywords.lower()
                title_match = keywords_lower in job_data.get('title', '').lower()
                desc_match = keywords_lower in job_data.get('description', '').lower()
                if not (title_match or desc_match):
                    return False
            
            # Location match
            if location and location.lower() != "united states":
                location_lower = location.lower()
                job_location = job_data.get('location', '').lower()
                if location_lower not in job_location:
                    return False
            
            # Job type filter
            if job_type_filter:
                job_type = job_data.get('employment_type', job_data.get('job_type', '')).lower()
                if job_type_filter.lower() not in job_type:
                    return False
            
            # Category filter
            if category_filter and category_filter != 'All':
                job_category = job_data.get('category', '')
                if category_filter not in job_category:
                    return False
            
            # Trusted companies filter
            if trusted_only:
                is_trusted = job_data.get('is_trusted_company', False)
                if not is_trusted:
                    return False
            
            return True
            
        except Exception as e:
            logger.error(f"Error matching search criteria: {e}")
            return False
    
    def _convert_scraped_job_format(self, job_data: dict) -> dict:
        """Convert job data from scraper format to our expected format"""
        try:
            # Handle different possible field names from scraper
            return {
                "id": job_data.get('job_id', job_data.get('id', '')),
                "employer_id": job_data.get('company', job_data.get('employer_id', '')),
                "title": job_data.get('title', ''),
                "description": job_data.get('description', job_data.get('responsibilities', '')),
                "requirements": job_data.get('requirements', []) if isinstance(job_data.get('requirements'), list) else [job_data.get('requirements', '')],
                "responsibilities": job_data.get('responsibilities', []) if isinstance(job_data.get('responsibilities'), list) else [job_data.get('responsibilities', '')],
                "employment_type": job_data.get('employment_type', job_data.get('job_type', 'full_time')),
                "salary": job_data.get('salary', {"min": 0, "max": 0, "currency": "USD", "is_public": False}),
                "location": self._parse_location(job_data.get('location', '')),
                "skills_required": job_data.get('skills', job_data.get('skills_required', [])),
                "benefits": job_data.get('benefits', []),
                "is_active": True,
                "posted_at": job_data.get('posted_date', job_data.get('posted_at', datetime.now().strftime('%Y-%m-%d'))),
                "expires_at": job_data.get('expires_at', '2024-12-31'),
                # Additional fields
                "company": job_data.get('company', ''),
                "url": job_data.get('url', job_data.get('job_link', '')),
                "experience_level": job_data.get('experience_level', ''),
                "category": job_data.get('category', ''),
                "is_trusted_company": job_data.get('is_trusted_company', False),
                "remote_work": job_data.get('remote_work', job_data.get('remote', 'No'))
            }
        except Exception as e:
            logger.error(f"Error converting scraped job format: {e}")
            return None
    
    def _parse_location(self, location_str: str) -> dict:
        """Parse location string into structured format"""
        location_parts = location_str.split(',') if location_str else []
        return {
            "city": location_parts[0].strip() if location_parts else "",
            "state": location_parts[1].strip() if len(location_parts) > 1 else None,
            "country": location_parts[2].strip() if len(location_parts) > 2 else "USA",
            "remote": "remote" in location_str.lower() or "hybrid" in location_str.lower(),
            "coordinates": None
        }
    
    def _convert_mongo_job(self, job_data: dict) -> dict:
        """Convert MongoDB job data to our expected format"""
        try:
            # Parse skills from string to list
            skills = []
            if job_data.get('skills'):
                skills = [skill.strip() for skill in job_data['skills'].split(',') if skill.strip()]
            
            # Parse location
            location_str = job_data.get('location_1', '')
            location = self._parse_location(location_str)
            
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
                "responsibilities": [],
                "employment_type": employment_type,
                "salary": {"min": 0, "max": 0, "currency": "USD", "is_public": False},
                "location": location,
                "skills_required": skills,
                "benefits": [],
                "is_active": True,
                "posted_at": job_data.get("created_at", "2024-01-01"),
                "expires_at": "2024-12-31",
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
                salary_text = job_data['salary'].lower()
                if 'lakh' in salary_text or 'lpa' in salary_text:
                    salary["currency"] = "INR"
                salary["is_public"] = True
            
            # Parse location
            location = self._parse_location(job_data.get('location', ''))
            if not location["country"]:
                location["country"] = "India"  # Default for scraped jobs
            
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
                "benefits": [],
                "is_active": True,
                "posted_at": job_data.get('posted_date', '2024-01-01'),
                "expires_at": job_data.get('expires_at', '2024-12-31'),
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
    
    async def get_scraper_stats(self) -> dict:
        """Get web scraper statistics"""
        try:
            self._initialize_scraper()
            if self.scraper:
                return {
                    "cache_status": self.scraper.get_cache_status(),
                    "job_statistics": self.scraper.get_job_statistics(),
                    "redis_health": self.scraper.get_redis_health(),
                    "available_categories": self.scraper.get_job_categories(),
                    "trusted_companies_count": len(self.scraper.get_trusted_companies())
                }
            else:
                return {"error": "Scraper not available"}
        except Exception as e:
            return {"error": str(e)}
    
    async def clear_job_cache(self, expired_only: bool = True) -> dict:
        """Clear job cache"""
        try:
            self._initialize_scraper()
            if self.scraper:
                cleared_count = self.scraper.clear_cache(expired_only=expired_only)
                return {
                    "success": True,
                    "cleared_count": cleared_count,
                    "expired_only": expired_only
                }
            else:
                return {"success": False, "error": "Scraper not available"}
        except Exception as e:
            return {"success": False, "error": str(e)}

# Singleton instance
db = Database()