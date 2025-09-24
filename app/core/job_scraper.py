"""
job_scraper.py - Complete Job Scraper with Redis Caching
This file integrates your actual LinkedInJobScraper and RedisJobDataCache
"""

import requests
import json
import time
import re
from bs4 import BeautifulSoup
from urllib.parse import urlencode, quote
from typing import Dict, List, Optional, Set
import logging
import redis
import hashlib
from datetime import datetime, timedelta

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class LinkedInJobScraper:
    def __init__(self):
        self.base_url = "https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        }
        self.session = requests.Session()
        self.session.headers.update(self.headers)
        
        # Trusted companies list - Fortune 500 + Major Tech Companies
        self.trusted_companies = {
            # Major Tech Companies
            'google', 'microsoft', 'amazon', 'apple', 'meta', 'facebook', 'tesla', 'netflix',
            'salesforce', 'oracle', 'adobe', 'nvidia', 'intel', 'ibm', 'cisco', 'vmware',
            'spotify', 'uber', 'airbnb', 'twitter', 'linkedin', 'dropbox', 'slack', 'zoom',
            'shopify', 'square', 'stripe', 'paypal', 'ebay', 'reddit', 'pinterest', 'snap',
            'twilio', 'okta', 'snowflake', 'databricks', 'palantir', 'cloudflare', 'mongodb',
            
            # Financial Services
            'jpmorgan', 'goldman sachs', 'morgan stanley', 'bank of america', 'wells fargo',
            'citigroup', 'american express', 'visa', 'mastercard', 'blackrock', 'fidelity',
            'charles schwab', 'robinhood', 'coinbase', 'stripe', 'square',
            
            # Consulting & Professional Services
            'mckinsey', 'bain', 'bcg', 'deloitte', 'pwc', 'kpmg', 'ey', 'accenture',
            'ibm consulting', 'tcs', 'infosys', 'wipro', 'cognizant', 'capgemini',
            
            # Fortune 500 Companies
            'walmart', 'exxon mobil', 'berkshire hathaway', 'unitedhealth', 'mckesson',
            'cvs health', 'amazon', 'at&t', 'general motors', 'ford', 'verizon',
            'chevron', 'kroger', 'general electric', 'walgreens', 'phillips 66',
            'marathon petroleum', 'costco', 'cardinal health', 'express scripts',
            
            # Healthcare & Pharma
            'johnson & johnson', 'pfizer', 'merck', 'abbott', 'bristol myers squibb',
            'eli lilly', 'gilead', 'amgen', 'biogen', 'regeneron', 'moderna',
            'kaiser permanente', 'anthem', 'humana', 'centene',
            
            # Startups & Unicorns
            'openai', 'anthropic', 'canva', 'figma', 'notion', 'discord', 'github',
            'gitlab', 'atlassian', 'asana', 'monday.com', 'miro', 'airtable'
        }
        
        # Job categories mapping with related keywords
        self.job_categories = {
            'Software Engineering': [
                'software engineer', 'software developer', 'full stack developer', 
                'frontend developer', 'backend developer', 'web developer',
                'mobile developer', 'ios developer', 'android developer',
                'python developer', 'java developer', 'javascript developer',
                'react developer', 'node.js developer', '.net developer'
            ],
            'Data Science & Analytics': [
                'data scientist', 'data analyst', 'data engineer', 'ml engineer',
                'machine learning engineer', 'ai engineer', 'research scientist',
                'business analyst', 'business intelligence', 'data visualization',
                'statistician', 'quantitative analyst', 'analytics engineer'
            ],
            'DevOps & Infrastructure': [
                'devops engineer', 'cloud engineer', 'infrastructure engineer',
                'site reliability engineer', 'platform engineer', 'systems engineer',
                'network engineer', 'security engineer', 'aws engineer',
                'kubernetes engineer', 'docker', 'terraform'
            ],
            'Product & Design': [
                'product manager', 'product owner', 'ux designer', 'ui designer',
                'product designer', 'user experience', 'user interface',
                'design lead', 'creative director', 'graphic designer'
            ],
            'Cybersecurity': [
                'security engineer', 'cybersecurity analyst', 'security architect',
                'penetration tester', 'security consultant', 'incident response',
                'vulnerability assessment', 'compliance analyst'
            ],
            'Project Management': [
                'project manager', 'program manager', 'scrum master',
                'agile coach', 'delivery manager', 'technical program manager',
                'pmp', 'project coordinator'
            ],
            'Sales & Marketing': [
                'sales representative', 'account manager', 'business development',
                'marketing manager', 'digital marketing', 'growth marketing',
                'content marketing', 'social media manager', 'seo specialist'
            ],
            'Finance & Accounting': [
                'financial analyst', 'accountant', 'controller', 'cfo',
                'investment analyst', 'risk analyst', 'auditor',
                'financial planner', 'treasury analyst'
            ],
            'Human Resources': [
                'hr manager', 'recruiter', 'talent acquisition', 'hr business partner',
                'compensation analyst', 'learning and development', 'hr generalist'
            ],
            'Operations': [
                'operations manager', 'supply chain', 'logistics coordinator',
                'business operations', 'process improvement', 'quality assurance'
            ]
        }
    
    def is_trusted_company(self, company_name: str) -> bool:
        """Check if a company is in the trusted companies list"""
        if not company_name:
            return False
        
        company_lower = company_name.lower().strip()
        
        # Direct match
        if company_lower in self.trusted_companies:
            return True
        
        # Partial match for companies with variations
        for trusted in self.trusted_companies:
            if trusted in company_lower or company_lower in trusted:
                return True
        
        return False
    
    def get_job_category(self, title: str, description: str = "") -> str:
        """Determine job category based on title and description"""
        title_lower = title.lower()
        desc_lower = description.lower()
        combined_text = f"{title_lower} {desc_lower}"
        
        category_scores = {}
        
        for category, keywords in self.job_categories.items():
            score = 0
            for keyword in keywords:
                # Higher weight for title matches
                if keyword in title_lower:
                    score += 3
                # Lower weight for description matches
                elif keyword in desc_lower:
                    score += 1
            
            if score > 0:
                category_scores[category] = score
        
        if category_scores:
            return max(category_scores, key=category_scores.get)
        
        return 'Other'
    
    def build_search_params(self, keywords: str = "software engineer", location: str = "United States", 
                          start: int = 0, count: int = 25, job_type_filter: str = None) -> Dict:
        """Build search parameters for LinkedIn API"""
        params = {
            'keywords': keywords,
            'location': location,
            'start': start,
            'count': count,
            'f_TPR': '',  # Time posted (empty for all)
        }
        
        # Job type filter mapping
        job_type_mapping = {
            'full-time': 'F',
            'part-time': 'P', 
            'contract': 'C',
            'temporary': 'T',
            'internship': 'I',
            'volunteer': 'V'
        }
        
        if job_type_filter and job_type_filter.lower() in job_type_mapping:
            params['f_JT'] = job_type_mapping[job_type_filter.lower()]
        else:
            params['f_JT'] = ''
        
        return params
    
    def extract_job_details(self, job_html: str) -> Dict:
        """Extract job details from HTML content"""
        soup = BeautifulSoup(job_html, 'html.parser')
        
        job_data = {
            'title': '',
            'company': '',
            'location': '',
            'description': '',
            'requirements': [],
            'job_type': '',
            'skills': [],
            'posted_date': '',
            'job_url': '',
            'salary': '',
            'category': '',
            'is_trusted_company': False,
            'experience_level': '',
            'employment_type': ''
        }
        
        try:
            # Extract job title
            title_elem = soup.find('h3', class_='base-search-card__title')
            if not title_elem:
                title_elem = soup.find('a', class_='base-card__full-link')
            if title_elem:
                job_data['title'] = title_elem.get_text().strip()
            
            # Extract company name
            company_elem = soup.find('h4', class_='base-search-card__subtitle')
            if not company_elem:
                company_elem = soup.find('a', {'data-tracking-control-name': 'public_jobs_jserp-result_job-search-card-subtitle'})
            if company_elem:
                company_link = company_elem.find('a')
                if company_link:
                    job_data['company'] = company_link.get_text().strip()
                else:
                    job_data['company'] = company_elem.get_text().strip()
            
            # Check if company is trusted
            job_data['is_trusted_company'] = self.is_trusted_company(job_data['company'])
            
            # Extract location
            location_elem = soup.find('span', class_='job-search-card__location')
            if location_elem:
                job_data['location'] = location_elem.get_text().strip()
            
            # Extract job URL
            job_link = soup.find('a', {'data-tracking-control-name': 'public_jobs_jserp-result_search-card'})
            if not job_link:
                job_link = soup.find('a', class_='base-card__full-link')
            if job_link and job_link.get('href'):
                job_data['job_url'] = job_link['href']
            
            # Extract posted date
            time_elem = soup.find('time', class_='job-search-card__listdate')
            if not time_elem:
                time_elem = soup.find('time')
            if time_elem:
                job_data['posted_date'] = time_elem.get('datetime', time_elem.get_text().strip())
            
            # Detect job type and experience level from title
            job_data['job_type'] = self.detect_job_type(job_data['title'])
            job_data['experience_level'] = self.detect_experience_level(job_data['title'])
            
            # Extract additional metadata
            metadata_elem = soup.find('div', class_='base-search-card__metadata')
            if metadata_elem:
                metadata_text = metadata_elem.get_text().strip()
                job_data['employment_type'] = self.parse_employment_type(metadata_text)
            
        except Exception as e:
            logger.error(f"Error extracting job details: {str(e)}")
        
        return job_data
    
    def detect_job_type(self, title: str) -> str:
        """Detect job type from job title"""
        title_lower = title.lower()
        
        if any(keyword in title_lower for keyword in ['intern', 'internship']):
            return 'Internship'
        elif any(keyword in title_lower for keyword in ['contract', 'contractor', 'freelance', 'temporary']):
            return 'Contract'
        elif any(keyword in title_lower for keyword in ['part-time', 'part time']):
            return 'Part-time'
        else:
            return 'Full-time'
    
    def detect_experience_level(self, title: str) -> str:
        """Detect experience level from job title"""
        title_lower = title.lower()
        
        if any(keyword in title_lower for keyword in ['senior', 'sr.', 'lead', 'principal', 'staff']):
            return 'Senior'
        elif any(keyword in title_lower for keyword in ['junior', 'jr.', 'entry', 'associate', 'intern']):
            return 'Entry Level'
        elif any(keyword in title_lower for keyword in ['mid', 'intermediate']):
            return 'Mid Level'
        else:
            return 'Mid Level'  # Default
    
    def parse_employment_type(self, metadata_text: str) -> str:
        """Parse employment type from metadata"""
        metadata_lower = metadata_text.lower()
        
        if 'full-time' in metadata_lower:
            return 'Full-time'
        elif 'part-time' in metadata_lower:
            return 'Part-time'
        elif 'contract' in metadata_lower:
            return 'Contract'
        elif 'internship' in metadata_lower:
            return 'Internship'
        else:
            return 'Full-time'  # Default
    
    def get_job_description(self, job_url: str) -> Dict:
        """Fetch detailed job description from job URL"""
        try:
            if not job_url.startswith('http'):
                job_url = f"https://www.linkedin.com{job_url}"
            
            response = self.session.get(job_url, timeout=15)
            if response.status_code == 200:
                soup = BeautifulSoup(response.content, 'html.parser')
                
                # Extract job description
                description_elem = soup.find('div', class_='show-more-less-html__markup')
                if not description_elem:
                    description_elem = soup.find('div', class_='description__text')
                
                description = ''
                if description_elem:
                    description = description_elem.get_text().strip()
                
                # Extract requirements and skills
                requirements, skills = self.parse_description_for_requirements(description)
                
                # Extract salary if available
                salary = self.extract_salary_info(soup)
                
                return {
                    'description': description,
                    'requirements': requirements,
                    'skills': skills,
                    'salary': salary
                }
        except Exception as e:
            logger.error(f"Error fetching job description from {job_url}: {str(e)}")
        
        return {'description': '', 'requirements': [], 'skills': [], 'salary': ''}
    
    def extract_salary_info(self, soup) -> str:
        """Extract salary information from job page"""
        try:
            # Look for salary information in various possible locations
            salary_selectors = [
                '.salary',
                '.compensation-text',
                '[data-automation-id="salary"]',
                '.jobs-unified-top-card__job-insight'
            ]
            
            for selector in salary_selectors:
                salary_elem = soup.select_one(selector)
                if salary_elem:
                    salary_text = salary_elem.get_text().strip()
                    if any(char.isdigit() for char in salary_text) and ('$' in salary_text or 'USD' in salary_text):
                        return salary_text
            
        except Exception as e:
            logger.error(f"Error extracting salary: {str(e)}")
        
        return ''
    
    def parse_description_for_requirements(self, description: str) -> tuple:
        """Parse job description to extract requirements and skills"""
        requirements = []
        skills = set()
        
        # Expanded skill keywords by category
        skill_categories = {
            'Programming Languages': [
                'python', 'java', 'javascript', 'typescript', 'c++', 'c#', '.net',
                'php', 'ruby', 'go', 'rust', 'swift', 'kotlin', 'scala', 'r',
                'matlab', 'perl', 'objective-c', 'dart', 'elixir'
            ],
            'Web Technologies': [
                'react', 'angular', 'vue.js', 'node.js', 'express', 'django',
                'flask', 'spring', 'laravel', 'rails', 'asp.net', 'html',
                'css', 'sass', 'less', 'webpack', 'babel', 'jquery'
            ],
            'Databases': [
                'sql', 'mysql', 'postgresql', 'mongodb', 'redis', 'elasticsearch',
                'oracle', 'sql server', 'sqlite', 'cassandra', 'dynamodb',
                'neo4j', 'influxdb', 'mariadb'
            ],
            'Cloud & DevOps': [
                'aws', 'azure', 'gcp', 'docker', 'kubernetes', 'terraform',
                'ansible', 'jenkins', 'git', 'github', 'gitlab', 'bitbucket',
                'ci/cd', 'linux', 'ubuntu', 'centos', 'nginx', 'apache'
            ],
            'Data & Analytics': [
                'machine learning', 'deep learning', 'artificial intelligence',
                'ai', 'data science', 'pandas', 'numpy', 'scikit-learn',
                'tensorflow', 'pytorch', 'keras', 'tableau', 'power bi',
                'spark', 'hadoop', 'kafka', 'airflow'
            ],
            'Mobile': [
                'ios', 'android', 'react native', 'flutter', 'xamarin',
                'cordova', 'ionic', 'swift', 'objective-c', 'kotlin', 'java'
            ]
        }
        
        # Flatten all skills
        all_skills = []
        for category_skills in skill_categories.values():
            all_skills.extend(category_skills)
        
        description_lower = description.lower()
        
        # Extract skills (case-insensitive matching)
        for skill in all_skills:
            if skill.lower() in description_lower:
                skills.add(skill.title())
        
        # Extract requirements using more sophisticated patterns
        requirement_patterns = [
            r'(?:required|must have|essential|mandatory)[:\s]([^.!?]{10,100})',
            r'(?:minimum|at least)[\s]+(\d+[\s]*(?:years?|yrs?)[\s]*(?:of)?[\s]*experience)',
            r'(?:bachelor|master|phd|degree)[^.!?]{0,50}',
            r'(?:experience with|proficiency in|knowledge of)[^.!?]{10,80}',
            r'(?:strong|excellent|solid)[\s]+(?:knowledge|understanding|experience)[^.!?]{10,80}'
        ]
        
        for pattern in requirement_patterns:
            matches = re.finditer(pattern, description, re.IGNORECASE | re.DOTALL)
            for match in matches:
                requirement = match.group(0).strip()
                if len(requirement) > 15 and len(requirement) < 200:  # Filter reasonable length
                    requirements.append(requirement)
        
        # Also extract bullet points and numbered lists
        sentences = re.split(r'[.!?•·‣▪▫-]\s*', description)
        for sentence in sentences:
            sentence = sentence.strip()
            sentence_lower = sentence.lower()
            
            # Look for requirement indicators
            if any(indicator in sentence_lower for indicator in [
                'required', 'must have', 'essential', 'mandatory', 'minimum',
                'years of experience', 'degree', 'certification', 'preferred'
            ]):
                if 20 <= len(sentence) <= 150:  # Reasonable length
                    requirements.append(sentence)
        
        # Remove duplicates and limit results
        requirements = list(dict.fromkeys(requirements))[:8]  # Keep unique, limit to 8
        skills = list(skills)[:12]  # Limit to 12 skills
        
        return requirements, skills
    
    def filter_by_category(self, jobs: List[Dict], category: str) -> List[Dict]:
        """Filter jobs by category"""
        if category == 'All' or not category:
            return jobs
        
        filtered_jobs = []
        for job in jobs:
            # Determine category if not already set
            if not job.get('category'):
                job['category'] = self.get_job_category(job['title'], job.get('description', ''))
            
            if job['category'] == category:
                filtered_jobs.append(job)
        
        return filtered_jobs
    
    def make_request_with_backoff(self, url: str, params: Dict = None, max_retries: int = 3,
                                base_timeout: int = 30) -> requests.Response:
        """Make HTTP request with exponential backoff and retry logic"""
        for attempt in range(max_retries):
            try:
                response = self.session.get(url, params=params, timeout=base_timeout)

                # Handle rate limiting (429 Too Many Requests)
                if response.status_code == 429:
                    wait_time = 2 ** attempt  # Exponential backoff
                    logger.warning(f"Rate limited (429). Waiting {wait_time}s before retry {attempt + 1}/{max_retries}")
                    time.sleep(wait_time)
                    continue

                # Handle other client/server errors
                if response.status_code >= 400:
                    if attempt == max_retries - 1:
                        logger.error(f"Request failed with status {response.status_code} after {max_retries} attempts")
                        response.raise_for_status()
                    else:
                        wait_time = 2 ** attempt
                        logger.warning(f"Request failed with status {response.status_code}. Retrying in {wait_time}s (attempt {attempt + 1}/{max_retries})")
                        time.sleep(wait_time)
                        continue

                return response

            except requests.exceptions.Timeout as e:
                if attempt == max_retries - 1:
                    logger.error(f"Request timed out after {max_retries} attempts: {str(e)}")
                    raise
                wait_time = 2 ** attempt
                logger.warning(f"Request timed out. Retrying in {wait_time}s (attempt {attempt + 1}/{max_retries})")
                time.sleep(wait_time)

            except requests.exceptions.ConnectionError as e:
                if attempt == max_retries - 1:
                    logger.error(f"Connection error after {max_retries} attempts: {str(e)}")
                    raise
                wait_time = 2 ** attempt
                logger.warning(f"Connection error. Retrying in {wait_time}s (attempt {attempt + 1}/{max_retries})")
                time.sleep(wait_time)

            except Exception as e:
                if attempt == max_retries - 1:
                    logger.error(f"Request failed after {max_retries} attempts: {str(e)}")
                    raise
                wait_time = 2 ** attempt
                logger.warning(f"Request failed: {str(e)}. Retrying in {wait_time}s (attempt {attempt + 1}/{max_retries})")
                time.sleep(wait_time)

        # This should never be reached, but just in case
        raise Exception(f"Request failed after {max_retries} attempts")

    def scrape_jobs(self, keywords: str = "software engineer", location: str = "United States",
                   max_jobs: int = 50, job_type_filter: str = None, category_filter: str = None,
                   trusted_only: bool = True) -> List[Dict]:
        """Main method to scrape LinkedIn jobs with advanced filtering and retry logic"""
        all_jobs = []
        start = 0
        count = 25

        logger.info(f"Starting job scraping: keywords='{keywords}', location='{location}', "
                   f"max_jobs={max_jobs}, trusted_only={trusted_only}")

        while len(all_jobs) < max_jobs:
            try:
                params = self.build_search_params(keywords, location, start, count, job_type_filter)

                logger.info(f"Fetching jobs: start={start}, count={count}")

                # Use enhanced request method with retry and backoff
                response = self.make_request_with_backoff(self.base_url, params=params, max_retries=3, base_timeout=30)

                soup = BeautifulSoup(response.content, 'html.parser')
                job_cards = soup.find_all('div', class_=['base-card', 'job-search-card'])

                if not job_cards:
                    logger.info("No more job cards found")
                    break

                jobs_added_this_batch = 0

                for card in job_cards:
                    if len(all_jobs) >= max_jobs:
                        break

                    job_data = self.extract_job_details(str(card))

                    # Skip if company is not trusted (when trusted_only is True)
                    if trusted_only and not job_data['is_trusted_company']:
                        continue

                    # Get detailed description if URL is available
                    if job_data['job_url']:
                        detailed_info = self.get_job_description(job_data['job_url'])
                        job_data.update(detailed_info)

                    # Determine job category
                    job_data['category'] = self.get_job_category(
                        job_data['title'],
                        job_data.get('description', '')
                    )

                    all_jobs.append(job_data)
                    jobs_added_this_batch += 1

                    # Add delay to be respectful to LinkedIn's servers
                    time.sleep(0.5)

                # If no jobs were added in this batch, break to avoid infinite loop
                if jobs_added_this_batch == 0:
                    logger.info("No qualifying jobs found in this batch")
                    break

                start += count
                time.sleep(2)  # Delay between pages

            except Exception as e:
                logger.error(f"Error during scraping: {str(e)}")
                break

        # Apply category filter if specified
        if category_filter and category_filter != 'All':
            all_jobs = self.filter_by_category(all_jobs, category_filter)

        logger.info(f"Successfully scraped {len(all_jobs)} jobs")
        return all_jobs
    
    def get_available_categories(self) -> List[str]:
        """Get list of available job categories"""
        return ['All'] + list(self.job_categories.keys())
    
    def get_trusted_companies_list(self) -> List[str]:
        """Get list of trusted companies"""
        return sorted(list(self.trusted_companies))


class RedisJobDataCache:
    def __init__(self, redis_host: str = 'localhost', redis_port: int = 6379, 
                 redis_db: int = 0, redis_password: str = None, 
                 cache_duration_hours: int = 72):
        """Initialize Redis cache manager"""
        self.cache_duration_seconds = cache_duration_hours * 3600
        self.hash_name = "job-scraping"
        
        # Test connection with retry
        max_retries = 3

        try:
            # Enhanced Redis configuration with connection pooling and retry logic
            self.redis_client = redis.Redis(
                host=redis_host,
                port=redis_port,
                db=redis_db,
                password=redis_password,
                decode_responses=True,
                socket_timeout=30,              # Increased from 5s to 30s
                socket_connect_timeout=10,      # Increased from 5s to 10s
                retry_on_timeout=True,          # Enable retries on timeout
                health_check_interval=30,       # Health check every 30 seconds
                max_connections=20,             # Connection pool size
                socket_keepalive=True           # Keep connections alive
            )

            for attempt in range(max_retries):
                try:
                    self.redis_client.ping()
                    logger.info(f"Connected to Redis at {redis_host}:{redis_port} (attempt {attempt + 1})")
                    break
                except Exception as e:
                    if attempt == max_retries - 1:
                        raise
                    logger.warning(f"Redis connection attempt {attempt + 1} failed: {str(e)}. Retrying...")
                    time.sleep(2 ** attempt)  # Exponential backoff

        except Exception as e:
            logger.error(f"Failed to connect to Redis after {max_retries} attempts: {str(e)}")
            raise ConnectionError(f"Could not connect to Redis: {str(e)}")
    
    def generate_cache_key(self, keywords: str, location: str, max_jobs: int = 50, 
                         job_type_filter: str = None, category_filter: str = None, 
                         trusted_only: bool = True) -> str:
        """Generate unique cache key based on search parameters"""
        search_params = f"{keywords}_{location}_{max_jobs}_{job_type_filter}_{category_filter}_{trusted_only}"
        return hashlib.md5(search_params.encode()).hexdigest()
    
    def save_job_to_redis(self, job_data: Dict) -> bool:
        """Save individual job to Redis hash with comprehensive fields"""
        try:
            # Generate unique job ID if not present
            if 'job_id' not in job_data:
                job_id_source = f"{job_data.get('title', '')}{job_data.get('company', '')}{job_data.get('location', '')}"
                job_data['job_id'] = hashlib.md5(job_id_source.encode()).hexdigest()[:12]
            
            job_id = job_data['job_id']
            
            # Prepare comprehensive job fields for Redis Hash
            redis_fields = {
                'title': job_data.get('title', ''),
                'company': job_data.get('company', ''),
                'skills': json.dumps(job_data.get('skills', [])),
                'salary': job_data.get('salary', ''),
                'location': job_data.get('location', ''),
                'job_type': job_data.get('job_type', ''),
                'experience_level': job_data.get('experience_level', ''),
                'category': job_data.get('category', ''),
                'posted_date': job_data.get('posted_date', ''),
                'url': job_data.get('job_url', ''),
                'job_id': job_id,
                'requirements': json.dumps(job_data.get('requirements', [])),
                'responsibilities': job_data.get('description', ''),
                'employment_type': job_data.get('employment_type', ''),
                'remote': self._determine_remote_status(job_data),
                'is_trusted_company': str(job_data.get('is_trusted_company', False)),
                'created_at': datetime.now().isoformat(),
                'expires_at': (datetime.now() + timedelta(seconds=self.cache_duration_seconds)).isoformat()
            }
            
            # Save to Redis Hash
            self.redis_client.hset(f"{self.hash_name}:{job_id}", mapping=redis_fields)
            
            # Set expiration for the individual job hash
            self.redis_client.expire(f"{self.hash_name}:{job_id}", self.cache_duration_seconds)
            
            return True
            
        except Exception as e:
            logger.error(f"Error saving job to Redis: {str(e)}")
            return False
    
    def _determine_remote_status(self, job_data: Dict) -> str:
        """Determine remote work status from job data"""
        location = job_data.get('location', '').lower()
        description = job_data.get('description', '').lower()
        title = job_data.get('title', '').lower()
        
        remote_indicators = ['remote', 'work from home', 'wfh', 'telecommute']
        hybrid_indicators = ['hybrid', 'flexible', 'part remote']
        
        combined_text = f"{location} {description} {title}"
        
        if any(indicator in combined_text for indicator in hybrid_indicators):
            return 'Hybrid'
        elif any(indicator in combined_text for indicator in remote_indicators):
            return 'Yes'
        else:
            return 'No'
    
    def save_to_cache(self, cache_key: str, jobs_data: List[Dict], metadata: Dict = None) -> bool:
        """Save job search results to Redis with metadata"""
        try:
            # Save individual jobs to Redis hashes
            saved_job_ids = []
            for job_data in jobs_data:
                if self.save_job_to_redis(job_data):
                    saved_job_ids.append(job_data.get('job_id'))
            
            # Save search results metadata
            search_metadata = {
                'cache_key': cache_key,
                'job_ids': json.dumps(saved_job_ids),
                'job_count': len(saved_job_ids),
                'metadata': json.dumps(metadata or {}),
                'created_at': datetime.now().isoformat(),
                'expires_at': (datetime.now() + timedelta(seconds=self.cache_duration_seconds)).isoformat()
            }
            
            # Store search metadata
            self.redis_client.hset(f"search:{cache_key}", mapping=search_metadata)
            self.redis_client.expire(f"search:{cache_key}", self.cache_duration_seconds)
            
            # Add to search index for easy retrieval
            self.redis_client.sadd("active_searches", cache_key)
            
            logger.info(f"Saved {len(saved_job_ids)} jobs to Redis cache with key: {cache_key}")
            return True
            
        except Exception as e:
            logger.error(f"Error saving to Redis cache: {str(e)}")
            return False
    
    def load_from_cache(self, cache_key: str) -> Optional[Dict]:
        """Load job search results from Redis cache"""
        try:
            # Check if search exists
            search_data = self.redis_client.hgetall(f"search:{cache_key}")
            if not search_data:
                logger.info(f"No cached data found for key: {cache_key}")
                return None
            
            # Check if cache has expired
            expires_at = datetime.fromisoformat(search_data.get('expires_at', ''))
            if datetime.now() > expires_at:
                logger.info(f"Cache expired for key: {cache_key}")
                self.clear_search_cache(cache_key)
                return None
            
            # Load individual jobs
            job_ids = json.loads(search_data.get('job_ids', '[]'))
            jobs_data = []
            
            for job_id in job_ids:
                job_data = self.redis_client.hgetall(f"{self.hash_name}:{job_id}")
                if job_data:
                    # Convert Redis hash back to job dictionary
                    processed_job = self._process_redis_job_data(job_data)
                    jobs_data.append(processed_job)
            
            logger.info(f"Loaded {len(jobs_data)} jobs from Redis cache")
            
            return {
                'timestamp': search_data.get('created_at'),
                'cache_key': cache_key,
                'metadata': json.loads(search_data.get('metadata', '{}')),
                'job_count': len(jobs_data),
                'data': jobs_data
            }
            
        except Exception as e:
            logger.error(f"Error loading from Redis cache: {str(e)}")
            return None
    
    def _process_redis_job_data(self, redis_job_data: Dict) -> Dict:
        """Convert Redis hash data back to job dictionary format"""
        try:
            return {
                'title': redis_job_data.get('title', ''),
                'company': redis_job_data.get('company', ''),
                'location': redis_job_data.get('location', ''),
                'description': redis_job_data.get('responsibilities', ''),
                'requirements': json.loads(redis_job_data.get('requirements', '[]')),
                'job_type': redis_job_data.get('job_type', ''),
                'skills': json.loads(redis_job_data.get('skills', '[]')),
                'posted_date': redis_job_data.get('posted_date', ''),
                'job_url': redis_job_data.get('url', ''),
                'salary': redis_job_data.get('salary', ''),
                'category': redis_job_data.get('category', ''),
                'is_trusted_company': redis_job_data.get('is_trusted_company', 'False') == 'True',
                'experience_level': redis_job_data.get('experience_level', ''),
                'employment_type': redis_job_data.get('employment_type', ''),
                'job_id': redis_job_data.get('job_id', ''),
                'remote_work': redis_job_data.get('remote', 'No')
            }
        except Exception as e:
            logger.error(f"Error processing Redis job data: {str(e)}")
            return {}
    
    def clear_expired_cache(self):
        """Remove expired cache entries"""
        try:
            current_time = datetime.now()
            expired_searches = []
            expired_jobs = []
            
            # Check active searches
            active_searches = self.redis_client.smembers("active_searches")
            for search_key in active_searches:
                search_data = self.redis_client.hget(f"search:{search_key}", "expires_at")
                if search_data:
                    try:
                        expires_at = datetime.fromisoformat(search_data)
                        if current_time > expires_at:
                            expired_searches.append(search_key)
                    except:
                        expired_searches.append(search_key)  # Invalid date format
            
            # Clear expired searches
            for search_key in expired_searches:
                self.clear_search_cache(search_key)
            
            # Check individual job hashes
            job_keys = self.redis_client.keys(f"{self.hash_name}:*")
            for job_key in job_keys:
                expires_at_str = self.redis_client.hget(job_key, "expires_at")
                if expires_at_str:
                    try:
                        expires_at = datetime.fromisoformat(expires_at_str)
                        if current_time > expires_at:
                            expired_jobs.append(job_key)
                    except:
                        expired_jobs.append(job_key)  # Invalid date format
            
            # Remove expired job hashes
            for job_key in expired_jobs:
                self.redis_client.delete(job_key)
            
            logger.info(f"Cleaned up {len(expired_searches)} expired searches and {len(expired_jobs)} expired jobs")
            
        except Exception as e:
            logger.error(f"Error clearing expired cache: {str(e)}")
    
    def clear_search_cache(self, cache_key: str):
        """Clear specific search cache"""
        try:
            # Get job IDs from search
            search_data = self.redis_client.hgetall(f"search:{cache_key}")
            if search_data and 'job_ids' in search_data:
                job_ids = json.loads(search_data['job_ids'])
                
                # Remove individual job hashes
                for job_id in job_ids:
                    self.redis_client.delete(f"{self.hash_name}:{job_id}")
            
            # Remove search metadata
            self.redis_client.delete(f"search:{cache_key}")
            
            # Remove from active searches
            self.redis_client.srem("active_searches", cache_key)
            
            logger.info(f"Cleared cache for search: {cache_key}")
            
        except Exception as e:
            logger.error(f"Error clearing search cache: {str(e)}")
    
    def clear_all_cache(self):
        """Remove all cached data"""
        try:
            # Get all active searches
            active_searches = self.redis_client.smembers("active_searches")
            
            # Clear each search
            for search_key in active_searches:
                self.clear_search_cache(search_key)
            
            # Clear any remaining job hashes
            job_keys = self.redis_client.keys(f"{self.hash_name}:*")
            if job_keys:
                self.redis_client.delete(*job_keys)
            
            # Clear search keys
            search_keys = self.redis_client.keys("search:*")
            if search_keys:
                self.redis_client.delete(*search_keys)
            
            # Clear active searches set
            self.redis_client.delete("active_searches")
            
            logger.info("Cleared all cached data from Redis")
            return len(active_searches)
            
        except Exception as e:
            logger.error(f"Error clearing all cache: {str(e)}")
            return 0
    
    def get_cache_info(self) -> Dict:
        """Get comprehensive cache information"""
        try:
            # Get active searches
            active_searches = self.redis_client.smembers("active_searches")
            
            # Get job statistics
            job_keys = self.redis_client.keys(f"{self.hash_name}:*")
            
            # Calculate memory usage
            memory_info = self.redis_client.info('memory')
            used_memory = memory_info.get('used_memory', 0)
            
            # Get detailed search info
            search_details = []
            total_jobs = 0
            
            for search_key in active_searches:
                search_data = self.redis_client.hgetall(f"search:{search_key}")
                if search_data:
                    job_count = int(search_data.get('job_count', 0))
                    total_jobs += job_count
                    
                    search_details.append({
                        'cache_key': search_key,
                        'job_count': job_count,
                        'created_at': search_data.get('created_at'),
                        'expires_at': search_data.get('expires_at'),
                        'metadata': json.loads(search_data.get('metadata', '{}'))
                    })
            
            return {
                'total_searches': len(active_searches),
                'total_job_hashes': len(job_keys),
                'total_jobs_cached': total_jobs,
                'redis_memory_used_mb': round(used_memory / (1024 * 1024), 2),
                'redis_connected': True,
                'cache_duration_hours': self.cache_duration_seconds // 3600,
                'searches': search_details[:10]  # Limit to first 10 for display
            }
            
        except Exception as e:
            logger.error(f"Error getting cache info: {str(e)}")
            return {
                'error': str(e),
                'redis_connected': False
            }
    
    def search_jobs_by_criteria(self, title_keyword: str = None, company_keyword: str = None,
                               location_keyword: str = None, remote_only: bool = False,
                               trusted_only: bool = False, limit: int = 50) -> List[Dict]:
        """Search cached jobs by specific criteria"""
        try:
            job_keys = self.redis_client.keys(f"{self.hash_name}:*")
            matching_jobs = []
            
            for job_key in job_keys:
                if len(matching_jobs) >= limit:
                    break
                    
                job_data = self.redis_client.hgetall(job_key)
                if not job_data:
                    continue
                
                # Apply filters
                if title_keyword and title_keyword.lower() not in job_data.get('title', '').lower():
                    continue
                
                if company_keyword and company_keyword.lower() not in job_data.get('company', '').lower():
                    continue
                
                if location_keyword and location_keyword.lower() not in job_data.get('location', '').lower():
                    continue
                
                if remote_only and job_data.get('remote', 'No') == 'No':
                    continue
                
                if trusted_only and job_data.get('is_trusted_company', 'False') != 'True':
                    continue
                
                # Convert and add to results
                processed_job = self._process_redis_job_data(job_data)
                matching_jobs.append(processed_job)
            
            logger.info(f"Found {len(matching_jobs)} jobs matching criteria")
            return matching_jobs
            
        except Exception as e:
            logger.error(f"Error searching jobs by criteria: {str(e)}")
            return []
    
    def get_job_statistics(self) -> Dict:
        """Get statistics about cached jobs"""
        try:
            job_keys = self.redis_client.keys(f"{self.hash_name}:*")
            
            if not job_keys:
                return {'total_jobs': 0}
            
            # Collect statistics
            stats = {
                'total_jobs': 0,
                'by_company': {},
                'by_location': {},
                'by_job_type': {},
                'by_category': {},
                'by_experience_level': {},
                'remote_stats': {'Yes': 0, 'No': 0, 'Hybrid': 0},
                'trusted_companies': 0
            }
            
            for job_key in job_keys:
                job_data = self.redis_client.hgetall(job_key)
                if not job_data:
                    continue
                
                stats['total_jobs'] += 1
                
                # Company stats
                company = job_data.get('company', 'Unknown')
                stats['by_company'][company] = stats['by_company'].get(company, 0) + 1
                
                # Location stats
                location = job_data.get('location', 'Unknown')
                stats['by_location'][location] = stats['by_location'].get(location, 0) + 1
                
                # Job type stats
                job_type = job_data.get('job_type', 'Unknown')
                stats['by_job_type'][job_type] = stats['by_job_type'].get(job_type, 0) + 1
                
                # Category stats
                category = job_data.get('category', 'Unknown')
                stats['by_category'][category] = stats['by_category'].get(category, 0) + 1
                
                # Experience level stats
                exp_level = job_data.get('experience_level', 'Unknown')
                stats['by_experience_level'][exp_level] = stats['by_experience_level'].get(exp_level, 0) + 1
                
                # Remote work stats
                remote = job_data.get('remote', 'No')
                if remote in stats['remote_stats']:
                    stats['remote_stats'][remote] += 1
                
                # Trusted company stats
                if job_data.get('is_trusted_company', 'False') == 'True':
                    stats['trusted_companies'] += 1
            
            # Sort top categories by count
            for category in ['by_company', 'by_location', 'by_job_type', 'by_category', 'by_experience_level']:
                stats[category] = dict(sorted(stats[category].items(), key=lambda x: x[1], reverse=True))
            
            return stats
            
        except Exception as e:
            logger.error(f"Error getting job statistics: {str(e)}")
            return {'error': str(e)}


class RedisCachedJobScraper:
    """Enhanced job scraper with Redis caching capability"""
    
    def __init__(self, redis_host: str = 'localhost', redis_port: int = 6379, 
                 redis_db: int = 0, redis_password: str = None, 
                 cache_duration_hours: int = 72):
        """Initialize Redis-cached job scraper"""
        try:
            self.scraper = LinkedInJobScraper()
            self.cache = RedisJobDataCache(
                redis_host=redis_host,
                redis_port=redis_port,
                redis_db=redis_db,
                redis_password=redis_password,
                cache_duration_hours=cache_duration_hours
            )
            logger.info("RedisCachedJobScraper initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize RedisCachedJobScraper: {str(e)}")
            raise
    
    def get_jobs(self, keywords: str = "software engineer", location: str = "United States",
                max_jobs: int = 50, job_type_filter: str = None, category_filter: str = None,
                trusted_only: bool = True, force_refresh: bool = False) -> List[Dict]:
        """Get jobs with Redis caching support"""
        
        # Generate cache key
        cache_key = self.cache.generate_cache_key(
            keywords, location, max_jobs, job_type_filter, category_filter, trusted_only
        )
        
        logger.info(f"Searching jobs: {keywords} in {location}")
        
        # Check cache first (unless force refresh)
        if not force_refresh:
            cached_data = self.cache.load_from_cache(cache_key)
            if cached_data:
                logger.info(f"Returning {len(cached_data['data'])} jobs from cache")
                return cached_data['data']
            
            # Search individual cached jobs
            cached_jobs = self.search_cached_jobs(
                title_keyword=keywords,
                location_keyword=location if location != "United States" else None,
                trusted_only=trusted_only,
                limit=max_jobs * 2
            )
            
            # Filter cached jobs
            filtered_jobs = self._filter_jobs(cached_jobs, keywords, location, job_type_filter, category_filter)
            
            if len(filtered_jobs) >= max_jobs:
                logger.info(f"Found {len(filtered_jobs)} matching jobs in cache")
                return filtered_jobs[:max_jobs]
        
        # Scrape fresh data
        logger.info("Scraping fresh data...")
        jobs_data = self.scraper.scrape_jobs(
            keywords=keywords,
            location=location,
            max_jobs=max_jobs,
            job_type_filter=job_type_filter,
            category_filter=category_filter,
            trusted_only=trusted_only
        )
        
        # Save to cache
        metadata = {
            'keywords': keywords,
            'location': location,
            'max_jobs': max_jobs,
            'job_type_filter': job_type_filter,
            'category_filter': category_filter,
            'trusted_only': trusted_only,
            'scraped_at': datetime.now().isoformat(),
            'scraper_version': '2.0',
            'source': 'LinkedIn'
        }
        
        success = self.cache.save_to_cache(cache_key, jobs_data, metadata)
        if success:
            logger.info(f"Successfully cached {len(jobs_data)} jobs")
        
        return jobs_data
    
    def _filter_jobs(self, jobs, keywords, location, job_type_filter, category_filter):
        """Filter jobs based on criteria"""
        filtered = []
        keywords_lower = keywords.lower() if keywords else ""
        location_lower = location.lower() if location else ""
        
        for job in jobs:
            # Keywords match
            if keywords:
                title_match = keywords_lower in job.get('title', '').lower()
                desc_match = keywords_lower in job.get('description', '').lower()
                if not (title_match or desc_match):
                    continue
            
            # Location match
            if location and location.lower() != "united states":
                if location_lower not in job.get('location', '').lower():
                    continue
            
            # Job type filter
            if job_type_filter:
                job_type = job.get('employment_type', '').lower()
                if job_type_filter.lower() not in job_type:
                    continue
            
            # Category filter
            if category_filter and category_filter != 'All':
                if category_filter not in job.get('category', ''):
                    continue
            
            filtered.append(job)
        
        return filtered
    
    def search_cached_jobs(self, title_keyword: str = None, company_keyword: str = None,
                          location_keyword: str = None, remote_only: bool = False,
                          trusted_only: bool = False, limit: int = 50) -> List[Dict]:
        """Search through cached jobs"""
        return self.cache.search_jobs_by_criteria(
            title_keyword=title_keyword,
            company_keyword=company_keyword,
            location_keyword=location_keyword,
            remote_only=remote_only,
            trusted_only=trusted_only,
            limit=limit
        )
    
    def get_job_categories(self) -> List[str]:
        """Get available job categories"""
        return self.scraper.get_available_categories()
    
    def get_trusted_companies(self) -> List[str]:
        """Get list of trusted companies"""
        return self.scraper.get_trusted_companies_list()
    
    def get_cache_status(self) -> Dict:
        """Get cache status"""
        return self.cache.get_cache_info()
    
    def get_job_statistics(self) -> Dict:
        """Get job statistics"""
        return self.cache.get_job_statistics()
    
    def clear_cache(self, expired_only: bool = True) -> int:
        """Clear cache"""
        if expired_only:
            self.cache.clear_expired_cache()
            return 0  # clear_expired_cache doesn't return count
        else:
            return self.cache.clear_all_cache()
    
    def get_redis_health(self) -> Dict:
        """Check Redis health"""
        try:
            self.cache.redis_client.ping()
            info = self.cache.redis_client.info()
            return {
                'connected': True,
                'redis_version': info.get('redis_version', 'unknown'),
                'used_memory_human': info.get('used_memory_human', 'unknown'),
                'connected_clients': info.get('connected_clients', 0)
            }
        except Exception as e:
            return {'connected': False, 'error': str(e)}