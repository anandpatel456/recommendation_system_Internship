import json
from app.core.db import get_users_collection, get_jobs_collection

def load_sample_data():
    with open('sample_users.json') as f:
        get_users_collection().insert_many(json.load(f))
    
    with open('sample_jobs.json') as f:
        get_jobs_collection().insert_many(json.load(f))

if __name__ == "__main__":
    load_sample_data()