from bson import ObjectId
from typing import Any

def convert_mongo_doc(doc: dict) -> dict:
    """Convert MongoDB document to Pydantic-compatible dict"""
    if not doc:
        return None
    
    # Convert ObjectId to string
    if "_id" in doc:
        doc["id"] = str(doc["_id"])
    
    # Handle nested objects
    for key, value in doc.items():
        if isinstance(value, ObjectId):
            doc[key] = str(value)
        elif isinstance(value, dict):
            doc[key] = convert_mongo_doc(value)
    
    return doc