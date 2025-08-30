from main import app
import os

# Vercel serverless function handler
def handler(request, context):
    # Set environment variables for Vercel
    os.environ.setdefault('MONGO_URI', os.getenv('MONGO_URI', 'mongodb://localhost:27017/jobswipe_prod'))
    
    # Import and return the FastAPI app
    from mangum import Mangum
    handler = Mangum(app)
    return handler(request, context)
