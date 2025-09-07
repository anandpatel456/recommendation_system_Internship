from fastapi import FastAPI
from app.routers import recommendations
from app.core.db import db
from fastapi.middleware.cors import CORSMiddleware


app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"]
)

app.get("/")
async def root():
    return {"message": "Welcome to the Job Recommender API"}
@app.get("/health")
async def health_check():
    return {"status": "healthy"}
app.include_router(
    recommendations.router,
    prefix="/api/recommend",
    tags=["recommendations"]
)

if __name__ == "__main__":
    import os
    import uvicorn
    port = int(os.getenv("PORT", 10000))  # Use PORT from environment, default to 10000 if unset
    uvicorn.run(app, host="0.0.0.0", port=port)