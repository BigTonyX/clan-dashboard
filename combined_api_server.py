from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from api_server import app as clan_app
from member_api_server import app as member_app

# Create the main FastAPI app
app = FastAPI(
    title="Clan Dashboard Combined API",
    version="0.1.0",
    docs_url=None,
    redoc_url=None
)

# Add CORS middleware to the main app
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Mount the clan API sub-application
app.mount("/api/clan", clan_app)
# Mount the member API sub-application
app.mount("/api/member", member_app)

# Define a basic 'root' endpoint
@app.get("/")
async def read_root():
    """Basic endpoint to check if the API is running."""
    return {"message": "Welcome to the Clan Dashboard Combined API!"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000) 