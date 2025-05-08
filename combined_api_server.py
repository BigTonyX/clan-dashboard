from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from api_server import app as clan_app
from member_api_server import app as member_app
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address

# Create the main FastAPI app
app = FastAPI(
    title="Clan Dashboard Combined API",
    version="0.1.0",
    docs_url=None,
    redoc_url=None
)

# Add CORS middleware to only allow GitHub Pages frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://bigtonyx.github.io"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Add global rate limiting: 30 requests per minute per IP
limiter = Limiter(
    key_func=get_remote_address,
    default_limits=["30/minute"]
)
app.state.limiter = limiter
app.add_exception_handler(429, _rate_limit_exceeded_handler)

@app.middleware("http")
async def add_rate_limit(request, call_next):
    response = await limiter(request, call_next)
    return response

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