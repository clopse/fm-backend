from fastapi import FastAPI, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.trustedhost import TrustedHostMiddleware
from fastapi.responses import JSONResponse
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
from dotenv import load_dotenv
import os
import time
from collections import defaultdict

load_dotenv()

# Rate limiter setup
limiter = Limiter(key_func=get_remote_address)

# Create FastAPI app with security configurations
app = FastAPI(
    title="JMK Hotels API",
    description="Secure API for JMK Hotels Management System",
    version="2.0.0",
    # Hide docs in production
    docs_url="/docs" if os.getenv("ENVIRONMENT") != "production" else None,
    redoc_url="/redoc" if os.getenv("ENVIRONMENT") != "production" else None,
)

# Add rate limiting
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

# Security middleware
@app.middleware("http")
async def security_headers_middleware(request: Request, call_next):
    response = await call_next(request)
    
    # Add security headers
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-XSS-Protection"] = "1; mode=block"
    response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
    response.headers["Permissions-Policy"] = "geolocation=(), microphone=(), camera=()"
    
    # HSTS in production
    if os.getenv("ENVIRONMENT") == "production":
        response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"
    
    # Remove server header
    if "server" in response.headers:
        del response.headers["server"]
    
    return response

# Request logging middleware
@app.middleware("http")
async def logging_middleware(request: Request, call_next):
    start_time = time.time()
    
    # Get client IP
    client_ip = request.headers.get("X-Forwarded-For", request.client.host if request.client else "unknown")
    
    response = await call_next(request)
    
    process_time = time.time() - start_time
    
    # Log request (in production, send to proper logging service)
    print(f"{client_ip} - {request.method} {request.url.path} - {response.status_code} - {process_time:.3f}s")
    
    return response

# HTTPS redirect middleware (for production)
@app.middleware("http")
async def https_redirect_middleware(request: Request, call_next):
    if (os.getenv("ENVIRONMENT") == "production" and 
        not request.url.scheme == "https" and 
        not request.headers.get("X-Forwarded-Proto") == "https"):
        
        # Redirect to HTTPS
        https_url = request.url.replace(scheme="https")
        return JSONResponse(
            status_code=301,
            headers={"Location": str(https_url)}
        )
    
    return await call_next(request)

# Trusted host middleware (configure for your domains)
if os.getenv("ENVIRONMENT") == "production":
    allowed_hosts = os.getenv("ALLOWED_HOSTS", "").split(",")
    if allowed_hosts:
        app.add_middleware(TrustedHostMiddleware, allowed_hosts=allowed_hosts)

# CORS configuration
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://yourdomain.com",  # Replace with your production domain
        "https://www.yourdomain.com",  # Replace with your production domain
        "http://localhost:3000",  # Development
        "http://127.0.0.1:3000",  # Development
    ] if os.getenv("ENVIRONMENT") == "production" else ["*"],
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE"],
    allow_headers=["*"],
    max_age=86400,  # 24 hours
)

# Health check endpoint with rate limiting
@app.get("/health")
@limiter.limit("10/minute")
async def health_check(request: Request):
    return {"status": "ok", "environment": os.getenv("ENVIRONMENT", "development")}

# Root endpoint
@app.get("/")
@limiter.limit("30/minute")
async def root(request: Request):
    return {"message": "JMK Hotels API v2.0 - Secure Edition 🔒"}

# Import and include routers
from app.routers import (
    user,  # Your updated secure user router
    uploads,
    utilities,
    water,
    drawings,
    tenders,
    compliance,
    admin,
    emails,
    incidents,
    inspections
)

# Include routers with rate limiting
app.include_router(
    user.router, 
    prefix="/api/users", 
    tags=["users"],
    dependencies=[limiter.limit("100/minute")]  # Rate limit user endpoints
)

app.include_router(uploads.router, prefix="/api/uploads", tags=["uploads"])
app.include_router(utilities.router, prefix="/api/utilities", tags=["utilities"])
app.include_router(water.router, prefix="/api/water", tags=["water"])
app.include_router(drawings.router, prefix="/api/drawings", tags=["drawings"])
app.include_router(tenders.router, prefix="/api/tenders", tags=["tenders"])
app.include_router(compliance.router, prefix="/api/compliance", tags=["compliance"])
app.include_router(admin.router, prefix="/api/admin", tags=["admin"])
app.include_router(emails.router, prefix="/api/emails", tags=["emails"])
app.include_router(incidents.router, prefix="/api/incidents", tags=["incidents"])
app.include_router(inspections.router, prefix="/api/inspections", tags=["inspections"])

# Global exception handler
@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    return JSONResponse(
        status_code=exc.status_code,
        content={"detail": exc.detail, "error_code": exc.status_code}
    )

# Generic exception handler
@app.exception_handler(Exception)
async def generic_exception_handler(request: Request, exc: Exception):
    # In production, log the full error but don't expose it
    if os.getenv("ENVIRONMENT") == "production":
        print(f"Unhandled exception: {exc}")
        return JSONResponse(
            status_code=500,
            content={"detail": "Internal server error", "error_code": 500}
        )
    else:
        # In development, show the full error
        return JSONResponse(
            status_code=500,
            content={"detail": str(exc), "error_code": 500}
        )

if __name__ == "__main__":
    import uvicorn
    
    # Run with security considerations
    uvicorn.run(
        app,
        host="0.0.0.0" if os.getenv("ENVIRONMENT") == "production" else "127.0.0.1",
        port=int(os.getenv("PORT", 8000)),
        reload=os.getenv("ENVIRONMENT") != "production",
        access_log=False,  # Use custom logging middleware instead
    )
