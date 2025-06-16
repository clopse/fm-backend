from fastapi import APIRouter, HTTPException, Request, Depends, status, BackgroundTasks
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel, EmailStr, validator
from typing import Optional, List, Dict, Any
import json
import boto3
import bcrypt
from jose import jwt, JWTError
from datetime import datetime, timedelta
import uuid
import os
import asyncio
from collections import defaultdict
import re

router = APIRouter()
security = HTTPBearer()

# Configuration
s3 = boto3.client(
    's3',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    region_name=os.getenv('AWS_REGION')
)
BUCKET_NAME = os.getenv("AWS_BUCKET_NAME")
USERS_KEY = "data/users.json"
AUDIT_LOG_KEY = "data/audit_logs.json"
RESET_TOKENS_KEY = "data/password_reset_tokens.json"
SECRET_KEY = os.getenv("SECRET_KEY", "your-secret-key-here-change-this")
REFRESH_SECRET_KEY = os.getenv("REFRESH_SECRET_KEY", "your-refresh-secret-key-here")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 15  # Short-lived access tokens
REFRESH_TOKEN_EXPIRE_DAYS = 7
PASSWORD_RESET_TOKEN_EXPIRE_MINUTES = 30
MAX_LOGIN_ATTEMPTS = 5
LOCKOUT_DURATION_MINUTES = 30

# Rate limiting storage (in production, use Redis)
login_attempts = defaultdict(list)
locked_accounts = {}

# Password strength requirements
MIN_PASSWORD_LENGTH = 8
REQUIRE_UPPERCASE = True
REQUIRE_LOWERCASE = True
REQUIRE_NUMBERS = True
REQUIRE_SPECIAL_CHARS = True

# Pydantic Models
class UserCreate(BaseModel):
    name: str
    email: EmailStr
    role: str
    hotel: str
    password: str

    @validator('email')
    def normalize_email(cls, v):
        return v.lower().strip()
    
    @validator('password')
    def validate_password_strength(cls, v):
        if len(v) < MIN_PASSWORD_LENGTH:
            raise ValueError(f'Password must be at least {MIN_PASSWORD_LENGTH} characters long')
        
        if REQUIRE_UPPERCASE and not re.search(r'[A-Z]', v):
            raise ValueError('Password must contain at least one uppercase letter')
        
        if REQUIRE_LOWERCASE and not re.search(r'[a-z]', v):
            raise ValueError('Password must contain at least one lowercase letter')
        
        if REQUIRE_NUMBERS and not re.search(r'\d', v):
            raise ValueError('Password must contain at least one number')
        
        if REQUIRE_SPECIAL_CHARS and not re.search(r'[!@#$%^&*(),.?":{}|<>]', v):
            raise ValueError('Password must contain at least one special character')
        
        return v

class UserUpdate(BaseModel):
    name: Optional[str] = None
    email: Optional[EmailStr] = None
    role: Optional[str] = None
    hotel: Optional[str] = None
    status: Optional[str] = None

    @validator('email')
    def normalize_email(cls, v):
        if v is not None:
            return v.lower().strip()
        return v

class UserLogin(BaseModel):
    email: EmailStr
    password: str

    @validator('email')
    def normalize_email(cls, v):
        return v.lower().strip()

class PasswordReset(BaseModel):
    password: str
    
    @validator('password')
    def validate_password_strength(cls, v):
        if len(v) < MIN_PASSWORD_LENGTH:
            raise ValueError(f'Password must be at least {MIN_PASSWORD_LENGTH} characters long')
        
        if REQUIRE_UPPERCASE and not re.search(r'[A-Z]', v):
            raise ValueError('Password must contain at least one uppercase letter')
        
        if REQUIRE_LOWERCASE and not re.search(r'[a-z]', v):
            raise ValueError('Password must contain at least one lowercase letter')
        
        if REQUIRE_NUMBERS and not re.search(r'\d', v):
            raise ValueError('Password must contain at least one number')
        
        if REQUIRE_SPECIAL_CHARS and not re.search(r'[!@#$%^&*(),.?":{}|<>]', v):
            raise ValueError('Password must contain at least one special character')
        
        return v

class ForgotPasswordRequest(BaseModel):
    email: EmailStr
    
    @validator('email')
    def normalize_email(cls, v):
        return v.lower().strip()

class ResetPasswordRequest(BaseModel):
    token: str
    password: str
    
    @validator('password')
    def validate_password_strength(cls, v):
        if len(v) < MIN_PASSWORD_LENGTH:
            raise ValueError(f'Password must be at least {MIN_PASSWORD_LENGTH} characters long')
        
        if REQUIRE_UPPERCASE and not re.search(r'[A-Z]', v):
            raise ValueError('Password must contain at least one uppercase letter')
        
        if REQUIRE_LOWERCASE and not re.search(r'[a-z]', v):
            raise ValueError('Password must contain at least one lowercase letter')
        
        if REQUIRE_NUMBERS and not re.search(r'\d', v):
            raise ValueError('Password must contain at least one number')
        
        if REQUIRE_SPECIAL_CHARS and not re.search(r'[!@#$%^&*(),.?":{}|<>]', v):
            raise ValueError('Password must contain at least one special character')
        
        return v

class TokenRefresh(BaseModel):
    refresh_token: str

class User(BaseModel):
    id: str
    name: str
    email: str
    role: str
    hotel: str
    status: str = "Active"
    created_at: str
    last_login: Optional[str] = None
    failed_login_attempts: Optional[int] = 0
    locked_until: Optional[str] = None

class UserResponse(BaseModel):
    id: str
    name: str
    email: str
    role: str
    hotel: str
    status: str
    created_at: str
    last_login: Optional[str]

class TokenResponse(BaseModel):
    access_token: str
    refresh_token: str
    token_type: str
    user: UserResponse
    expires_in: int

class StandardResponse(BaseModel):
    message: str
    success: bool = True

# Helper Functions
def load_users() -> dict:
    """Load users from S3"""
    try:
        obj = s3.get_object(Bucket=BUCKET_NAME, Key=USERS_KEY)
        return json.loads(obj["Body"].read().decode("utf-8"))
    except s3.exceptions.NoSuchKey:
        return {}
    except Exception:
        return {}

def save_users(users: dict):
    """Save users to S3"""
    try:
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=USERS_KEY,
            Body=json.dumps(users, indent=2),
            ContentType="application/json"
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to save users: {str(e)}")

def load_reset_tokens() -> dict:
    """Load password reset tokens from S3"""
    try:
        obj = s3.get_object(Bucket=BUCKET_NAME, Key=RESET_TOKENS_KEY)
        return json.loads(obj["Body"].read().decode("utf-8"))
    except s3.exceptions.NoSuchKey:
        return {}
    except Exception:
        return {}

def save_reset_tokens(tokens: dict):
    """Save password reset tokens to S3"""
    try:
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=RESET_TOKENS_KEY,
            Body=json.dumps(tokens, indent=2),
            ContentType="application/json"
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to save reset tokens: {str(e)}")

def log_audit_event(event_type: str, user_id: str, details: dict, ip_address: str = None):
    """Log audit events"""
    try:
        # Load existing logs
        try:
            obj = s3.get_object(Bucket=BUCKET_NAME, Key=AUDIT_LOG_KEY)
            logs = json.loads(obj["Body"].read().decode("utf-8"))
        except s3.exceptions.NoSuchKey:
            logs = []
        
        # Add new log entry
        log_entry = {
            "timestamp": datetime.utcnow().isoformat(),
            "event_type": event_type,
            "user_id": user_id,
            "ip_address": ip_address,
            "details": details
        }
        logs.append(log_entry)
        
        # Keep only last 10000 entries to prevent infinite growth
        if len(logs) > 10000:
            logs = logs[-10000:]
        
        # Save logs
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=AUDIT_LOG_KEY,
            Body=json.dumps(logs, indent=2),
            ContentType="application/json"
        )
    except Exception as e:
        # Don't fail the main operation if logging fails
        print(f"Audit logging failed: {e}")

def find_user_by_email(users: dict, email: str) -> tuple:
    """Find user by email (case-insensitive)"""
    normalized_email = email.lower().strip()
    for user_id, user_data in users.items():
        if user_data["email"].lower().strip() == normalized_email:
            return user_id, user_data
    return None, None

def hash_password(password: str) -> str:
    """Hash password using bcrypt with higher cost factor"""
    return bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt(rounds=12)).decode('utf-8')

def verify_password(password: str, hashed: str) -> bool:
    """Verify password against hash"""
    return bcrypt.checkpw(password.encode('utf-8'), hashed.encode('utf-8'))

def create_access_token(data: dict, expires_delta: Optional[timedelta] = None):
    """Create JWT access token"""
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    to_encode.update({"exp": expire, "type": "access"})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt

def create_refresh_token(data: dict):
    """Create JWT refresh token"""
    to_encode = data.copy()
    expire = datetime.utcnow() + timedelta(days=REFRESH_TOKEN_EXPIRE_DAYS)
    to_encode.update({"exp": expire, "type": "refresh"})
    encoded_jwt = jwt.encode(to_encode, REFRESH_SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt

def create_reset_token(email: str) -> str:
    """Create a password reset token"""
    token_data = {
        "email": email,
        "expires": (datetime.utcnow() + timedelta(minutes=PASSWORD_RESET_TOKEN_EXPIRE_MINUTES)).isoformat(),
        "used": False
    }
    
    # Generate a secure random token
    token = str(uuid.uuid4())
    
    # Load existing tokens
    tokens = load_reset_tokens()
    
    # Clean up expired tokens
    now = datetime.utcnow()
    tokens = {k: v for k, v in tokens.items() if datetime.fromisoformat(v["expires"]) > now}
    
    # Add new token
    tokens[token] = token_data
    save_reset_tokens(tokens)
    
    return token

def verify_reset_token(token: str) -> Optional[str]:
    """Verify a password reset token and return email if valid"""
    tokens = load_reset_tokens()
    
    if token not in tokens:
        return None
    
    token_data = tokens[token]
    
    # Check if token is expired
    if datetime.fromisoformat(token_data["expires"]) < datetime.utcnow():
        return None
    
    # Check if token is already used
    if token_data["used"]:
        return None
    
    return token_data["email"]

def invalidate_reset_token(token: str):
    """Mark a reset token as used"""
    tokens = load_reset_tokens()
    if token in tokens:
        tokens[token]["used"] = True
        save_reset_tokens(tokens)

def send_password_reset_email(email: str, token: str, background_tasks: BackgroundTasks):
    """Send password reset email (placeholder - implement with your email service)"""
    # This is where you'd integrate with your email service
    # For now, this is a placeholder that logs the token
    def log_reset_email():
        print(f"Password reset email for {email}")
        print(f"Reset token: {token}")
        print(f"Reset link: https://your-frontend-domain.com/reset-password?token={token}")
        
        # Log the reset request
        log_audit_event("password_reset_requested", email, {
            "token": token[:8] + "...",  # Only log partial token for security
            "expires": (datetime.utcnow() + timedelta(minutes=PASSWORD_RESET_TOKEN_EXPIRE_MINUTES)).isoformat()
        })
    
    background_tasks.add_task(log_reset_email)

def verify_token(credentials: HTTPAuthorizationCredentials = Depends(security)):
    """Verify JWT access token"""
    try:
        payload = jwt.decode(credentials.credentials, SECRET_KEY, algorithms=[ALGORITHM])
        email: str = payload.get("sub")
        token_type: str = payload.get("type")
        
        if email is None or token_type != "access":
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid token",
                headers={"WWW-Authenticate": "Bearer"},
            )
        return email
    except JWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token",
            headers={"WWW-Authenticate": "Bearer"},
        )

def get_current_user(email: str = Depends(verify_token)):
    """Get current user from token"""
    users = load_users()
    user_id, user_data = find_user_by_email(users, email)
    
    if not user_data:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="User not found"
        )
    
    if user_data["status"] != "Active":
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Account is inactive"
        )
    
    return User(**user_data, id=user_id)

def require_admin(current_user: User = Depends(get_current_user)):
    """Require admin privileges"""
    admin_roles = ['system admin', 'administrator', 'admin']
    if not any(role.lower() in current_user.role.lower() for role in admin_roles):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Admin privileges required"
        )
    return current_user

def require_manager_or_admin(current_user: User = Depends(get_current_user)):
    """Require manager or admin privileges"""
    privileged_roles = ['system admin', 'administrator', 'admin', 'manager', 'boss', 'director']
    if not any(role.lower() in current_user.role.lower() for role in privileged_roles):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Manager or admin privileges required"
        )
    return current_user

def check_rate_limit(email: str, ip_address: str) -> bool:
    """Check if user has exceeded login rate limit"""
    now = datetime.utcnow()
    
    # Clean old attempts (older than lockout duration)
    cutoff_time = now - timedelta(minutes=LOCKOUT_DURATION_MINUTES)
    login_attempts[email] = [attempt for attempt in login_attempts[email] if attempt > cutoff_time]
    
    # Check if account is locked
    if email in locked_accounts:
        if now < locked_accounts[email]:
            return False
        else:
            # Unlock account
            del locked_accounts[email]
            login_attempts[email] = []
    
    # Check number of recent attempts
    return len(login_attempts[email]) < MAX_LOGIN_ATTEMPTS

def record_failed_login(email: str, ip_address: str):
    """Record failed login attempt"""
    now = datetime.utcnow()
    login_attempts[email].append(now)
    
    # Lock account if too many attempts
    if len(login_attempts[email]) >= MAX_LOGIN_ATTEMPTS:
        locked_accounts[email] = now + timedelta(minutes=LOCKOUT_DURATION_MINUTES)

def get_client_ip(request: Request) -> str:
    """Get client IP address"""
    forwarded = request.headers.get("X-Forwarded-For")
    if forwarded:
        return forwarded.split(",")[0].strip()
    return request.client.host

# Authentication Routes
@router.post("/auth/login", response_model=TokenResponse)
async def login(user_login: UserLogin, request: Request, background_tasks: BackgroundTasks):
    """User login with rate limiting and audit logging"""
    ip_address = get_client_ip(request)
    
    # Check rate limit
    if not check_rate_limit(user_login.email, ip_address):
        log_audit_event("login_blocked", user_login.email, {
            "reason": "rate_limit_exceeded",
            "ip_address": ip_address
        })
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail=f"Too many failed login attempts. Account locked for {LOCKOUT_DURATION_MINUTES} minutes."
        )
    
    users = load_users()
    user_id, user_data = find_user_by_email(users, user_login.email)

    if not user_data or not verify_password(user_login.password, user_data["password"]):
        record_failed_login(user_login.email, ip_address)
        log_audit_event("login_failed", user_login.email, {
            "reason": "invalid_credentials",
            "ip_address": ip_address
        })
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect email or password"
        )

    if user_data["status"] != "Active":
        log_audit_event("login_failed", user_id, {
            "reason": "account_inactive",
            "ip_address": ip_address
        })
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Account is inactive"
        )

    # Clear failed attempts on successful login
    if user_login.email in login_attempts:
        del login_attempts[user_login.email]

    # Update last login
    user_data["last_login"] = datetime.utcnow().isoformat()
    user_data["failed_login_attempts"] = 0
    users[user_id] = user_data
    save_users(users)

    # Create tokens
    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(
        data={"sub": user_data["email"]}, expires_delta=access_token_expires
    )
    refresh_token = create_refresh_token(data={"sub": user_data["email"]})

    user_response = UserResponse(**user_data, id=user_id)
    
    # Log successful login
    log_audit_event("login_success", user_id, {
        "ip_address": ip_address,
        "user_agent": request.headers.get("user-agent", "unknown")
    })

    return TokenResponse(
        access_token=access_token,
        refresh_token=refresh_token,
        token_type="bearer",
        user=user_response,
        expires_in=ACCESS_TOKEN_EXPIRE_MINUTES * 60
    )

@router.post("/auth/refresh", response_model=TokenResponse)
async def refresh_token(token_data: TokenRefresh, request: Request):
    """Refresh access token using refresh token"""
    try:
        payload = jwt.decode(token_data.refresh_token, REFRESH_SECRET_KEY, algorithms=[ALGORITHM])
        email: str = payload.get("sub")
        token_type: str = payload.get("type")
        
        if email is None or token_type != "refresh":
            raise JWTError("Invalid refresh token")
            
    except JWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid refresh token"
        )
    
    # Get user
    users = load_users()
    user_id, user_data = find_user_by_email(users, email)
    
    if not user_data or user_data["status"] != "Active":
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="User not found or inactive"
        )
    
    # Create new tokens
    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(
        data={"sub": user_data["email"]}, expires_delta=access_token_expires
    )
    new_refresh_token = create_refresh_token(data={"sub": user_data["email"]})
    
    user_response = UserResponse(**user_data, id=user_id)
    
    # Log token refresh
    log_audit_event("token_refresh", user_id, {
        "ip_address": get_client_ip(request)
    })
    
    return TokenResponse(
        access_token=access_token,
        refresh_token=new_refresh_token,
        token_type="bearer",
        user=user_response,
        expires_in=ACCESS_TOKEN_EXPIRE_MINUTES * 60
    )

@router.post("/auth/logout", response_model=StandardResponse)
async def logout(current_user: User = Depends(get_current_user), request: Request = None):
    """User logout with audit logging"""
    log_audit_event("logout", current_user.id, {
        "ip_address": get_client_ip(request) if request else None
    })
    return StandardResponse(message="Successfully logged out")

@router.post("/auth/forgot-password", response_model=StandardResponse)
async def forgot_password(
    request_data: ForgotPasswordRequest, 
    request: Request,
    background_tasks: BackgroundTasks
):
    """Request password reset"""
    users = load_users()
    user_id, user_data = find_user_by_email(users, request_data.email)
    
    # Always return success to prevent email enumeration
    # But only send email if user exists
    if user_data and user_data["status"] == "Active":
        # Create reset token
        reset_token = create_reset_token(request_data.email)
        
        # Send reset email
        send_password_reset_email(request_data.email, reset_token, background_tasks)
        
        # Log the request
        log_audit_event("password_reset_requested", user_id, {
            "ip_address": get_client_ip(request)
        })
    else:
        # Log failed attempt for non-existent users
        log_audit_event("password_reset_failed", request_data.email, {
            "reason": "user_not_found",
            "ip_address": get_client_ip(request)
        })
    
    return StandardResponse(
        message="If your email address is registered, you will receive a password reset link shortly."
    )

@router.post("/auth/reset-password", response_model=StandardResponse)
async def reset_password(
    reset_data: ResetPasswordRequest,
    request: Request
):
    """Reset password using token"""
    # Verify token
    email = verify_reset_token(reset_data.token)
    if not email:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid or expired reset token"
        )
    
    # Get user
    users = load_users()
    user_id, user_data = find_user_by_email(users, email)
    
    if not user_data:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found"
        )
    
    if user_data["status"] != "Active":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Account is not active"
        )
    
    # Update password
    user_data["password"] = hash_password(reset_data.password)
    user_data["failed_login_attempts"] = 0  # Reset failed attempts
    user_data["locked_until"] = None  # Unlock account if locked
    
    users[user_id] = user_data
    save_users(users)
    
    # Invalidate the reset token
    invalidate_reset_token(reset_data.token)
    
    # Clear any existing login attempts
    if email in login_attempts:
        del login_attempts[email]
    if email in locked_accounts:
        del locked_accounts[email]
    
    # Log password reset
    log_audit_event("password_reset_completed", user_id, {
        "ip_address": get_client_ip(request)
    })
    
    return StandardResponse(
        message="Password successfully reset. You can now log in with your new password."
    )

@router.get("/auth/verify-reset-token/{token}")
async def verify_reset_token_endpoint(token: str):
    """Verify if a reset token is valid (for frontend validation)"""
    email = verify_reset_token(token)
    if not email:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid or expired reset token"
        )
    
    return {
        "valid": True,
        "email": email
    }

# Admin-Only User Management Routes
@router.get("/", response_model=List[UserResponse])
async def get_users(
    role: Optional[str] = None,
    hotel: Optional[str] = None,
    status: Optional[str] = None,
    search: Optional[str] = None,
    current_user: User = Depends(require_admin)
):
    """Get all users with optional filtering - ADMIN ONLY"""
    users = load_users()
    user_list = []

    for user_id, user_data in users.items():
        user_response = UserResponse(**user_data, id=user_id)

        # Apply filters
        if role and user_data["role"] != role:
            continue
        if hotel and hotel != "All Hotels" and user_data["hotel"] != hotel:
            continue
        if status and user_data["status"] != status:
            continue
        if search and search.lower() not in user_data["name"].lower() and search.lower() not in user_data["email"].lower():
            continue

        user_list.append(user_response)

    return user_list

@router.get("/{user_id}", response_model=UserResponse)
async def get_user(user_id: str, current_user: User = Depends(require_manager_or_admin)):
    """Get a specific user - MANAGER/ADMIN ONLY"""
    users = load_users()

    if user_id not in users:
        raise HTTPException(status_code=404, detail="User not found")

    return UserResponse(**users[user_id], id=user_id)

@router.post("/", response_model=UserResponse)
async def create_user(user_create: UserCreate, request: Request, current_user: User = Depends(require_admin)):
    """Create a new user - ADMIN ONLY"""
    users = load_users()

    # Check if email already exists
    existing_user_id, existing_user_data = find_user_by_email(users, user_create.email)
    if existing_user_data:
        raise HTTPException(status_code=400, detail="Email already registered")

    # Create new user
    user_id = str(uuid.uuid4())
    hashed_password = hash_password(user_create.password)

    new_user = {
        "name": user_create.name,
        "email": user_create.email,
        "role": user_create.role,
        "hotel": user_create.hotel,
        "password": hashed_password,
        "status": "Active",
        "created_at": datetime.utcnow().isoformat(),
        "last_login": None,
        "failed_login_attempts": 0,
        "locked_until": None
    }

    users[user_id] = new_user
    save_users(users)
    
    # Log user creation
    log_audit_event("user_created", user_id, {
        "created_by": current_user.id,
        "user_email": user_create.email,
        "user_role": user_create.role,
        "ip_address": get_client_ip(request)
    })

    return UserResponse(**new_user, id=user_id)

@router.put("/{user_id}", response_model=UserResponse)
async def update_user(
    user_id: str, 
    user_update: UserUpdate,
    request: Request,
    current_user: User = Depends(require_admin)
):
    """Update a user - ADMIN ONLY"""
    users = load_users()

    if user_id not in users:
        raise HTTPException(status_code=404, detail="User not found")

    user_data = users[user_id]
    original_data = user_data.copy()

    # Update fields
    if user_update.name is not None:
        user_data["name"] = user_update.name
    if user_update.email is not None:
        # Check if new email already exists
        existing_user_id, existing_user_data = find_user_by_email(users, user_update.email)
        if existing_user_data and existing_user_id != user_id:
            raise HTTPException(status_code=400, detail="Email already exists")
        user_data["email"] = user_update.email
    if user_update.role is not None:
        user_data["role"] = user_update.role
    if user_update.hotel is not None:
        user_data["hotel"] = user_update.hotel
    if user_update.status is not None:
        user_data["status"] = user_update.status

    users[user_id] = user_data
    save_users(users)
    
    # Log user update
    changes = {}
    for key in ["name", "email", "role", "hotel", "status"]:
        if key in original_data and key in user_data:
            if original_data[key] != user_data[key]:
                changes[key] = {
                    "old": original_data[key],
                    "new": user_data[key]
                }
    
    log_audit_event("user_updated", user_id, {
        "updated_by": current_user.id,
        "changes": changes,
        "ip_address": get_client_ip(request)
    })

    return UserResponse(**user_data, id=user_id)

@router.delete("/{user_id}", response_model=StandardResponse)
async def delete_user(
    user_id: str,
    request: Request,
    current_user: User = Depends(require_admin)
):
    """Delete a user - ADMIN ONLY"""
    users = load_users()

    if user_id not in users:
        raise HTTPException(status_code=404, detail="User not found")

    # Prevent self-deletion
    if user_id == current_user.id:
        raise HTTPException(status_code=400, detail="Cannot delete your own account")

    user_data = users[user_id]
    del users[user_id]
    save_users(users)
    
    # Log user deletion
    log_audit_event("user_deleted", user_id, {
        "deleted_by": current_user.id,
        "deleted_user_email": user_data["email"],
        "ip_address": get_client_ip(request)
    })

    return StandardResponse(message="User deleted successfully")

@router.post("/{user_id}/reset-password", response_model=StandardResponse)
async def admin_reset_password(
    user_id: str,
    password_reset: PasswordReset,
    request: Request,
    current_user: User = Depends(require_admin)
):
    """Admin reset user password - ADMIN ONLY"""
    users = load_users()

    if user_id not in users:
        raise HTTPException(status_code=404, detail="User not found")

    user_data = users[user_id]
    user_data["password"] = hash_password(password_reset.password)
    user_data["failed_login_attempts"] = 0
    user_data["locked_until"] = None

    users[user_id] = user_data
    save_users(users)
    
    # Clear any existing login attempts for this user
    user_email = user_data["email"]
    if user_email in login_attempts:
        del login_attempts[user_email]
    if user_email in locked_accounts:
        del locked_accounts[user_email]
    
    # Log password reset
    log_audit_event("admin_password_reset", user_id, {
        "reset_by": current_user.id,
        "target_user_email": user_data["email"],
        "ip_address": get_client_ip(request)
    })

    return StandardResponse(message="Password reset successfully")

@router.post("/{user_id}/unlock", response_model=StandardResponse)
async def unlock_user_account(
    user_id: str,
    request: Request,
    current_user: User = Depends(require_admin)
):
    """Unlock a user account - ADMIN ONLY"""
    users = load_users()

    if user_id not in users:
        raise HTTPException(status_code=404, detail="User not found")

    user_data = users[user_id]
    user_data["failed_login_attempts"] = 0
    user_data["locked_until"] = None

    users[user_id] = user_data
    save_users(users)
    
    # Clear login attempts and locked status
    user_email = user_data["email"]
    if user_email in login_attempts:
        del login_attempts[user_email]
    if user_email in locked_accounts:
        del locked_accounts[user_email]
    
    # Log account unlock
    log_audit_event("account_unlocked", user_id, {
        "unlocked_by": current_user.id,
        "target_user_email": user_data["email"],
        "ip_address": get_client_ip(request)
    })

    return StandardResponse(message="Account unlocked successfully")

# Profile Management Routes (for authenticated users)
@router.get("/profile/me", response_model=UserResponse)
async def get_my_profile(current_user: User = Depends(get_current_user)):
    """Get current user's profile"""
    return UserResponse(**current_user.dict())

@router.put("/profile/me", response_model=UserResponse)
async def update_my_profile(
    profile_update: UserUpdate,
    request: Request,
    current_user: User = Depends(get_current_user)
):
    """Update current user's profile (limited fields)"""
    users = load_users()
    user_data = users[current_user.id]
    original_data = user_data.copy()

    # Users can only update certain fields
    allowed_fields = ["name"]
    
    changes = {}
    if profile_update.name is not None:
        user_data["name"] = profile_update.name
        if original_data["name"] != profile_update.name:
            changes["name"] = {
                "old": original_data["name"],
                "new": profile_update.name
            }

    # Email updates require admin approval or verification
    if profile_update.email is not None and profile_update.email != user_data["email"]:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Email updates must be requested through an administrator"
        )

    # Role and hotel updates require admin privileges
    if profile_update.role is not None or profile_update.hotel is not None:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Role and hotel updates require administrator privileges"
        )

    users[current_user.id] = user_data
    save_users(users)
    
    # Log profile update
    if changes:
        log_audit_event("profile_updated", current_user.id, {
            "changes": changes,
            "ip_address": get_client_ip(request)
        })

    return UserResponse(**user_data, id=current_user.id)

@router.post("/profile/change-password", response_model=StandardResponse)
async def change_my_password(
    current_password: str,
    new_password: str,
    request: Request,
    current_user: User = Depends(get_current_user)
):
    """Change current user's password"""
    users = load_users()
    user_data = users[current_user.id]

    # Verify current password
    if not verify_password(current_password, user_data["password"]):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Current password is incorrect"
        )

    # Validate new password strength
    try:
        # Use the same validation as PasswordReset model
        if len(new_password) < MIN_PASSWORD_LENGTH:
            raise ValueError(f'Password must be at least {MIN_PASSWORD_LENGTH} characters long')
        
        if REQUIRE_UPPERCASE and not re.search(r'[A-Z]', new_password):
            raise ValueError('Password must contain at least one uppercase letter')
        
        if REQUIRE_LOWERCASE and not re.search(r'[a-z]', new_password):
            raise ValueError('Password must contain at least one lowercase letter')
        
        if REQUIRE_NUMBERS and not re.search(r'\d', new_password):
            raise ValueError('Password must contain at least one number')
        
        if REQUIRE_SPECIAL_CHARS and not re.search(r'[!@#$%^&*(),.?":{}|<>]', new_password):
            raise ValueError('Password must contain at least one special character')
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )

    # Update password
    user_data["password"] = hash_password(new_password)
    users[current_user.id] = user_data
    save_users(users)
    
    # Log password change
    log_audit_event("password_changed", current_user.id, {
        "ip_address": get_client_ip(request)
    })

    return StandardResponse(message="Password changed successfully")

# Utility Routes
@router.get("/audit-logs", response_model=List[Dict[str, Any]])
async def get_audit_logs(
    limit: int = 100,
    event_type: Optional[str] = None,
    user_id: Optional[str] = None,
    current_user: User = Depends(require_admin)
):
    """Get audit logs - ADMIN ONLY"""
    try:
        obj = s3.get_object(Bucket=BUCKET_NAME, Key=AUDIT_LOG_KEY)
        logs = json.loads(obj["Body"].read().decode("utf-8"))
    except s3.exceptions.NoSuchKey:
        return []
    except Exception:
        return []

    # Apply filters
    filtered_logs = logs
    if event_type:
        filtered_logs = [log for log in filtered_logs if log.get("event_type") == event_type]
    if user_id:
        filtered_logs = [log for log in filtered_logs if log.get("user_id") == user_id]

    # Sort by timestamp (newest first) and limit
    filtered_logs.sort(key=lambda x: x.get("timestamp", ""), reverse=True)
    return filtered_logs[:limit]

@router.get("/stats", response_model=Dict[str, Any])
async def get_user_stats(current_user: User = Depends(require_admin)):
    """Get user statistics - ADMIN ONLY"""
    users = load_users()
    
    stats = {
        "total_users": len(users),
        "active_users": sum(1 for user in users.values() if user["status"] == "Active"),
        "inactive_users": sum(1 for user in users.values() if user["status"] != "Active"),
        "users_by_role": {},
        "users_by_hotel": {},
        "users_with_recent_login": 0
    }
    
    # Count by role and hotel
    for user_data in users.values():
        role = user_data.get("role", "Unknown")
        hotel = user_data.get("hotel", "Unknown")
        
        stats["users_by_role"][role] = stats["users_by_role"].get(role, 0) + 1
        stats["users_by_hotel"][hotel] = stats["users_by_hotel"].get(hotel, 0) + 1
        
        # Check for recent login (last 30 days)
        if user_data.get("last_login"):
            try:
                last_login = datetime.fromisoformat(user_data["last_login"])
                if (datetime.utcnow() - last_login).days <= 30:
                    stats["users_with_recent_login"] += 1
            except:
                pass
    
    return stats

@router.get("/health")
async def health_check():
    """Health check endpoint"""
    try:
        # Test S3 connectivity
        s3.head_bucket(Bucket=BUCKET_NAME)
        return {
            "status": "healthy",
            "timestamp": datetime.utcnow().isoformat(),
            "services": {
                "s3": "connected",
                "authentication": "operational"
            }
        }
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Service unhealthy: {str(e)}"
        )
