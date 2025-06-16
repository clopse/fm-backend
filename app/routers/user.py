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
SECRET_KEY = os.getenv("SECRET_KEY", "your-secret-key-here-change-this")
REFRESH_SECRET_KEY = os.getenv("REFRESH_SECRET_KEY", "your-refresh-secret-key-here")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 15  # Short-lived access tokens
REFRESH_TOKEN_EXPIRE_DAYS = 7
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
    for key, new_value in user_data.items():
        if key in original_data and original_data[key] != new_value and key != "password":
            changes[key] = {"from": original_data[key], "to": new_value}
    
    log_audit_event("user_updated", user_id, {
        "updated_by": current_user.id,
        "changes": changes,
        "ip_address": get_client_ip(request)
    })

    return UserResponse(**user_data, id=user_id)

@router.delete("/{user_id}", response_model=StandardResponse)
async def delete_user(user_id: str, request: Request, current_user: User = Depends(require_admin)):
    """Permanently delete a user - ADMIN ONLY"""
    users = load_users()

    if user_id not in users:
        raise HTTPException(status_code=404, detail="User not found")
    
    # Store user info for logging before deletion
    deleted_user = users[user_id]

    # Actually delete the user from the dictionary
    del users[user_id]
    save_users(users)
    
    # Log user deletion
    log_audit_event("user_deleted", user_id, {
        "deleted_by": current_user.id,
        "deleted_user_email": deleted_user["email"],
        "deleted_user_role": deleted_user["role"],
        "ip_address": get_client_ip(request)
    })

    return StandardResponse(message="User deleted successfully")

@router.put("/{user_id}/reset-password", response_model=StandardResponse)
async def reset_password(
    user_id: str, 
    password_data: PasswordReset,
    request: Request,
    current_user: User = Depends(require_admin)
):
    """Reset user password - ADMIN ONLY"""
    users = load_users()

    if user_id not in users:
        raise HTTPException(status_code=404, detail="User not found")

    # Hash new password
    hashed_password = hash_password(password_data.password)
    users[user_id]["password"] = hashed_password
    users[user_id]["failed_login_attempts"] = 0
    users[user_id]["locked_until"] = None
    save_users(users)
    
    # Log password reset
    log_audit_event("password_reset", user_id, {
        "reset_by": current_user.id,
        "target_user_email": users[user_id]["email"],
        "ip_address": get_client_ip(request)
    })

    return StandardResponse(message="Password reset successfully")

@router.post("/{user_id}/activate", response_model=StandardResponse)
async def activate_user(user_id: str, request: Request, current_user: User = Depends(require_admin)):
    """Activate a user - ADMIN ONLY"""
    users = load_users()

    if user_id not in users:
        raise HTTPException(status_code=404, detail="User not found")

    users[user_id]["status"] = "Active"
    users[user_id]["failed_login_attempts"] = 0
    users[user_id]["locked_until"] = None
    save_users(users)
    
    # Log user activation
    log_audit_event("user_activated", user_id, {
        "activated_by": current_user.id,
        "user_email": users[user_id]["email"],
        "ip_address": get_client_ip(request)
    })

    return StandardResponse(message="User activated successfully")

@router.get("/stats/summary")
async def get_user_stats(current_user: User = Depends(require_manager_or_admin)):
    """Get user statistics - MANAGER/ADMIN ONLY"""
    users = load_users()

    total_users = len(users)
    active_users = sum(1 for user in users.values() if user["status"] == "Active")
    inactive_users = total_users - active_users

    # Count by role
    roles = {}
    for user in users.values():
        role = user["role"]
        roles[role] = roles.get(role, 0) + 1

    # Count by hotel
    hotels = {}
    for user in users.values():
        hotel = user["hotel"]
        hotels[hotel] = hotels.get(hotel, 0) + 1

    return {
        "total_users": total_users,
        "active_users": active_users,
        "inactive_users": inactive_users,
        "roles": roles,
        "hotels": hotels
    }

@router.get("/me", response_model=UserResponse)
async def get_me(current_user: User = Depends(get_current_user)):
    """Get current user profile"""
    return UserResponse(**current_user.dict())

# Audit log endpoint for super admins
@router.get("/audit/logs")
async def get_audit_logs(
    limit: int = 100,
    event_type: Optional[str] = None,
    current_user: User = Depends(require_admin)
):
    """Get audit logs - ADMIN ONLY"""
    try:
        obj = s3.get_object(Bucket=BUCKET_NAME, Key=AUDIT_LOG_KEY)
        logs = json.loads(obj["Body"].read().decode("utf-8"))
        
        # Filter by event type if specified
        if event_type:
            logs = [log for log in logs if log.get("event_type") == event_type]
        
        # Return most recent logs
        return logs[-limit:]
    except s3.exceptions.NoSuchKey:
        return []

# Add these new routes to your existing user.py router

from app.services.email_service import email_service, EmailTemplates, PasswordResetManager

# Initialize password reset manager
reset_manager = PasswordResetManager(s3, BUCKET_NAME)

# Add these new Pydantic models to your existing models:

class ForgotPasswordRequest(BaseModel):
    email: EmailStr
    
    @validator('email')
    def normalize_email(cls, v):
        return v.lower().strip()

class ResetPasswordRequest(BaseModel):
    token: str
    new_password: str
    
    @validator('new_password')
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

# Add these new routes to your router:

@router.post("/auth/forgot-password", response_model=StandardResponse)
async def forgot_password(request_data: ForgotPasswordRequest, request: Request):
    """Send password reset email"""
    users = load_users()
    user_id, user_data = find_user_by_email(users, request_data.email)
    
    # Always return success to prevent email enumeration attacks
    # But only send email if user exists
    if user_data and user_data.get("status") == "Active":
        try:
            # Create reset token
            reset_token = reset_manager.create_reset_token(user_id, expires_minutes=15)
            
            # Build reset URL
            frontend_url = os.getenv("FRONTEND_URL", "https://jmkfacilities.ie")
            reset_url = f"{frontend_url}/reset-password?token={reset_token}"
            
            # Generate email content
            html_content, text_content = EmailTemplates.password_reset_template(
                reset_link=reset_url,
                user_name=user_data["name"],
                expires_minutes=15
            )
            
            # Send email
            email_sent = await email_service.send_email(
                to_emails=[user_data["email"]],
                subject="Password Reset Request - JMK Facilities",
                html_content=html_content,
                text_content=text_content
            )
            
            if email_sent:
                # Log password reset request
                log_audit_event("password_reset_requested", user_id, {
                    "email": user_data["email"],
                    "ip_address": get_client_ip(request),
                    "reset_token_created": True
                })
            else:
                logger.error(f"Failed to send password reset email to {user_data['email']}")
                
        except Exception as e:
            logger.error(f"Error in forgot password process: {str(e)}")
    
    # Always return success message
    return StandardResponse(
        message="If your email address is registered, you will receive a password reset link shortly."
    )

@router.post("/auth/reset-password", response_model=StandardResponse)
async def reset_password_with_token(request_data: ResetPasswordRequest, request: Request):
    """Reset password using reset token"""
    
    # Validate and consume token
    user_id = reset_manager.consume_reset_token(request_data.token)
    
    if not user_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid or expired reset token"
        )
    
    # Load users and update password
    users = load_users()
    
    if user_id not in users:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found"
        )
    
    # Hash new password and update user
    hashed_password = hash_password(request_data.new_password)
    users[user_id]["password"] = hashed_password
    users[user_id]["failed_login_attempts"] = 0
    users[user_id]["locked_until"] = None
    
    save_users(users)
    
    # Log password reset completion
    log_audit_event("password_reset_completed", user_id, {
        "email": users[user_id]["email"],
        "ip_address": get_client_ip(request),
        "reset_via_token": True
    })
    
    return StandardResponse(message="Password reset successfully. You can now login with your new password.")

@router.post("/auth/send-welcome-email/{user_id}", response_model=StandardResponse)
async def send_welcome_email(user_id: str, current_user: User = Depends(require_admin)):
    """Send welcome email to new user - ADMIN ONLY"""
    users = load_users()
    
    if user_id not in users:
        raise HTTPException(status_code=404, detail="User not found")
    
    user_data = users[user_id]
    
    try:
        # Build login URL
        frontend_url = os.getenv("FRONTEND_URL", "https://jmkfacilities.ie")
        login_url = f"{frontend_url}/login"
        
        # Generate welcome email
        html_content, text_content = EmailTemplates.welcome_email_template(
            user_name=user_data["name"],
            user_email=user_data["email"],
            login_url=login_url
        )
        
        # Send email
        email_sent = await email_service.send_email(
            to_emails=[user_data["email"]],
            subject="Welcome to JMK Facilities Management System",
            html_content=html_content,
            text_content=text_content
        )
        
        if email_sent:
            log_audit_event("welcome_email_sent", user_id, {
                "sent_by": current_user.id,
                "recipient_email": user_data["email"]
            })
            return StandardResponse(message="Welcome email sent successfully")
        else:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to send welcome email"
            )
            
    except Exception as e:
        logger.error(f"Error sending welcome email: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to send welcome email"
        )

@router.get("/auth/verify-reset-token/{token}", response_model=StandardResponse)
async def verify_reset_token(token: str):
    """Verify if a reset token is valid (for frontend validation)"""
    user_id = reset_manager.validate_reset_token(token)
    
    if user_id:
        return StandardResponse(message="Token is valid")
    else:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid or expired token"
        )

# Enhanced user creation with optional welcome email
@router.post("/", response_model=UserResponse)
async def create_user_enhanced(
    user_create: UserCreate, 
    request: Request, 
    send_welcome_email: bool = False,
    current_user: User = Depends(require_admin)
):
    """Create a new user with optional welcome email - ADMIN ONLY"""
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
        "ip_address": get_client_ip(request),
        "welcome_email_requested": send_welcome_email
    })

    # Send welcome email if requested
    if send_welcome_email:
        try:
            frontend_url = os.getenv("FRONTEND_URL", "https://jmkfacilities.ie")
            login_url = f"{frontend_url}/login"
            
            html_content, text_content = EmailTemplates.welcome_email_template(
                user_name=new_user["name"],
                user_email=new_user["email"],
                login_url=login_url
            )
            
            await email_service.send_email(
                to_emails=[new_user["email"]],
                subject="Welcome to JMK Facilities Management System",
                html_content=html_content,
                text_content=text_content
            )
            
            log_audit_event("welcome_email_sent", user_id, {
                "sent_by": current_user.id,
                "recipient_email": new_user["email"],
                "sent_during_creation": True
            })
            
        except Exception as e:
            logger.error(f"Failed to send welcome email during user creation: {str(e)}")
            # Don't fail user creation if email fails

    return UserResponse(**new_user, id=user_id)
