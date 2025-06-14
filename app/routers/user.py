from fastapi import APIRouter, HTTPException, Request, Depends, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel, EmailStr, validator
from typing import Optional, List
import json
import boto3
import bcrypt
from jose import jwt
from datetime import datetime, timedelta
import uuid
import os

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
SECRET_KEY = os.getenv("SECRET_KEY", "your-secret-key-here-change-this")  # Use environment variable
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 30

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
    email: EmailStr
    new_password: str
    reset_token: Optional[str] = None
    
    @validator('email')
    def normalize_email(cls, v):
        return v.lower().strip()

# NEW: Simple password reset model for the reset endpoint
class SimplePasswordReset(BaseModel):
    password: str

class User(BaseModel):
    id: str
    name: str
    email: str
    role: str
    hotel: str
    status: str = "Active"
    created_at: str
    last_login: Optional[str] = None

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
    token_type: str
    user: UserResponse

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

def find_user_by_email(users: dict, email: str) -> tuple:
    """Find user by email (case-insensitive)"""
    normalized_email = email.lower().strip()
    for user_id, user_data in users.items():
        if user_data["email"].lower().strip() == normalized_email:
            return user_id, user_data
    return None, None

def hash_password(password: str) -> str:
    """Hash password using bcrypt"""
    return bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')

def verify_password(password: str, hashed: str) -> bool:
    """Verify password against hash"""
    return bcrypt.checkpw(password.encode('utf-8'), hashed.encode('utf-8'))

def create_access_token(data: dict, expires_delta: Optional[timedelta] = None):
    """Create JWT access token"""
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=15)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt

def verify_token(credentials: HTTPAuthorizationCredentials = Depends(security)):
    """Verify JWT token"""
    try:
        payload = jwt.decode(credentials.credentials, SECRET_KEY, algorithms=[ALGORITHM])
        email: str = payload.get("sub")
        if email is None:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Could not validate credentials",
                headers={"WWW-Authenticate": "Bearer"},
            )
        return email
    except jwt.PyJWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Could not validate credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )

def get_current_user(email: str = Depends(verify_token)):
    """Get current user from token"""
    users = load_users()
    # First try exact match (for backward compatibility)
    for user_id, user_data in users.items():
        if user_data["email"] == email:
            return User(**user_data, id=user_id)
    
    # If exact match fails, try case-insensitive match
    user_id, user_data = find_user_by_email(users, email)
    if user_data:
        return User(**user_data, id=user_id)
    
    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="User not found"
    )

# Authentication Routes
@router.post("/auth/login", response_model=TokenResponse)
async def login(user_login: UserLogin):
    """User login"""
    users = load_users()
    
    # Find user by email (case-insensitive)
    user_id, user_data = find_user_by_email(users, user_login.email)
    
    if not user_data or not verify_password(user_login.password, user_data["password"]):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect email or password"
        )
    
    if user_data["status"] == "Inactive":
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Account is inactive"
        )
    
    # Update last login
    user_data["last_login"] = datetime.now().isoformat()
    users[user_id] = user_data
    save_users(users)
    
    # Create access token
    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(
        data={"sub": user_data["email"]}, expires_delta=access_token_expires
    )
    
    user_response = UserResponse(**user_data, id=user_id)
    
    return TokenResponse(
        access_token=access_token,
        token_type="bearer",
        user=user_response
    )

@router.post("/auth/logout")
async def logout(current_user: User = Depends(get_current_user)):
    """User logout (client should delete token)"""
    return {"message": "Successfully logged out"}

# User Management Routes
@router.get("/", response_model=List[UserResponse])
async def get_users(
    role: Optional[str] = None,
    hotel: Optional[str] = None,
    status: Optional[str] = None,
    search: Optional[str] = None,
    current_user: User = Depends(get_current_user)
):
    """Get all users with optional filtering"""
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
async def get_user(user_id: str, current_user: User = Depends(get_current_user)):
    """Get a specific user"""
    users = load_users()
    
    if user_id not in users:
        raise HTTPException(status_code=404, detail="User not found")
    
    return UserResponse(**users[user_id], id=user_id)

@router.post("/", response_model=UserResponse)
async def create_user(user_create: UserCreate, current_user: User = Depends(get_current_user)):
    """Create a new user"""
    users = load_users()
    
    # Check if email already exists (case-insensitive)
    existing_user_id, existing_user_data = find_user_by_email(users, user_create.email)
    if existing_user_data:
        raise HTTPException(status_code=400, detail="Email already registered")
    
    # Create new user
    user_id = str(uuid.uuid4())
    hashed_password = hash_password(user_create.password)
    
    new_user = {
        "name": user_create.name,
        "email": user_create.email,  # Already normalized by validator
        "role": user_create.role,
        "hotel": user_create.hotel,
        "password": hashed_password,
        "status": "Active",
        "created_at": datetime.now().isoformat(),
        "last_login": None
    }
    
    users[user_id] = new_user
    save_users(users)
    
    return UserResponse(**new_user, id=user_id)

@router.put("/{user_id}", response_model=UserResponse)
async def update_user(
    user_id: str, 
    user_update: UserUpdate, 
    current_user: User = Depends(get_current_user)
):
    """Update a user"""
    users = load_users()
    
    if user_id not in users:
        raise HTTPException(status_code=404, detail="User not found")
    
    user_data = users[user_id]
    
    # Update fields
    if user_update.name is not None:
        user_data["name"] = user_update.name
    if user_update.email is not None:
        # Check if new email already exists (case-insensitive)
        existing_user_id, existing_user_data = find_user_by_email(users, user_update.email)
        if existing_user_data and existing_user_id != user_id:
            raise HTTPException(status_code=400, detail="Email already exists")
        user_data["email"] = user_update.email  # Already normalized by validator
    if user_update.role is not None:
        user_data["role"] = user_update.role
    if user_update.hotel is not None:
        user_data["hotel"] = user_update.hotel
    if user_update.status is not None:
        user_data["status"] = user_update.status
    
    users[user_id] = user_data
    save_users(users)
    
    return UserResponse(**user_data, id=user_id)

# FIXED: Actually delete the user permanently
@router.delete("/{user_id}")
async def delete_user(user_id: str, current_user: User = Depends(get_current_user)):
    """Permanently delete a user"""
    users = load_users()
    
    if user_id not in users:
        raise HTTPException(status_code=404, detail="User not found")
    
    # Actually delete the user from the dictionary
    del users[user_id]
    save_users(users)
    
    return {"message": "User deleted successfully"}

# FIXED: Simple password reset endpoint (changed back to POST to match frontend)
@router.post("/{user_id}/reset-password")
async def reset_password(
    user_id: str, 
    password_data: SimplePasswordReset,
    current_user: User = Depends(get_current_user)
):
    """Reset user password"""
    users = load_users()
    
    if user_id not in users:
        raise HTTPException(status_code=404, detail="User not found")
    
    # Hash new password
    hashed_password = hash_password(password_data.password)
    users[user_id]["password"] = hashed_password
    save_users(users)
    
    return {"message": "Password reset successfully"}

# FIXED: Add deactivate endpoint for those who want to deactivate instead of delete
@router.put("/{user_id}/deactivate")
async def deactivate_user(user_id: str, current_user: User = Depends(get_current_user)):
    """Deactivate a user"""
    users = load_users()
    
    if user_id not in users:
        raise HTTPException(status_code=404, detail="User not found")
    
    users[user_id]["status"] = "Inactive"
    save_users(users)
    
    return {"message": "User deactivated successfully"}

@router.post("/{user_id}/activate")
async def activate_user(user_id: str, current_user: User = Depends(get_current_user)):
    """Activate a user"""
    users = load_users()
    
    if user_id not in users:
        raise HTTPException(status_code=404, detail="User not found")
    
    users[user_id]["status"] = "Active"
    save_users(users)
    
    return {"message": "User activated successfully"}

@router.get("/stats/summary")
async def get_user_stats(current_user: User = Depends(get_current_user)):
    """Get user statistics"""
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

@router.get("/me")
async def get_me(current_user: User = Depends(get_current_user)):
    """Get current user profile"""
    return current_user
