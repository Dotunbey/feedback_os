import os
import math
from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, ConfigDict
from typing import Optional, List, Any, Dict
from dotenv import load_dotenv
from supabase import create_client, Client

# Load environment variables
load_dotenv()

# Initialize FastAPI App
app = FastAPI(
    title="FeedbackOS API",
    description="Core backend for data querying and sequence management.",
    version="1.0.0"
)

# CORS configuration (Crucial for when your frontend talks to this backend)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], # In production, change this to your frontend URL
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize Supabase Client
supabase_url = os.environ.get("SUPABASE_URL")
supabase_key = os.environ.get("SUPABASE_SERVICE_KEY")
supabase: Client = create_client(supabase_url, supabase_key)

# --- PYDANTIC RESPONSE MODELS ---
# These define exactly how the data looks when it leaves our API

class ContactResponse(BaseModel):
    id: str
    email: str
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    company_name: Optional[str] = None
    custom_data: Dict[str, Any]
    
    model_config = ConfigDict(from_attributes=True)

class PaginatedSearchResponse(BaseModel):
    data: List[ContactResponse]
    total_count: int
    page: int
    page_size: int
    total_pages: int

# --- THE SEARCH ENDPOINT ---

@app.get("/api/v1/contacts/search", response_model=PaginatedSearchResponse)
async def search_global_contacts(
    q: Optional[str] = Query(None, description="Search across name, email, or company"),
    industry: Optional[str] = Query(None, description="Filter by Industry"),
    country: Optional[str] = Query(None, description="Filter by Company Country"),
    title: Optional[str] = Query(None, description="Filter by Job Title"),
    company_size: Optional[str] = Query(None, description="Filter by exact Company Size"),
    
    # NEW: Critical B2B Filter
    has_linkedin: Optional[bool] = Query(None, description="Only return contacts with a LinkedIn URL"),
    
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=100)
):
    try:
        query = supabase.table("contacts").select("*", count="exact").is_("owner_id", "null")

        if q:
            search_term = f"%{q}%"
            query = query.or_(f"first_name.ilike.{search_term},last_name.ilike.{search_term},company_name.ilike.{search_term},email.ilike.{search_term}")

        if industry: query = query.ilike("custom_data->>Industry", f"%{industry}%")
        if country: query = query.ilike("custom_data->>Company Country", f"%{country}%")
        if title: query = query.ilike("custom_data->>Title", f"%{title}%")
        if company_size: query = query.eq("custom_data->>Company Size", company_size)
        
        # NEW: LinkedIn Filter Logic
        if has_linkedin is True:
            query = query.not_.is_("linkedin_url", "null")
        elif has_linkedin is False:
            query = query.is_("linkedin_url", "null")

        start_idx = (page - 1) * page_size
        query = query.range(start_idx, start_idx + page_size - 1)

        response = query.execute()
        total_count = response.count if response.count else 0

        return PaginatedSearchResponse(
            data=response.data, total_count=total_count,
            page=page, page_size=page_size, 
            total_pages=math.ceil(total_count / page_size) if total_count > 0 else 0
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
# --- HEALTH CHECK ---
@app.get("/health")
async def health_check():
    return {"status": "ok", "message": "FeedbackOS Backend is running securely."}



# --- NEW PYDANTIC MODELS FOR SAVING ---

class SaveContactRequest(BaseModel):
    user_id: str # In production, we extract this securely from the JWT token
    contact_id: str

class SaveContactResponse(BaseModel):
    success: bool
    message: str
    workspace_contact_id: Optional[str] = None

# --- THE POST ENDPOINT ---

@app.post("/api/v1/workspaces/contacts", response_model=SaveContactResponse)
async def save_contact_to_workspace(payload: SaveContactRequest):
    """
    Saves a global contact into a specific user's private workspace.
    """
    try:
        # We attempt to insert the relationship into user_contacts
        response = supabase.table("user_contacts").insert({
            "user_id": payload.user_id,
            "contact_id": payload.contact_id
        }).execute()
        
        # If successful, get the newly created ID from the user_contacts table
        new_id = response.data[0]['id']

        return SaveContactResponse(
            success=True,
            message="Contact successfully saved to workspace.",
            workspace_contact_id=new_id
        )

    except Exception as e:
        error_msg = str(e)
        # Catch the specific PostgreSQL error for our UNIQUE constraint
        # This prevents the server from crashing if they click "Save" twice
        if "duplicate key value violates unique constraint" in error_msg:
            raise HTTPException(
                status_code=409, # 409 Conflict
                detail="This contact is already saved in your workspace."
            )
        
        # Handle foreign key errors (e.g., they sent a fake contact_id)
        if "violates foreign key constraint" in error_msg:
            raise HTTPException(
                status_code=400, # 400 Bad Request
                detail="Invalid user ID or contact ID provided."
            )

        # Catch-all for other database errors
        raise HTTPException(status_code=500, detail="Failed to save contact.")

# --- GET WORKSPACE CONTACTS ENDPOINT ---
class WorkspacePaginatedResponse(BaseModel):
    data: List[Dict[str, Any]]
    total_count: int
    page: int
    page_size: int
    total_pages: int

@app.get("/api/v1/workspaces/contacts", response_model=WorkspacePaginatedResponse)
async def get_workspace_contacts(
    # TEMP: In production, this comes from Depends(get_current_user_jwt)
    user_id: str = Query(..., description="The user ID"), 
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=100)
):
    try:
        # We add count="exact" to the relational query
        query = supabase.table("user_contacts") \
            .select("id, override_first_name, override_last_name, custom_data, created_at, contacts(*)", count="exact") \
            .eq("user_id", user_id) \
            .order("created_at", desc=True)

        # Apply Pagination
        start_idx = (page - 1) * page_size
        query = query.range(start_idx, start_idx + page_size - 1)

        response = query.execute()
        total_count = response.count if response.count else 0

        return WorkspacePaginatedResponse(
            data=response.data,
            total_count=total_count,
            page=page,
            page_size=page_size,
            total_pages=math.ceil(total_count / page_size) if total_count > 0 else 0
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))