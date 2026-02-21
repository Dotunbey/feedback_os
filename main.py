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
    # Core Fields
    q: Optional[str] = Query(None, description="Search across name, email, or company"),
    
    # Flexible JSONB Fields (Specific to your B2B data)
    industry: Optional[str] = Query(None, description="Filter by Industry in JSONB"),
    country: Optional[str] = Query(None, description="Filter by Company Country in JSONB"),
    
    # Pagination
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(50, ge=1, le=100, description="Items per page")
):
    """
    High-performance search endpoint hitting both relational and JSONB data using GIN indexes.
    """
    try:
        # 1. Base Query: Select all records and ask Postgres for the exact total count
        query = supabase.table("contacts").select("*", count="exact")
        
        # 2. Filter: Only search the Global directory (where owner_id is NULL)
        query = query.is_("owner_id", "null")

        # 3. Handle Core Field Search (The 'q' parameter)
        if q:
            # Uses PostgREST's 'or' syntax to search multiple core columns at once
            search_term = f"%{q}%"
            query = query.or_(f"first_name.ilike.{search_term},last_name.ilike.{search_term},company_name.ilike.{search_term},email.ilike.{search_term}")

        # 4. Handle JSONB Flexible Field Search (The Magic Trick)
        # We use the ->> operator to extract text from the JSONB column for querying
        if industry:
            query = query.ilike("custom_data->>Industry", f"%{industry}%")
        
        if country:
            query = query.ilike("custom_data->>Company Country", f"%{country}%")

        # 5. Apply Pagination
        start_idx = (page - 1) * page_size
        end_idx = start_idx + page_size - 1
        query = query.range(start_idx, end_idx)

        # 6. Execute Query
        response = query.execute()

        # 7. Calculate Pagination Metadata
        total_count = response.count if response.count else 0
        total_pages = math.ceil(total_count / page_size) if total_count > 0 else 0

        return PaginatedSearchResponse(
            data=response.data,
            total_count=total_count,
            page=page,
            page_size=page_size,
            total_pages=total_pages
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database query failed: {str(e)}")

# --- HEALTH CHECK ---
@app.get("/health")
async def health_check():
    return {"status": "ok", "message": "FeedbackOS Backend is running securely."}
