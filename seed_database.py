import os
import pandas as pd
import math
from dotenv import load_dotenv
from supabase import create_client, Client
from pydantic import BaseModel, EmailStr, Field, field_validator
from typing import Optional, Dict, Any

load_dotenv()

# Initialize Supabase
url: str = os.environ.get("SUPABASE_URL")
key: str = os.environ.get("SUPABASE_SERVICE_KEY") # Use Service Role Key to bypass RLS for seeding
supabase: Client = create_client(url, key)

# --- 1. PYDANTIC VALIDATION MODEL ---
class GlobalContactCreate(BaseModel):
    email: EmailStr
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    company_name: Optional[str] = None
    source: str = "asian_b2b_excel"
    custom_data: Dict[str, Any] = Field(default_factory=dict)

    @field_validator('first_name', 'last_name', 'company_name', mode='before')
    def clean_strings(cls, v):
        # Handle NaN values from Pandas
        if isinstance(v, float) and math.isnan(v):
            return None
        return str(v).strip() if v else None

# --- 2. THE INGESTION ENGINE ---
def process_and_upload_csv(file_path: str):
    print(f"Processing: {file_path}...")
    
    try:
        df = pd.read_csv(file_path)
    except Exception as e:
        print(f"Failed to read {file_path}: {e}")
        return

    # Clean the dataframe columns (strip whitespace)
    df.columns = df.columns.str.strip()
    
    records_to_insert = []
    
    for _, row in df.iterrows():
        row_dict = row.to_dict()
        
        # Check if email exists and is valid string
        raw_email = row_dict.get('Email')
        if not isinstance(raw_email, str) or '@' not in raw_email:
            continue # Skip rows without valid emails
            
        # Separate Core Fields vs Flexible Fields
        core_first = row_dict.pop('First name', None)
        core_last = row_dict.pop('Last name', None)
        core_company = row_dict.pop('Company name', None)
        core_email = row_dict.pop('Email', None)
        
        # Clean up remaining dictionary for JSONB (remove NaNs)
        flexible_fields = {
            k: str(v).strip() 
            for k, v in row_dict.items() 
            if pd.notna(v) and str(v).strip() != ""
        }

        try:
            # Validate through Pydantic
            contact = GlobalContactCreate(
                email=core_email,
                first_name=core_first,
                last_name=core_last,
                company_name=core_company,
                custom_data=flexible_fields
            )
            records_to_insert.append(contact.model_dump())
        except Exception as e:
            # Pydantic caught a bad record (e.g., malformed email)
            # print(f"Validation failed for {core_email}: {e}")
            pass

    # --- 3. BATCH UPLOAD TO SUPABASE ---
    # Uploading in chunks of 500 to respect API limits
    chunk_size = 500
    total_inserted = 0
    
    for i in range(0, len(records_to_insert), chunk_size):
        chunk = records_to_insert[i:i + chunk_size]
        try:
            # Upsert prevents crashing on duplicate emails
            response = supabase.table('global_contacts').upsert(
                chunk, on_conflict='email'
            ).execute()
            total_inserted += len(response.data)
            print(f"Uploaded {total_inserted}/{len(records_to_insert)} records...")
        except Exception as e:
            print(f"Supabase upload error on chunk {i}: {e}")

    print(f"Finished {file_path}! Successfully stored {total_inserted} high-quality contacts.\n")

if __name__ == "__main__":
    # Add your .env file with SUPABASE_URL and SUPABASE_SERVICE_KEY
    csv_files = [
        "contact_data.xlsx - Owners.csv",
        "Asian_B2B.xlsx - Founder.csv"
        # Add the rest of your files here
    ]
    
    for file in csv_files:
        if os.path.exists(file):
            process_and_upload_csv(file)
        else:
            print(f"File not found: {file}")
