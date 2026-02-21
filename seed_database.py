import os
import math
import pandas as pd
from dotenv import load_dotenv
from supabase import create_client, Client
from pydantic import BaseModel, EmailStr, Field, field_validator, AliasChoices
from typing import Optional, Dict, Any

load_dotenv()

# Service role key required to bypass RLS for seeding global data
supabase: Client = create_client(os.environ.get("SUPABASE_URL"), os.environ.get("SUPABASE_SERVICE_KEY"))

# --- 1. PYDANTIC VALIDATION MODEL ---
class ContactCreate(BaseModel):
    # This automatically maps "Company" or "organization" from Excel/Apollo to company_name
    email: EmailStr = Field(validation_alias=AliasChoices('Email', 'email', 'work_email'))
    first_name: Optional[str] = Field(default=None, validation_alias=AliasChoices('First name', 'first_name'))
    last_name: Optional[str] = Field(default=None, validation_alias=AliasChoices('Last name', 'last_name'))
    company_name: Optional[str] = Field(default=None, validation_alias=AliasChoices('Company name', 'Company', 'organization'))
    linkedin_url: Optional[str] = Field(default=None, validation_alias=AliasChoices('LinkedIn', 'linkedin_url'))
    
    # Global data has no owner
    owner_id: Optional[str] = None 
    custom_data: Dict[str, Any] = Field(default_factory=dict)

    @field_validator('first_name', 'last_name', 'company_name', mode='before')
    def clean_strings(cls, v):
        if isinstance(v, float) and math.isnan(v):
            return None
        return str(v).strip() if v else None

# --- 2. INGESTION ENGINE ---
def seed_database(file_path: str):
    print(f"Loading {file_path} into memory...")
    try:
        # Load all sheets at once
        sheets_dict = pd.read_excel(file_path, sheet_name=None)
    except Exception as e:
        print(f"Failed to read Excel: {e}")
        return

    for sheet_name, df in sheets_dict.items():
        print(f"\n--- Processing Sheet: '{sheet_name}' ({len(df)} rows) ---")
        
        # Clean column headers
        df.columns = df.columns.astype(str).str.strip()
        records_to_insert = []
        
        for _, row in df.iterrows():
            clean_dict = {
                k: str(v).strip() 
                for k, v in row.to_dict().items() 
                if pd.notna(v) and str(v).strip() != ""
            }

            if not clean_dict.get('Email') or '@' not in str(clean_dict.get('Email')):
                continue # Skip invalid emails
                
            try:
                # Let Pydantic extract core fields based on AliasChoices
                contact = ContactCreate(**clean_dict)
                
                # Identify remaining flexible fields for JSONB
                core_aliases = ['First name', 'Last name', 'Company name', 'Company', 'Email', 'LinkedIn']
                contact.custom_data = {k: v for k, v in clean_dict.items() if k not in core_aliases}
                
                # Tag the source sheet inside custom_data for filtering later
                contact.custom_data['original_sheet'] = sheet_name
                
                records_to_insert.append(contact.model_dump(by_alias=False))
            except Exception:
                pass # Skip rows that fail strict validation

        if not records_to_insert:
            continue

        # --- 3. BATCH UPLOAD (Fault Tolerant) ---
        chunk_size = 500
        for i in range(0, len(records_to_insert), chunk_size):
            chunk = records_to_insert[i:i + chunk_size]
            try:
                # Upsert prevents crashing on duplicates
                supabase.table('contacts').upsert(chunk, on_conflict='email').execute()
                print(f"  -> Uploaded {min(i + chunk_size, len(records_to_insert))}/{len(records_to_insert)} records...")
            except Exception as e:
                print(f"  -> Error on chunk {i}: {e}")

if __name__ == "__main__":
    seed_database("contact_data.xlsx")
