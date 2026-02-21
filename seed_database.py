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
        sheets_dict = pd.read_excel(file_path, sheet_name=None)
    except Exception as e:
        print(f"Failed to read Excel: {e}")
        return

    # 🚀 NEW: Fetch existing global emails to prevent duplicate crashes
    print("Fetching existing global contacts from database...")
    try:
        # We only need the emails where owner_id is NULL
        res = supabase.table('contacts').select('email').is_('owner_id', 'null').execute()
        seen_emails = {row['email'] for row in res.data}
        print(f"Found {len(seen_emails)} existing contacts.")
    except Exception as e:
        print(f"Warning: Could not fetch existing contacts: {e}")
        seen_emails = set()

    for sheet_name, df in sheets_dict.items():
        print(f"\n--- Processing Sheet: '{sheet_name}' ({len(df)} rows) ---")
        
        df.columns = df.columns.astype(str).str.strip()
        records_to_insert = []
        
        for _, row in df.iterrows():
            clean_dict = {
                k: str(v).strip() 
                for k, v in row.to_dict().items() 
                if pd.notna(v) and str(v).strip() != ""
            }

            raw_email = clean_dict.get('Email')
            # Check for valid email AND check if we've already seen it
            if not raw_email or '@' not in str(raw_email):
                continue
            
            clean_email = str(raw_email).lower()
            if clean_email in seen_emails:
                continue # Skip! We already have this email.
                
            try:
                contact = ContactCreate(**clean_dict)
                
                core_aliases = ['First name', 'Last name', 'Company name', 'Company', 'Email', 'LinkedIn']
                contact.custom_data = {k: v for k, v in clean_dict.items() if k not in core_aliases}
                contact.custom_data['original_sheet'] = sheet_name
                
                records_to_insert.append(contact.model_dump(by_alias=False))
                
                # 🚀 NEW: Add to our seen list so we don't duplicate within the same Excel file
                seen_emails.add(clean_email)
                
            except Exception:
                pass 

        if not records_to_insert:
            print("No new/valid records to insert in this sheet.")
            continue

        # --- 3. BATCH INSERT ---
        chunk_size = 500
        for i in range(0, len(records_to_insert), chunk_size):
            chunk = records_to_insert[i:i + chunk_size]
            try:
                # 🚀 CHANGED: Using .insert() instead of .upsert()
                supabase.table('contacts').insert(chunk).execute()
                print(f"  -> Uploaded {min(i + chunk_size, len(records_to_insert))}/{len(records_to_insert)} records...")
            except Exception as e:
                print(f"  -> Error on chunk {i}: {e}")

if __name__ == "__main__":
    seed_database("contact_data.xlsx") # Make sure the filename matches!
