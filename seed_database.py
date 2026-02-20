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
key: str = os.environ.get("SUPABASE_SERVICE_KEY") # Service Role Key
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
        if isinstance(v, float) and math.isnan(v):
            return None
        return str(v).strip() if v else None

# --- 2. THE EXCEL INGESTION ENGINE ---
def process_and_upload_excel(file_path: str):
    print(f"Loading Excel file: {file_path} (This might take a few seconds)...")
    
    try:
        # sheet_name=None loads ALL sheets into a dictionary: {'Sheet1': df1, 'Sheet2': df2}
        sheets_dict = pd.read_excel(file_path, sheet_name=None)
    except Exception as e:
        print(f"Failed to read {file_path}. Error: {e}")
        return

    # Loop through every sheet in the Excel file
    for sheet_name, df in sheets_dict.items():
        print(f"\n--- Processing Sheet: '{sheet_name}' ({len(df)} rows) ---")
        
        # Clean the dataframe columns (strip whitespace)
        df.columns = df.columns.astype(str).str.strip()
        
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
                    source=f"excel_{sheet_name}", # Tag it with the sheet name!
                    custom_data=flexible_fields
                )
                records_to_insert.append(contact.model_dump())
            except Exception as e:
                # Pydantic caught a bad record
                pass

        if not records_to_insert:
            print(f"No valid records found in sheet '{sheet_name}'. Skipping.")
            continue

        # --- 3. BATCH UPLOAD TO SUPABASE ---
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
                print(f"  -> Uploaded {total_inserted}/{len(records_to_insert)} records...")
            except Exception as e:
                print(f"  -> Supabase upload error on chunk {i}: {e}")

        print(f"Finished sheet '{sheet_name}'! Stored {total_inserted} high-quality contacts.")

if __name__ == "__main__":
    # Point this to your actual Excel file
    excel_file = "Asian_B2B.xlsx" 
    
    if os.path.exists(excel_file):
        process_and_upload_excel(excel_file)
    else:
        print(f"File not found: {excel_file}. Please ensure it is in your Codespace directory.")
