import os
from dotenv import load_dotenv
from supabase import create_client, Client

load_dotenv()
supabase: Client = create_client(os.environ.get("SUPABASE_URL"), os.environ.get("SUPABASE_SERVICE_KEY"))

def create_test_tenant():
    print("Creating test user in Supabase Auth...")
    email = "test.founder@feedbackos.com"
    password = "SuperSecurePassword123!"
    
    try:
        # 1. Create the user in the secure Supabase Auth system
        auth_response = supabase.auth.admin.create_user({
            "email": email,
            "password": password,
            "email_confirm": True
        })
        user_id = auth_response.user.id
        print(f"✅ Auth User created! ID: {user_id}")

        # 2. Register this user in our public.users table (The SaaS Tenant)
        supabase.table("users").insert({
            "id": user_id,
            "email": email,
            "plan": "pro_tier"
        }).execute()
        print(f"✅ Tenant registered in database!")
        print(f"\n🚀 COPY THIS USER ID FOR YOUR API TESTS:\n{user_id}\n")

    except Exception as e:
        print(f"Error (User might already exist): {e}")

if __name__ == "__main__":
    create_test_tenant()
