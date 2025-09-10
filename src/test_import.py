print("1. Testing sqlalchemy import...")
try:
    import sqlalchemy
    print(f"   ✅ SQLAlchemy version: {sqlalchemy.__version__}")
except ImportError as e:
    print(f"   ❌ Error: {e}")

print("\n2. Testing from sqlalchemy.orm import Session...")
try:
    from sqlalchemy.orm import Session
    print("   ✅ Session imported")
except ImportError as e:
    print(f"   ❌ Error: {e}")

print("\n3. Testing auth_endpoints import...")
try:
    from api.endpoints.auth_endpoints import router
    print("   ✅ auth_endpoints imported")
except ImportError as e:
    print(f"   ❌ Error: {e}")