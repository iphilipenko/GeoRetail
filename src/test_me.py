import requests

# Ваш токен з попереднього логіну
token = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxIiwidXNlcm5hbWUiOiJhZG1pbiIsImVtYWlsIjoiYWRtaW5AZ2VvcmV0YWlsLmNvbSIsInJvbGVzIjpbImFkbWluIl0sImlzX3N1cGVydXNlciI6dHJ1ZSwiZXhwIjoxNzU3NTE0OTQ1LCJpYXQiOjE3NTc1MTEzNDV9.CnGMkh0hKDZN03y0IfXtQiceFTtWyZFJOc-fYY-fz-Q"

# Запит до /me з токеном
headers = {"Authorization": f"Bearer {token}"}
response = requests.get("http://localhost:8000/api/v2/auth/me", headers=headers)

print(f"Status: {response.status_code}")
if response.status_code == 200:
    import json
    print(json.dumps(response.json(), indent=2))
else:
    print(f"Error: {response.text}")