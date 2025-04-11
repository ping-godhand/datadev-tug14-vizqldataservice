import _config as config
import requests
import json
import _config as config
import jwt
import uuid
import datetime

# Create JWT for authentication
client_id = config.connectedAppClientId
secret_id = config.connectedAppSecretId
secret_key = config.connectedAppSecretKey
user_email = config.user_email
site_name = config.site_name
pod = config.pod

now = datetime.datetime.now(datetime.UTC)
exp = now + datetime.timedelta(minutes=5)
jwt_payload = {
    "iss": client_id,
    "sub": user_email,
    "aud": "tableau",
    "iat": int(now.timestamp()),
    "exp": int(exp.timestamp()),
    "jti": str(uuid.uuid4()),
    "scp": ["tableau:viz_data_service:*"]
}

jwt_headers = {
    "kid": secret_id,
    "iss": client_id
}

jwt_token = jwt.encode(jwt_payload, secret_key, algorithm="HS256", headers=jwt_headers)

# Authenticate using the JWT
signin_endpoint = f"https://{pod}.online.tableau.com/api/3.26/auth/signin"
headers = {
    "Content-Type": "application/json",
    "Accept": "application/json"
}
payload = {
    "credentials": {
        "jwt": jwt_token,
        "site": {
            "contentUrl": site_name
        }
    }
}

res = requests.post(signin_endpoint, headers=headers, json=payload)
if res.status_code != 200:
    print("\n❌ Sign-in Failed")
    print(f"JWT Payload: {jwt_payload}")
    print(f"JWT Token Length: {len(str(jwt_token))}")
    exit(1)

res_json = res.json()
auth_token = res_json['credentials']['token']
site_id = res_json['credentials']['site']['id']
print("✅ Sign-in Successful")
print(f"X-Tableau-Auth: {auth_token}")
print(f"Site ID: {site_id}")

headers['X-tableau-auth'] = auth_token

datasource_id = config.datasource_id