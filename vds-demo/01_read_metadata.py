import pandas as pd
import requests
import json

# ===============================================
# SECTION 1: Configuration and Setup
# ===============================================

# Import authentication details from separate config file
import _include as config

headers = config.headers  # Authentication headers
site_id = config.site_id  # Your Tableau site ID
pod = config.pod          # prod-southeast-b
datasource_id = config.datasource_id  # The datasource to query

# ===============================================
# SECTION 2: Endpoint Construction
# ===============================================

# Define API endpoints
server_api = f'https://{pod}.online.tableau.com/api/3.26'
vizql_api = f'https://{pod}.online.tableau.com/api/v1/vizql-data-service'

# ===============================================
# SECTION 3: Prepare Request Payload
# ===============================================

# Create the request payload for metadata retrieval
metadata_payload = {
    "datasource": {
        "datasourceLuid": datasource_id
    }
}

print("============================================")
print("TABLEAU METADATA RETRIEVAL")
print("============================================")

# ===============================================
# SECTION 4: Make API Request
# ===============================================

# Send POST request to the metadata endpoint
metadata_response = requests.post(
    f"{vizql_api}/read-metadata",
    headers=headers,
    json=metadata_payload
)

# ===============================================
# SECTION 5: Process and Display Results
# ===============================================

# Parse the JSON response
metadata_json = metadata_response.json()

# Print the full response for learning purposes
print("\nParsed Metadata Response:")
print(json.dumps(metadata_json, indent=2))

# Extract relevant fields and create a DataFrame
if 'data' in metadata_json:
    # Convert to DataFrame for easier analysis
    metadata_df = pd.DataFrame(metadata_json['data'])

    # Select only the most important columns
    metadata_df = metadata_df[['fieldName', 'fieldCaption', 'dataType']]

    print("\nMetadata DataFrame:")
    print(metadata_df)
else:
    print("\nNo data found in metadata response")
    print("Check your authentication or datasource ID")
