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
# SECTION 3: Prepare Data Query Payload
# ===============================================

# Create the query payload with the fields we want to retrieve
query_payload = {
    "datasource": {
        "datasourceLuid": datasource_id
    },
    "query": {
        "fields": [
            {
                "fieldCaption": "Region"
            },
            {
                "fieldCaption": "Segment"
            },            
            {
                "fieldCaption": "SalesVAT",
                "function": "SUM",
                "maxDecimalPlaces": 2
            }
        ]
    },
    "options": {
        "returnFormat": "OBJECTS",
        "debug": True
    }
}

print("============================================")
print("TABLEAU DATA QUERY")
print("============================================")
print("\nQuery Payload:")
print(json.dumps(query_payload, indent=2))

# ===============================================
# SECTION 4: Execute Data Query
# ===============================================

print("\nQuerying VizQL Data Service...")

# Send POST request to the query endpoint
query_response = requests.post(
    f"{vizql_api}/query-datasource",
    headers=headers,
    json=query_payload
)

# ===============================================
# SECTION 5: Process and Display Results
# ===============================================

# Parse the JSON response
query_json = query_response.json()

print("\nResponse Status Code:", query_response.status_code)

# Print the full raw response (good for debugging)
print("\nRaw Response:")
print(json.dumps(query_json, indent=2))

# Process the results
if 'data' in query_json:
    # Convert to DataFrame for easier analysis and display
    results_df = pd.DataFrame(query_json['data'])

    print("\nResults as DataFrame:")
    print(results_df)

    # Optional: Save results to CSV
    # results_df.to_csv('tableau_query_results.csv', index=False)
    # print("\nResults saved to tableau_query_results.csv")
else:
    print("\nNo data found in query response")
    print("Check your query parameters or authentication")
