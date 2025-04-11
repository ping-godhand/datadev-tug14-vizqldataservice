import pandas as pd
import requests
import json
import sys

# ===============================================
# SECTION 1: Configuration and Setup
# ===============================================

# Import authentication details from config file
import _include as config

# Set up connection parameters
headers = config.headers
site_id = config.site_id
pod = config.pod
datasource_id = config.datasource_id

# Define API endpoint
vizql_api = f'https://{pod}.online.tableau.com/api/v1/vizql-data-service'

# ===============================================
# SECTION 2: Fetch Metadata
# ===============================================

metadata_payload = {
    "datasource": {
        "datasourceLuid": datasource_id
    }
}

# Send metadata request
metadata_response = requests.post(
    f"{vizql_api}/read-metadata",
    headers=headers,
    json=metadata_payload
)

# Parse the JSON response
metadata_json = metadata_response.json()

# Extract relevant fields into a DataFrame
metadata_df = pd.DataFrame(metadata_json['data'])[['fieldName', 'fieldCaption', 'dataType']]

# ===============================================
# SECTION 3: Analyze Field Types
# ===============================================

print("\nAnalyzing field types...")
# Initialize lists for different field types
list_of_ids = []       # ID fields
list_of_measures = []  # Measure fields (typically numeric)
list_of_dimensions = [] # Dimension fields (typically categorical)
list_of_calcs = []     # Calculated fields

# Process each field and categorize it
for index, row in metadata_df.iterrows():
    field_name = row['fieldName']        
    field_caption = row['fieldCaption']
    data_type = row['dataType']
    
    # Categorize based on field name and data type
    if "ID" in field_caption:
        list_of_ids.append(field_caption)
    elif "Calculation_" in field_name:
        list_of_calcs.append(field_caption)            
    elif data_type == "REAL":
        list_of_measures.append(field_caption)
    else:
        list_of_dimensions.append(field_caption)

# ===============================================
# SECTION 4: Display Results
# ===============================================

print("\n============================================")
print("FIELD ANALYSIS RESULTS")
print("============================================")

# Print the detail lists
print("ID columns:", ", ".join(list_of_ids) if list_of_ids else "None found")
print("Measure columns:", ", ".join(list_of_measures) if list_of_measures else "None found")
print("Dimension columns:", ", ".join(list_of_dimensions) if list_of_dimensions else "None found")
print("Calculation columns:", ", ".join(list_of_calcs) if list_of_calcs else "None found")

