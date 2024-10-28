import pandas as pd
from supabase import create_client, Client

# Load the CSV file
raw_material_file_path = 'raw_material.csv'
countries_file_path = 'raw_material_country.csv'

# Supabase configuration
SUPABASE_URL = ''  # Replace with your Supabase URL
SUPABASE_KEY = ''  # Replace with your Supabase API key

# Create a Supabase client
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Load the CSV files
data = pd.read_csv('raw_material.csv')
countries_data = pd.read_csv('raw_material_country.csv')

# Filter and clean data for raw materials
filtered_data = data[['raw_material_name', 'application1_name', 'application1_percentage', 'application1_rank', 'industry']]
filtered_data = filtered_data.rename(columns={
    'raw_material_name': 'raw_material_name',  # Matches raw_materials table
    'application1_name': 'application_name',    # Matches applications table
    'application1_percentage': 'application_percentage',  # To be stored as string in junction table
    'application1_rank': 'application_rank',    # Junction table
    'industry': 'industry_name'                  # Matches industries table
})

# Function to insert data into the Supabase table
def insert_data_into_table(table_name, data_frame):
    relevant_data = data_frame.dropna(how='all').drop_duplicates()
    for index, row in relevant_data.iterrows():
        cleaned_row_dict = {k: str(v) for k, v in row.to_dict().items() if pd.notna(v) and v != ''}
        if cleaned_row_dict:  # Ensure only non-empty rows are inserted
            response = supabase.table(table_name).insert(cleaned_row_dict).execute()
            # Check for errors
            if response.data is None:  # If data is None, there was an error
                print(f"Error inserting into {table_name}: {response.status_code} - {response.error}")
        else:
            print(f"Skipping empty or invalid row in {table_name}")

# Insert into the 'applications' table (only application_name)
applications_data = filtered_data[['application_name']].drop_duplicates()
insert_data_into_table('applications', applications_data)

# Insert into the 'raw_materials' table (only raw_material_name)
raw_materials_data = filtered_data[['raw_material_name']].drop_duplicates()
insert_data_into_table('raw_materials', raw_materials_data)

# Prepare data for the industries table with splitting
industries_data = []
for _, row in filtered_data.iterrows():
    if pd.notna(row['industry_name']) and row['industry_name'].strip() != "":
        for industry in row['industry_name'].split('/'):
            industry = industry.strip()
            if industry:  # Only add non-empty industry names
                industries_data.append({'industry_name': industry})

industries_data_df = pd.DataFrame(industries_data).drop_duplicates()
insert_data_into_table('industries', industries_data_df)

# Retrieve IDs for mappings from Supabase tables
def retrieve_ids(table_name, name_column):
    response = supabase.table(table_name).select('uuid, {}'.format(name_column)).execute()
    return {row[name_column]: row['uuid'] for row in response.data}

# Get the mappings of names to IDs
applications_ids = retrieve_ids('applications', 'application_name')
raw_materials_ids = retrieve_ids('raw_materials', 'raw_material_name')
industry_ids = retrieve_ids('industries', 'industry_name')

# Prepare data for the junction table industries_applications with correct IDs
industries_applications_data = []

for index, row in filtered_data.iterrows():
    application_id = applications_ids.get(row['application_name'])
    industry_name = row['industry_name']
    
    # Check if industry_name is not NaN or empty
    if pd.notna(industry_name) and isinstance(industry_name, str):
        industry_id = industry_ids.get(industry_name.strip())
        
        # Append to the list if both IDs are valid
        if industry_id and application_id:
            industries_applications_data.append({
                'industry_id': industry_id,
                'application_id': application_id
            })
    else:
        # Print row index and application_name if industry_name is invalid
        print(f"Skipping row {index} with empty or NaN industry_name. Application name: {row['application_name']}")

# Insert into the industries_applications junction table
if industries_applications_data:
    industries_applications_df = pd.DataFrame(industries_applications_data)
    insert_data_into_table('industries_applications', industries_applications_df)

# Prepare and insert into the raw_materials_applications table
raw_materials_applications_data = []

# Iterate over the filtered data
for _, row in filtered_data.iterrows():
    # Retrieve IDs for raw material and application
    raw_material_id = raw_materials_ids.get(row['raw_material_name'])  # Get the raw material ID
    application_id = applications_ids.get(row['application_name'])     # Get the application ID

    # Check if application_id and raw_material_id are valid
    if raw_material_id and application_id:
        raw_materials_applications_data.append({
            'raw_material_id': raw_material_id,                        # Use the raw material ID
            'application_id': application_id,                          # Use the application ID
            'application_percentage': str(row['application_percentage']).strip(),  # Store percentage as string
        })

# Insert into the raw_materials_applications table
if raw_materials_applications_data:
    raw_materials_applications_df = pd.DataFrame(raw_materials_applications_data)
    insert_data_into_table('raw_materials_applications', raw_materials_applications_df)

# Load country data
filtered_countries_data = countries_data[['country_name', 'raw_material_name', 'production_percentage']]

# Insert into the 'countries' table (only country_name)
countries_unique_data = filtered_countries_data[['country_name']].drop_duplicates()
insert_data_into_table('countries', countries_unique_data) 

# Retrieve mappings for countries and raw materials
country_ids = retrieve_ids('countries', 'country_name')
raw_material_ids = retrieve_ids('raw_materials', 'raw_material_name')

# Prepare data for the countries_raw_materials junction table
countries_raw_materials_data = []
for _, row in filtered_countries_data.iterrows():
    country_id = country_ids.get(row['country_name'])
    raw_material_id = raw_material_ids.get(row['raw_material_name'])

    # Store production_percentage as a string
    production_percentage = str(row['production_percentage']).strip()  # Keep as string

    if country_id and raw_material_id:
        countries_raw_materials_data.append({
            'country_id': country_id,
            'raw_material_id': raw_material_id,
            'production_percentage': production_percentage  # Add production_percentage to the dictionary
        })

# Insert into the countries_raw_materials junction table
if countries_raw_materials_data:
    countries_raw_materials_df = pd.DataFrame(countries_raw_materials_data)
    insert_data_into_table('raw_materials_countries', countries_raw_materials_df)
print("Data insertion process completed.")
