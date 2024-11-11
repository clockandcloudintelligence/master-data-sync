import os
import pandas as pd
from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv()

def insert_data_into_table(supabase: Client, table_name: str, data_frame: pd.DataFrame, unique_columns: list = None):
    """
    Insert data into a specified Supabase table, checking for duplicates based on unique columns.
    
    Args:
        supabase (Client): The Supabase client instance.
        table_name (str): The name of the Supabase table to insert data into.
        data_frame (pd.DataFrame): The DataFrame containing data to insert.
        unique_columns (list): The list of column names to check for uniqueness before insertion.
    
    Returns:
        None
    """
    relevant_data = data_frame.dropna(how='all').drop_duplicates()
    
    # Loop through each row in the cleaned DataFrame
    for index, row in relevant_data.iterrows():
        cleaned_row_dict = {k: str(v).strip() for k, v in row.to_dict().items() if pd.notna(v) and v != ''}
        
        # If unique_columns are provided, check for duplicates based on those columns
        if unique_columns:
            # Build the query to check if the entry already exists based on the unique columns
            for column in unique_columns:
                if column not in cleaned_row_dict:
                    print(f"Skipping row due to missing required column: {column}")
                    continue

            # Check for duplicate entries based on the unique_columns
            query = supabase.table(table_name).select(*unique_columns)
            for column in unique_columns:
                query = query.eq(column, cleaned_row_dict[column])
            response = query.execute()

            # If duplicate found, skip the row
            if response.data and len(response.data) > 0:
                print(f"Skipping duplicate entry: {cleaned_row_dict} already exists.")
                continue  # Skip if duplicate found
        
        # If no duplicates, insert the data
        response = supabase.table(table_name).insert(cleaned_row_dict).execute()
        
        if response.data is None:  # If data is None, there was an error
            print(f"Error inserting into {table_name}: {response.status_code} - {response.error}")
        else:
            print(f"Successfully inserted {cleaned_row_dict} into {table_name}.")

def retrieve_ids(supabase: Client, table_name: str, name_column: str) -> dict:
    """
    Retrieve IDs from a specified Supabase table based on a name column.

    Args:
        supabase (Client): The Supabase client instance.
        table_name (str): The name of the Supabase table to query.
        name_column (str): The column name to use for matching names.

    Returns:
        dict: A dictionary mapping names to UUIDs.
    """
    response = supabase.table(table_name).select('uuid, {}'.format(name_column)).execute()
    return {row[name_column]: row['uuid'] for row in response.data}

def main():
    """
    Main function to execute the data import process from CSV files to Supabase.

    This function loads raw materials and countries data from specified CSV files,
    processes the data, and inserts it into the appropriate Supabase tables. The
    function performs the following operations:
    
    - Loads raw material data and country data from CSV files.
    - Cleans and filters the data.
    - Inserts data into the Supabase tables: applications, raw_materials, industries,
      industries_applications, raw_materials_applications, countries, and countries_raw_materials.

    Args:
        None

    Returns:
        None
    """
    # Load the CSV file
    raw_material_file_path = 'raw_material.csv'
    countries_file_path = 'raw_material_country.csv'

    # Supabase configuration
    SUPABASE_URL = os.getenv("SUPABASE_URL")
    SUPABASE_KEY = os.getenv("SUPABASE_KEY")

    # Create a Supabase client
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

    # Load the CSV files
    data = pd.read_csv(raw_material_file_path)
    countries_data = pd.read_csv(countries_file_path)

    # Filter and clean data for raw materials
    filtered_data = data[['raw_material_name', 'application1_name', 'application1_percentage', 'application1_rank', 'industry']]
    filtered_data = filtered_data.rename(columns={
        'raw_material_name': 'raw_material_name',  # Matches raw_materials table
        'application1_name': 'application_name',    # Matches applications table
        'application1_percentage': 'application_percentage',  # To be stored as string in junction table
        'application1_rank': 'application_rank',    # Junction table
        'industry': 'industry_name'                  # Matches industries table
    })

    applications_data = filtered_data[['application_name']].drop_duplicates(subset=['application_name'])
    insert_data_into_table(supabase, 'applications', applications_data, unique_columns=['application_name'])
   
    # Insert into the 'raw_materials' table (only raw_material_name)
    raw_materials_data = filtered_data[['raw_material_name']].drop_duplicates(subset=['raw_material_name'])
    insert_data_into_table(supabase, 'raw_materials', raw_materials_data)

    # Prepare data for the industries table with splitting
    industries_data = set()  # Use a set to automatically handle duplicates

    for _, row in filtered_data.iterrows():
        if pd.notna(row['industry_name']) and row['industry_name'].strip() != "":
            # Split and process each industry name
            for industry in row['industry_name'].split('/'):
                industry = industry.strip()  # Trim whitespace
                if industry:  # Only add non-empty industry names
                    industries_data.add(industry)

    # Convert set to a DataFrame, as expected by insert_data_into_table
    industries_df = pd.DataFrame({'industry_name': list(industries_data)})

    # Insert into the 'industries' table, ensuring uniqueness on 'industry_name'
    insert_data_into_table(supabase, 'industries', industries_df, unique_columns=['industry_name'])

    # Get the mappings of names to IDs
    applications_ids = retrieve_ids(supabase, 'applications', 'application_name')
    raw_materials_ids = retrieve_ids(supabase, 'raw_materials', 'raw_material_name')
    industry_ids = retrieve_ids(supabase, 'industries', 'industry_name')

    # Prepare data for the junction table industries_applications with correct IDs
    industries_applications_data = []

        # Iterate through each row in the filtered data
    for index, row in filtered_data.iterrows():
        application_id = applications_ids.get(row['application_name'])
        industry_names = row['industry_name']  # Get the industry name field (may contain multiple industries)

        # Check if industry_name is not NaN or empty
        if pd.notna(industry_names) and isinstance(industry_names, str):
            # Split industry names by '/' and strip whitespace
            for industry_name in industry_names.split('/'):
                industry_name = industry_name.strip()  # Remove extra whitespace around industry name
                
                # Get the industry_id for each industry
                industry_id = industry_ids.get(industry_name)
                
                # Append to the list if both IDs are valid
                if industry_id and application_id:
                    industries_applications_data.append({
                        'industry_id': industry_id,
                        'application_id': application_id
                    })
                else:
                    print(f"Skipping invalid industry or application. Industry: {industry_name}, Application: {row['application_name']}")
        else:
            # Print row index and application_name if industry_name is invalid
            print(f"Skipping row {index} with empty or NaN industry_name. Application name: {row['application_name']}")

    # Insert into the industries_applications junction table
    if industries_applications_data:
        industries_applications_df = pd.DataFrame(industries_applications_data)
        insert_data_into_table(supabase, 'industries_applications', industries_applications_df, unique_columns=['industry_id', 'application_id'])
        
    # Prepare and insert into the raw_materials_applications table
    raw_materials_applications_data = []

    # Iterate over the filtered data
    for index, row in filtered_data.iterrows():
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
        else:
            # Print row index and raw material/application name if IDs are invalid
            print(f"Skipping row {index} due to missing raw_material_id or application_id.")

    # Insert into the raw_materials_applications table
    if raw_materials_applications_data:
        raw_materials_applications_df = pd.DataFrame(raw_materials_applications_data)
        insert_data_into_table(supabase, 'raw_materials_applications', raw_materials_applications_df, unique_columns=['raw_material_id', 'application_id'])

    # Load country data
    filtered_countries_data = countries_data[['country_name', 'raw_material_name', 'production_percentage']]

    # Insert into the 'countries' table (only country_name)
    countries_unique_data = filtered_countries_data[['country_name']].drop_duplicates()
    insert_data_into_table(supabase, 'countries', countries_unique_data)

    # Retrieve mappings for countries and raw materials
    country_ids = retrieve_ids(supabase, 'countries', 'country_name')
    raw_material_ids = retrieve_ids(supabase, 'raw_materials', 'raw_material_name')

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
        insert_data_into_table(supabase, 'raw_materials_countries', countries_raw_materials_df)

    print("Data insertion process completed.")

if __name__ == "__main__":
    main()

# Only run this during explicit call
if os.getenv("RUN_IMPORT_SCRIPT") == "true":
    main()