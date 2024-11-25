import requests
from supabase import create_client, Client
import uuid
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


# Strapi and Supabase configurations
STRAPI_API_KEY = os.getenv("STRAPI_API_KEY")
STRAPI_COUNTIRES_API_URL = os.getenv("STRAPI_COUNTIRES_API_URL")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

# Fetch countries from Strapi
def fetch_countries_from_strapi():
    try:
        headers = {"Authorization": f"Bearer {STRAPI_API_KEY}"}
        countries = []
        page = 1
        while True:
            params = {
                "pagination[page]": page,
                "pagination[pageSize]": 100,
                "fields": ["Name"],
            }
            response = requests.get(STRAPI_COUNTIRES_API_URL, headers=headers, params=params)
            response.raise_for_status()
            data = response.json()
            page_countries = [item["attributes"]["Name"] for item in data.get("data", [])]
            
            if not page_countries:
                break

            countries.extend(page_countries)
            page += 1

        return countries
    except Exception as e:
        print(f"Error fetching countries from Strapi: {e}")
        return []

# Fetch countries from Supabase
def fetch_countries_from_supabase(supabase: Client):
    try:
        response = supabase.table("countries").select("country_name").execute()
        # Supabase Python client doesn't return an `error` attribute; directly handle the response.
        return [item["country_name"] for item in response.data]
    except Exception as e:
        print(f"Error fetching countries from Supabase: {e}")
        return []

# Add missing countries to Supabase
def add_missing_countries_to_supabase(supabase: Client, missing_countries):
    try:
        for country_name in missing_countries:
            # Check if the country already exists (to prevent duplicates)
            existing_country = supabase.table("countries").select("country_name").eq("country_name", country_name).execute()
            if existing_country.data:  # If data exists, skip insertion
                print(f"Country '{country_name}' already exists in Supabase. Skipping.")
                continue

            # Prepare data for insertion
            new_country = {
                "country_name": country_name,
            }

            # Insert the new country into the Supabase table
            response = supabase.table("countries").insert(new_country).execute()
            if response.data:  # Check if the response contains data
                print(f"Added country '{country_name}' to Supabase.")
            else:  # Handle cases where insertion did not succeed
                print(f"Failed to add country '{country_name}' to Supabase. No data in response.")
    except Exception as e:
        print(f"Error during Supabase insertion: {e}")

# Main script execution
def main():
    # Initialize Supabase client
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    
    # Fetch countries from both sources
    strapi_countries = fetch_countries_from_strapi()
    supabase_countries = fetch_countries_from_supabase(supabase)
    
    if not strapi_countries or not supabase_countries:
        print("Failed to fetch data from one or both sources.")
        return

    # Compare countries
    missing_in_supabase, missing_in_strapi = compare_countries(strapi_countries, supabase_countries)

    # Output results
    print("\nCountries missing in Supabase:")
    for country in missing_in_supabase:
        print(f"- {country}")

    # Add missing countries to Supabase
    if missing_in_supabase:
        print("\nAdding missing countries to Supabase...")
        add_missing_countries_to_supabase(supabase, missing_in_supabase)
    else:
        print("\nNo countries missing in Supabase.")

    print("\nCountries missing in Strapi:")
    for country in missing_in_strapi:
        print(f"- {country}")

# Compare countries and identify mismatches
def compare_countries(strapi_countries, supabase_countries):
    # Countries in Strapi but not in Supabase
    missing_in_supabase = set(strapi_countries) - set(supabase_countries)
    # Countries in Supabase but not in Strapi
    missing_in_strapi = set(supabase_countries) - set(strapi_countries)
    return missing_in_supabase, missing_in_strapi

if __name__ == "__main__":
    main()