"""
A script to update raw materials in the database with symbols from an external API.

Classes:
    RawMaterial: Represents a raw material in the database.
    ApiSource: Represents an API source configuration.

Functions:
    fetch_symbols(api_url): Fetches symbols from the specified API URL.
    update_raw_materials_with_symbols(): Updates raw materials with matching symbols from the API.

Usage:
    python3 commodities_update.py <API Source Name>
"""

import os
import sys
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy import Column, String, or_
import requests
import uuid

# Load environment variables
load_dotenv()

# Database setup
SUPABASE_DATABASE_URL = os.getenv("SUPABASE_DATABASE_URL")
engine = create_engine(SUPABASE_DATABASE_URL)
Session = sessionmaker(bind=engine)
session = Session()
Base = declarative_base()

# ORM models for raw_materials and api_source
class RawMaterial(Base):
    """
    Represents a raw material in the database.

    Attributes:
        uuid (UUID): The unique identifier for the raw material. :no-index:
        raw_material_name (str): The name of the raw material. :no-index:
        api_source (UUID): The identifier of the API source. :no-index:
        symbol (str): The symbol associated with the raw material. :no-index:
    """
    __tablename__ = 'raw_materials'
    uuid = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    raw_material_name = Column(String)
    api_source = Column(UUID(as_uuid=True))
    symbol = Column(String)

class ApiSource(Base):
    """
    Represents an API source configuration in the database.

    Attributes:
        uuid (UUID): The unique identifier for the API source. :no-index:
        name (str): The name of the API source. :no-index:
        url (str): The base URL for the API source. :no-index:
    """
    __tablename__ = 'api_sources'
    uuid = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name = Column(String)
    url = Column(String)

# Command-line argument validation
if len(sys.argv) != 2:
    print("Usage: python3 commodities_update.py <API Source Name>")
    sys.exit(1)

# Fetching API source name from command-line arguments
api_source_name = sys.argv[1]

# Fetch specific API source from the database
api_source = session.query(ApiSource).filter_by(name=api_source_name).first()

# Verify if the API source exists in the database
if not api_source:
    print(f"API source '{api_source_name}' not found in the database.")
    sys.exit(1)

# Determine the API key based on the API source name
if api_source_name.lower() == "commodities api":
    api_key = os.getenv("COMMODITIES_API_KEY")
elif api_source_name.lower() == "metal api":
    api_key = os.getenv("METAL_API_KEY")
else:
    print(f"Unknown API source: {api_source_name}")
    sys.exit(1)

# Construct the API URL with the API key
api_url = f"{api_source.url}/api/symbols?access_key={api_key}"
print(f"Using API source: {api_source.name}")
print(f"URL: {api_url}")

# Function to fetch symbols from the specified API
def fetch_symbols(api_url):
    """
    Fetch symbols from the specified API URL.

    Args:
        api_url (str): The URL to fetch symbols from.

    Returns:
        dict: A dictionary of symbols and their associated names.

    Raises:
        Exception: If the API request fails or returns a non-200 status code.
    """
    response = requests.get(api_url)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception("Failed to fetch symbols from API")

# Fetch raw materials with existing symbols only
raw_materials = session.query(RawMaterial).filter(
    or_(RawMaterial.symbol == '', RawMaterial.symbol.is_(None))
).all()

# Update raw materials with matching symbols
def update_raw_materials_with_symbols():
    """
    Updates raw materials in the database with matching symbols from the API.

    This function fetches symbols from the API, matches them with raw materials
    in the database, and updates the database with the corresponding symbol and
    API source information.

    Steps:
        1. Fetch symbols from the API.
        2. Match symbols with raw materials by name.
        3. Update raw materials with matching symbols and save changes.

    Raises:
        Exception: If there are issues with fetching or updating data.
    """
    symbols_data = fetch_symbols(api_url)
   
    # Check if the API response format is Metals API format (nested dictionary)
    if any(isinstance(value, dict) and 'name' in value for value in symbols_data.values()):
        # Metals API format: { "SYMBOL": { "id": "SYMBOL", "name": "Name" } }
        # Use the 'name' field for comparison
        symbols = {key: value['name'] for key, value in symbols_data.items() if 'name' in value}
    else:
        # Commodities API format: { "SYMBOL": "Name" }
        symbols = symbols_data

    print("Raw materials updated successfully.", symbols.items())

    for raw_material in raw_materials:
        matching_symbol = None

        # Iterate over symbols to find a match
        for symbol, name_value in symbols.items():
            # Check if name_value matches the raw material's name
            if isinstance(name_value, str) and raw_material.raw_material_name.lower() == name_value.lower():
                matching_symbol = symbol
                break

        if matching_symbol:
            # Update the API source ID if not already set
            if raw_material.api_source is None:
                raw_material.api_source = api_source.uuid
            raw_material.symbol = matching_symbol
            session.add(raw_material)
            print(f"Updated raw material '{raw_material.raw_material_name}' with symbol '{matching_symbol}'")

    session.commit()
    print("Raw materials updated successfully.")

# Run the update function
if __name__ == "__main__":
    update_raw_materials_with_symbols()