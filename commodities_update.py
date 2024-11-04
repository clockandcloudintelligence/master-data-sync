from sqlalchemy import create_engine, Column, String
from sqlalchemy.dialects.postgresql import UUID  
from sqlalchemy.orm import sessionmaker, declarative_base
import requests
import uuid  
import os
from dotenv import load_dotenv

load_dotenv()

# Database setup
SUPABASE_DATABASE_URL = os.getenv("SUPABASE_DATABASE_URL")
engine = create_engine(SUPABASE_DATABASE_URL)
Session = sessionmaker(bind=engine)
session = Session()
Base = declarative_base()

# ORM models for raw_materials and api_source
class RawMaterial(Base):
    __tablename__ = 'raw_materials'
    
    uuid = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    raw_material_name = Column(String)
    api_source = Column(UUID(as_uuid=True))
    symbol = Column(String)

class ApiSource(Base):
    __tablename__ = 'api_sources'
    
    uuid = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name = Column(String)
    url = Column(String)

# Fetch symbols from the Commodities API
def fetch_commodities_symbols(api_url):
    response = requests.get(api_url)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception("Failed to fetch symbols from Commodities API")

# Update raw materials with matching symbols and UUID from the database
def update_raw_materials_with_symbols():
    # Fetch commodities API details from the api_source table
    commodities_api = session.query(ApiSource).filter_by(name="Commodities API").first()
    
    if not commodities_api:
        print("Commodities API not found in the database.")
        return

    # Extract the API UUID and URL
    commodities_api_uuid = commodities_api.uuid
    commodities_api_url = commodities_api.url + '/api/symbols?access_key=' + os.getenv("COMMODITIES_API_KEY")
    
    # Fetch symbols from the Commodities API using the retrieved URL
    commodities_symbols = fetch_commodities_symbols(commodities_api_url)
    
    # Fetch all raw materials from the database
    raw_materials = session.query(RawMaterial).all()
    
    for raw_material in raw_materials:
        # Check if raw material's name exists in the commodities symbols
        matching_symbol = None
        for symbol, name in commodities_symbols.items():
           
            if raw_material.raw_material_name.lower() == name.lower():
                matching_symbol = symbol
                break

        if matching_symbol:
            # Set the commodities API UUID if it's not already set
            if raw_material.api_source is None:
                raw_material.api_source = commodities_api_uuid
    
            # Update the symbol field with the matching symbol
            raw_material.symbol = matching_symbol

            # Add the raw material to the session to commit changes later
            session.add(raw_material)
            print(f"Updated raw material '{raw_material.raw_material_name}' with symbol '{matching_symbol}' and API UUID '{commodities_api_uuid}'")

    # Commit all changes to the database
    session.commit()
    print("Raw materials updated successfully.")

# Run the update function
if __name__ == "__main__":
    update_raw_materials_with_symbols()