import os
import uuid
from sqlalchemy import create_engine, func, Numeric
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy import Column, String, DateTime, ForeignKey, UUID
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv

# Updated import for SQLAlchemy 2.0
Base = declarative_base()

# Load environment variables
load_dotenv()

# Define your models
class ApiSource(Base):
    __tablename__ = 'api_sources'
    
    uuid = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4, nullable=False)
    name = Column(String, nullable=False)
    url = Column(String, nullable=False)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=True)

class RawMaterial(Base):
    __tablename__ = 'raw_materials'
    
    uuid = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4, nullable=False)
    raw_material_name = Column(String, nullable=True)
    slug = Column(String, nullable=True)
    api_source = Column(UUID(as_uuid=True), ForeignKey('api_sources.uuid', onupdate='CASCADE', ondelete='SET NULL'), nullable=True)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=True)

class RawMaterialPrice(Base):
    __tablename__ = 'raw_material_prices'
    
    uuid = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4, nullable=False)
    raw_material_id = Column(UUID(as_uuid=True), ForeignKey('raw_materials.uuid', onupdate='CASCADE', ondelete='CASCADE'), nullable=False)
    price = Column(Numeric, nullable=False)
    price_in_usd = Column(Numeric, nullable=True)  # Newly added column
    unit = Column(String, nullable=True)
    recorded_at = Column(DateTime, server_default=func.now(), nullable=False)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=True)

# Create the database connection string (replace with your actual database URL)
DATABASE_URL = os.getenv("SUPABASE_DATABASE_URL")

# Initialize SQLAlchemy engine and session
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)
session = Session()

# Fetch API source with name 'Commodities API'
def get_commodities_api_source():
    return session.query(ApiSource).filter(ApiSource.name == 'Metals API').first()

# Fetch raw materials associated with 'Commodities API'
def get_raw_materials(api_source_uuid):
    return session.query(RawMaterial).filter(RawMaterial.api_source == api_source_uuid).all()

# Fetch raw material prices for the given raw materials
def get_raw_material_prices(raw_material_ids):
    return session.query(RawMaterialPrice).filter(RawMaterialPrice.raw_material_id.in_(raw_material_ids)).all()

# Update price_in_usd for raw material prices
def update_price_in_usd():
    try:
        # Fetch the 'Commodities API' source UUID
        api_source = get_commodities_api_source()
        
        if not api_source:
            print("No 'Commodities API' source found!")
            return

        # Fetch raw materials for the given 'Commodities API' source
        raw_materials = get_raw_materials(api_source.uuid)
       
        # Check if raw materials were found
        if not raw_materials:
            print("No raw materials found for 'Commodities API'!")
            return

        # for raw_material in raw_materials:
        #     print(f"UUID: {raw_material.uuid}, Name: {raw_material.raw_material_name}")


        # Fetch raw material prices for the raw materials fetched above
        raw_material_ids = [material.uuid for material in raw_materials]
        raw_material_prices = get_raw_material_prices(raw_material_ids)


       
            

        if not raw_material_prices:
            print("No prices found for the selected raw materials!")
            return
      
        # Process each raw material price
        for price_record in raw_material_prices:
            # Assuming you want to divide the price by a specific value (e.g., 1/price of raw material)
           

            if price_record.price:
                # Print raw material name and price
                print(f"Price Record UUID: {price_record.uuid}, Raw Material ID: {price_record.raw_material_id}, Price: {price_record.price}, Unit: {price_record.unit}")
                price_in_usd = 1 / price_record.price  # Example conversion, change the logic as needed
                price_record.price_in_usd = price_in_usd
                session.add(price_record)  # Mark for update

        # Commit the changes to the database
        session.commit()
        print(f"Updated price_in_usd for {len(raw_material_prices)} raw material prices.")

    except SQLAlchemyError as e:
        session.rollback()
        print(f"Error during database operation: {e}")
    finally:
        session.close()

if __name__ == "__main__":
    update_price_in_usd()
