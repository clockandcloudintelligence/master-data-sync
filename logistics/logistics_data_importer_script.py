import pandas as pd
from sqlalchemy import create_engine, Table, Column, String, Float, Numeric, MetaData, insert, select
from sqlalchemy.dialects.postgresql import UUID
import uuid
import os
from dotenv import load_dotenv
import logging
logging.basicConfig(level=logging.DEBUG)
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from slugify import slugify

# Load environment variables
load_dotenv()

# Supabase Database Configuration
SUPABASE_DATABASE_URL = os.getenv("SUPABASE_DATABASE_URL")
# Create SQLAlchemy engine
engine = create_engine(SUPABASE_DATABASE_URL)
metadata = MetaData()

# Define the tables
choke_points = Table(
    "choke_points", metadata,
    Column("uuid", UUID(as_uuid=True), primary_key=True, default=uuid.uuid4),
    Column("chokepoint_name", String, unique=True),
)

cargo_types = Table(
    "cargo_types", metadata,
   Column("uuid", UUID(as_uuid=True), primary_key=True, default=uuid.uuid4),
    Column("cargo_type_name", String, unique=True)
)

junction_table = Table(
    "choke_points_cargo_types", metadata,
    Column("uuid", PG_UUID(as_uuid=True), primary_key=True, default=uuid.uuid4),  
    Column("cargo_type_id", PG_UUID(as_uuid=True)),
    Column("choke_point_id", PG_UUID(as_uuid=True)),
    Column("avg_annual_transit_calls", String),
    Column("est_vessel_count_by_cargo", String),
    Column("vessel_composition_pct", String)
)

# Define the routes table
routes = Table(
    "routes", metadata,
    Column("uuid", UUID(as_uuid=True), primary_key=True, default=uuid.uuid4),
    Column("route_name", String),
    Column("slug", String),
    Column("category", String),  
    Column("node_1", String),    
    Column("node_2", String),    
    Column("created_at", String),
    Column("updated_at", String),
    Column("route", String)     
)

# Define the routes_choke_points junction table
routes_choke_points = Table(
    "routes_choke_points", metadata,
    Column("uuid", UUID(as_uuid=True), primary_key=True, default=uuid.uuid4),
    Column("route_id",  UUID(as_uuid=True)),
    Column("choke_point_id", UUID(as_uuid=True)),
    Column("created_at", String),
    Column("updated_at", String)
)

# Load CSV data
def load_data_from_csv(file_path):
    """Load data from a CSV file and drop completely empty rows."""
    return pd.read_csv(file_path).dropna(how="all")

def insert_data_into_table(table, data_frame, unique_columns):
    with engine.connect() as connection:
        with connection.begin():  # Ensures commit happens at the end
            for _, row in data_frame.iterrows():
                row_data = {k: str(v).strip() for k, v in row.items() if pd.notna(v)}
                print(f"Preparing to insert data: {row_data}")  # Debugging output

                query = select(*[table.c[column] for column in unique_columns])
                for column in unique_columns:
                    query = query.where(table.c[column] == row_data[column])

                existing = connection.execute(query).fetchone()

                if existing:
                    print(f"Duplicate found for: {row_data}. Skipping insert.")
                else:
                    print(f"No duplicate found. Inserting: {row_data}")
                    
                    # Insert data
                    try:
                        insert_result = connection.execute(insert(table).values(**row_data))
                        print(f"Insert SQL: {insert_result}")
                        if insert_result.rowcount > 0:
                            print(f"Insert successful for {row_data}")
                        else:
                            print(f"Insert failed for {row_data}. No rows affected.")
                    except Exception as e:
                        print(f"Error inserting data: {e}")

# Insert data into choke_points table
def insert_choke_points(data):
    # Strip spaces around chokepoint_name to avoid duplicate entries with trailing spaces
    data["primary_chokepoints"] = data["primary_chokepoints"].str.strip()

    # Map CSV columns to database table columns
    data = data.rename(columns={
        "primary_chokepoints": "chokepoint_name",
    })
    
    insert_data_into_table(choke_points, data, ["chokepoint_name"])

# Insert data into cargo_types table
def insert_cargo_types(data):
    # Map CSV columns to database table columns
    data = data.rename(columns={
        "vessel_composition_cargo_type": "cargo_type_name"
    })
    insert_data_into_table(cargo_types, data, ["cargo_type_name"])

def insert_junction_table(data):
    with engine.connect() as connection:
        # Begin a transaction explicitly
        with connection.begin():
            for _, row in data.iterrows():
                # Strip whitespace from CSV data
                chokepoint_name = row["primary_chokepoints"].strip() if isinstance(row["primary_chokepoints"], str) else row["primary_chokepoints"]
                cargo_type_name = row["vessel_composition_cargo_type"].strip() if isinstance(row["vessel_composition_cargo_type"], str) else row["vessel_composition_cargo_type"]

                # Fetch the UUIDs of the related foreign keys from choke_points and cargo_types tables
                chokepoint = connection.execute(
                    select(choke_points.c.uuid)
                    .where(choke_points.c.chokepoint_name == chokepoint_name)
                ).fetchone()

                cargo_type = connection.execute(
                    select(cargo_types.c.uuid)
                    .where(cargo_types.c.cargo_type_name == cargo_type_name)
                ).fetchone()

                if not chokepoint:
                    print(f"Chokepoint '{chokepoint_name}' not found in choke_points table")
                    continue

                if not cargo_type:
                    print(f"Cargo type '{cargo_type_name}' not found in cargo_types table")
                    continue

                # Convert UUIDs to string format (so psycopg2 can handle them)
                cargo_type_uuid_str = str(cargo_type[0])  # Convert to string format
                chokepoint_uuid_str = str(chokepoint[0])  # Convert to string format

                # Prepare the data for insertion, including converted UUIDs as strings
                junction_row = {
                    "cargo_type_id": cargo_type_uuid_str,  # Foreign key UUID (as string)
                    "choke_point_id": chokepoint_uuid_str,  # Foreign key UUID (as string)
                    "avg_annual_transit_calls": str(row["average_annual_number_of_transit_calls"]).strip() if row["average_annual_number_of_transit_calls"] else None,
                    "est_vessel_count_by_cargo": str(row["estimated_vessel_numbers_by_cargo_type"]).strip() if row["estimated_vessel_numbers_by_cargo_type"] else None,
                    "vessel_composition_pct": str(row["vessel_composition_%"]).strip() if row["vessel_composition_%"] else None
                }

                # Debugging: Print the data to ensure it looks correct
                print(f"Preparing to insert data: {junction_row}")

                try:
                    # Insert the data into the junction table without the 'uuid' field
                    connection.execute(insert(junction_table).values(junction_row))
                    print(f"Insert successful for: {junction_row}")
                except Exception as e:
                    print(f"Error inserting data: {e}")

            # Commit the transaction explicitly if no exceptions
            connection.commit()

    # Insert data into routes table
def insert_routes(data):
    # Map CSV columns to database table columns
    data = data.rename(columns={
        "route_name": "route_name",
        "importance": "category",
        "market1": "node_1",
        "market2": "node_2"
    })

    # Add a 'slug' column by applying slugify on 'route_name'
    data["slug"] = data["route_name"].apply(lambda x: slugify(x) if pd.notna(x) else None)

    # Insert data using the insert_data_into_table function
    insert_data_into_table(routes, data, ["slug"])

def insert_route_chokepoints(route_name, data):
    with engine.connect() as connection:
        # Start a transaction to ensure all operations are atomic
        with connection.begin():
            # Fetch route_id from routes table using the route_name
            result = connection.execute(
                select(routes.c.uuid).where(routes.c.route_name == route_name)
            ).fetchone()

            if not result:
                print(f"Route '{route_name}' not found in routes table.")
                return

            # Access route_id from the result tuple (UUID object)
            route_id = result[0]  # result[0] corresponds to 'route_id'

            # Ensure route_id is a string for insertion
            route_id_str = str(route_id)  # Convert route_id to string for insertion

            # Process each chokepoint from chokepoint1 to chokepoint10
            for i in range(1, 11):  # chokepoint1 to chokepoint10
                chokepoint_column = f"chokepoint{i}"
                chokepoint_name = data.get(chokepoint_column)

                if not chokepoint_name or chokepoint_name == '-' or chokepoint_name.isspace():
                    print(f"Skipping invalid or empty chokepoint '{chokepoint_name}'.")
                    continue  # Skip invalid or empty chokepoints

                # Fetch chokepoint_id from choke_points table using chokepoint_name
                chokepoint_result = connection.execute(
                    select(choke_points.c.uuid)
                    .where(choke_points.c.chokepoint_name == chokepoint_name)
                ).fetchone()

                if chokepoint_result:
                    # Access chokepoint_id from the result tuple (UUID object)
                    chokepoint_id = chokepoint_result[0]  # chokepoint_id is the first column in the result

                    # Ensure chokepoint_id is a string for insertion
                    chokepoint_id_str = str(chokepoint_id)  # Convert chokepoint_id to string for insertion

                    # Prepare the data for insertion into routes_choke_points junction table
                    junction_data = {
                        'route_id': route_id_str,  # route_id as string
                        'choke_point_id': chokepoint_id_str  # chokepoint_id as string
                    }

                    try:
                        # Insert the data into the junction table
                        connection.execute(insert(routes_choke_points).values(junction_data))
                        print(f"Inserted chokepoint '{chokepoint_name}' for route '{route_name}'.")
                    except exc.IntegrityError as e:
                        print(f"Integrity error inserting data for chokepoint '{chokepoint_name}': {e}")
                    except Exception as e:
                        print(f"Error inserting data for chokepoint '{chokepoint_name}': {e}")
                else:
                    print(f"Chokepoint '{chokepoint_name}' not found in choke_points table.")

def process_single_route_row(row):
    route_name = row['route_name']
    insert_route_chokepoints(route_name, row)

# Main processing logic
def process_csv(csv_file):
    data = pd.read_csv(csv_file)

    # Iterate over the rows and process each row
    for _, row in data.iterrows():
        process_single_route_row(row)

# Main function
def main():
    # File path for the single CSV file
    single_csv = "cargo_type_choke_points.csv"

    # Load data
    data = load_data_from_csv(single_csv)

    # Extract unique choke_points data
    choke_points_data = data[
        ["primary_chokepoints"]
    ].drop_duplicates()

    # Extract unique cargo_types data
    cargo_types_data = data[
        ["vessel_composition_cargo_type"]
    ].drop_duplicates()

    # Extract junction table data
    junction_data = data[
        [
            "primary_chokepoints",
            "vessel_composition_cargo_type",
            "average_annual_number_of_transit_calls",
            "estimated_vessel_numbers_by_cargo_type",
            "vessel_composition_%"
        ]
    ]
    insert_choke_points(choke_points_data)
    insert_cargo_types(cargo_types_data)
    insert_junction_table(junction_data)


    new_csv = "route_choke_points_data.csv"

    # Load data
    data = load_data_from_csv(new_csv)

    # Extract routes data
    routes_data = data[
        ["route_name", "importance", "market1", "market2"]
    ].drop_duplicates()

    # Insert into routes table
    insert_routes(routes_data)

    # Insert into routes_choke_point junction table
    process_csv("route_choke_points_data.csv")
    

if __name__ == "__main__":
    main()