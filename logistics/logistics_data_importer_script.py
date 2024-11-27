"""
Script Name: Logistics Data Importer

Description:
This script automates the process of importing and processing logistics-related data into the database. 
It handles the following types of data:
1. **Choke Points and Cargo Types**:
   - Extracts and processes choke points and associated cargo types from a CSV file.
   - Populates the database with choke point data, cargo type data, and their relationships.

2. **Routes and Choke Points**:
   - Processes routes and their associated choke points from another CSV file.
   - Populates the routes table and the junction table that links routes with choke points.

3. **Ports and Related Data**:
   - Processes port data, including their associations with countries and industries.
   - Populates the ports table, country-port-industry relationships, and other related tables.

Data Sources:
- `cargo_type_choke_points.csv`: Contains choke points and associated cargo type data.
- `route_choke_points_data.csv`: Contains route and choke point relationships.
- `ports_country.csv`: Contains port data and associated country/industry information.

Workflow:
1. Load data from CSV files.
2. Extract and deduplicate relevant data for each table or relationship.
3. Insert data into the respective database tables using predefined functions.

Functions Called:
- `insert_choke_points`: Inserts choke point data into the `choke_points` table.
- `insert_cargo_types`: Inserts cargo type data into the `cargo_types` table.
- `insert_choke_points_cargo_types`: Inserts relationships between choke points and cargo types.
- `insert_routes`: Inserts route data into the `routes` table.
- `process_csv_insert_route_choke_point`: Processes and inserts route-choke point relationships.
- `insert_ports`: Inserts port data into the `country_ports` table.
- `insert_countries_port_industries`: Inserts country-port-industry relationships.
- `insert_port_cargo_type`: Inserts port-cargo type relationships.
- `insert_ports_route_junction_table`: Inserts relationships between ports and routes.

Execution:
Run this script directly to load and process data from the specified CSV files into the database.
"""

import pandas as pd
from sqlalchemy import create_engine, Table, Column, String, Float, Numeric, MetaData, insert, select, exc
from sqlalchemy.dialects.postgresql import UUID
import uuid
import os
from dotenv import load_dotenv
import logging
logging.basicConfig(level=logging.DEBUG)
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from slugify import slugify
from sqlalchemy.dialects.postgresql import insert

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
    Column("latitude", Numeric),
    Column("longitude", Numeric),
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

ports = Table(
    "country_ports", metadata,
    Column("uuid", UUID(as_uuid=True), primary_key=True, default=uuid.uuid4),
    Column("port_name", String, nullable=False, unique=True),
    Column("created_at", String),
    Column("updated_at", String),
    Column("latitude", Numeric),
    Column("longitude", Numeric),
    Column("country_id", UUID(as_uuid=True)),
    Column("import_percentage_maritime_trade", String),
    Column("export_percentage_maritime_trade", String)
)

countries = Table(
    "countries", metadata,
    Column("uuid", UUID(as_uuid=True), primary_key=True, default=uuid.uuid4),
    Column("country_name", String, nullable=False, unique=True), 
    Column("created_at", String),  
    Column("updated_at", String),
    Column("isoalphathree", String),
    Column("geojson", String), 
)

industries = Table(
    "industries",
    metadata,
    Column("uuid", UUID, primary_key=True),
    Column("industry_name", String, nullable=True, unique=True),
    Column("created_at", String),
    Column("updated_at", String),
)

countries_port_industries = Table(
    "countries_port_industries",
    metadata,
    Column("uuid",UUID(as_uuid=True), primary_key=True, default=uuid.uuid4),
    Column("port_id", UUID(as_uuid=True)),
    Column("industry_id", UUID(as_uuid=True)),
    Column("created_at", String),
    Column("updated_at", String),
)

port_cargo_type = Table(
    "port_cargo_type",
    metadata,
    Column("uuid", UUID(as_uuid=True), primary_key=True, default=uuid.uuid4),
    Column("port_id", UUID(as_uuid=True)),  
    Column("cargo_type_id", UUID(as_uuid=True)),  
    Column("vessel_composition_pct", String),
    Column("created_at", String),
    Column("updated_at", String),
)

routes_ports = Table(
    "routes_ports",
    metadata,
    Column("uuid", UUID(as_uuid=True), primary_key=True, default=uuid.uuid4),
    Column("route_id", UUID(as_uuid=True)),  
    Column("port_id", UUID(as_uuid=True)),  
    Column("created_at", String),
    Column("updated_at", String),
)

# Load CSV data
def load_data_from_csv(file_path):
    """
    Load data from a CSV file and drop completely empty rows.

    :param file_path: str
        Path to the CSV file to load.
    :return: pandas.DataFrame
        DataFrame containing the data from the CSV file, with completely empty rows removed.
    """
    return pd.read_csv(file_path).dropna(how="all")

def insert_data_into_table(table, data_frame, unique_columns):
    """
    Insert data from a DataFrame into a database table, ensuring no duplicates based on unique columns.

    This function checks for duplicate rows in the database based on the provided unique columns.
    If a row already exists, it skips the insertion. Otherwise, it inserts the new data.

    :param table: sqlalchemy.Table
        The SQLAlchemy Table object representing the database table.
    :param data_frame: pandas.DataFrame
        The DataFrame containing the data to be inserted.
    :param unique_columns: list of str
        A list of column names used to determine uniqueness in the database.
    :return: None
    """
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
    """
    Insert chokepoint data into the `choke_points` table.

    Strips whitespace around chokepoint names to avoid duplicates and maps CSV columns 
    to database table columns before insertion.

    :param data: pandas.DataFrame
        DataFrame containing chokepoint data with the column `primary_chokepoints`.
    :return: None
    """
    # Strip spaces around chokepoint_name to avoid duplicate entries with trailing spaces
    data["primary_chokepoints"] = data["primary_chokepoints"].str.strip()
    data["latitude"] = pd.to_numeric(data["latitude"], errors="coerce")
    data["longitude"] = pd.to_numeric(data["longitude"], errors="coerce")

    # Drop rows where latitude or longitude are NaN after conversion
    data = data.dropna(subset=["latitude", "longitude"])

    # Map CSV columns to database table columns
    data = data.rename(columns={
        "primary_chokepoints": "chokepoint_name",
        "latitude": "latitude",
        "longitude": "longitude"
    })

    # Call insert function to handle data insertion
    insert_data_into_table(choke_points, data, ["chokepoint_name", "latitude", "longitude"])

# Insert data into cargo_types table
def insert_cargo_types(data):
    """
    Insert cargo type data into the `cargo_types` table.

    Maps CSV columns to database table columns and ensures no duplicates exist.

    :param data: pandas.DataFrame
        DataFrame containing cargo type data with the column `vessel_composition_cargo_type`.
    :return: None
    """
    # Map CSV columns to database table columns
    data = data.rename(columns={
        "vessel_composition_cargo_type": "cargo_type_name"
    })
    insert_data_into_table(cargo_types, data, ["cargo_type_name"])

def insert_choke_points_cargo_types(data):
    """
    Insert data into the junction table linking chokepoints and cargo types.

    Looks up foreign key IDs for chokepoints and cargo types and inserts the 
    corresponding data into the junction table, ensuring valid relationships.

    :param data: pandas.DataFrame
        DataFrame containing relationships between chokepoints and cargo types.
    :return: None
    """
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
    """
    Insert route data into the `routes` table.

    Maps CSV columns to database table columns, generates a slug for each route,
    and inserts the data while ensuring no duplicates.

    :param data: pandas.DataFrame
        DataFrame containing route data with columns `route_name`, `importance`, 
        `market1`, and `market2`.
    :return: None
    """
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
    """
    Link a route to its chokepoints in the `routes_choke_points` junction table.

    Looks up foreign key IDs for the route and its associated chokepoints and inserts 
    the relationships into the database.

    :param route_name: str
        The name of the route.
    :param data: dict
        Dictionary containing chokepoint information (columns `chokepoint1` to `chokepoint10`).
    :return: None
    """
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


def insert_ports(data):
    """
    Insert port data into the `ports` table.

    Maps port data from the CSV file, validates mandatory fields, and links ports
    to countries using foreign keys.

    :param data: pandas.DataFrame
        DataFrame containing port data with columns like `port_name`, `latitude`, 
        `longitude`, and `country`.
    :return: None
    """
    with engine.connect() as connection:
        # Start a transaction
        with connection.begin():
            for _, row in data.iterrows():
                # Fetch country_id using country_name
                country_name = row.get("country", "").strip()
                country = connection.execute(
                    select(countries.c.uuid).where(countries.c.country_name == country_name)
                ).fetchone()

                if not country:
                    print(f"Country '{country_name}' not found in countries table. Skipping.")
                    continue

                country_id = str(country[0])

                # Map CSV columns to database table columns
                port_data = {
                    'port_name': row.get('port_name', '').strip(),
                    'latitude': row.get('latitude', None),
                    'longitude': row.get('longitude', None),
                    'country_id': country_id,
                    'import_percentage_maritime_trade': row.get('import_percentage_countrys_maritime_trade', '').strip(),
                    'export_percentage_maritime_trade': row.get('export_percentage_countrys_maritime_trade', '').strip(),
                }

                # Validate mandatory fields
                if not port_data['port_name']:
                    print(f"Missing port_name. Skipping row: {row}")
                    continue

                # Insert into ports table
                try:
                    connection.execute(insert(ports).values(port_data))
                    print(f"Inserted port '{port_data['port_name']}' for country '{country_name}'.")
                except Exception as e:
                    print(f"Error inserting port '{port_data['port_name']}': {e}")

def insert_countries_port_industries(data_frame):
    """
    Insert data into the `countries_port_industries` junction table, mapping ports to their industries.

    This function ensures that industries exist in the `industries` table and creates entries in the
    `countries_port_industries` table to link ports with their corresponding industries.

    :param data_frame: pandas.DataFrame
        DataFrame containing the data to be processed. The DataFrame must include the following columns:
        - `port_name`: Name of the port to be linked.
        - `top1_industry`, `top2_industry`, `top3_industry`: Names of the industries associated with the port.

    :raises Exception:
        Handles database-related exceptions and prints error messages for each failure case.

    :return: None
    """
    with engine.connect() as connection:
        with connection.begin():  # Start transaction
            for _, row in data_frame.iterrows():
                # Get country name, port name, and industry columns
                port_name = row["port_name"].strip()
                industry_list = [
                    row["top1_industry"].strip(),
                    row["top2_industry"].strip(),
                    row["top3_industry"].strip(),
                ]

                # Fetch the UUID of the port
                port_result = connection.execute(
                    select(ports.c.uuid).where(ports.c.port_name == port_name)
                ).fetchone()

                if not port_result:
                    print(f"Port '{port_name}' not found. Skipping row.")
                    continue

                # Convert port UUID to string
                port_uuid = str(port_result[0])

                for industry_name in industry_list:
                    if not industry_name or industry_name == "-":  # Skip empty or invalid industries
                        continue

                    # Check if the industry exists
                    industry_result = connection.execute(
                        select(industries.c.uuid).where(industries.c.industry_name == industry_name)
                    ).fetchone()

                    # Insert the industry if it does not exist
                    if not industry_result:
                        try:
                            print(f"Industry '{industry_name}' not found. Adding to industries table.")
                            industry_uuid = str(uuid.uuid4())  # Generate a new UUID for the industry
                            connection.execute(
                                insert(industries).values(
                                    industry_name=industry_name
                                )
                            )
                        except exc.IntegrityError:
                            print(f"Failed to insert industry '{industry_name}'. Skipping.")
                            continue
                        except Exception as e:
                            print(f"Error inserting industry '{industry_name}': {e}")
                            continue
                    else:
                        # Use the existing industry's UUID
                        industry_uuid = str(industry_result[0])

                    # Insert into the junction table
                    try:
                        connection.execute(
                            insert(countries_port_industries).values(
                                port_id=port_uuid, 
                                industry_id=industry_uuid
                            )
                        )
                        print(f"Linked port '{port_name}' with industry '{industry_name}'.")
                    except exc.IntegrityError as e:
                        print(f"Duplicate entry for port '{port_name}' and industry '{industry_name}': {e}")
                    except Exception as e:
                        print(f"Error linking port '{port_name}' with industry '{industry_name}': {e}")

def insert_port_cargo_type(data_frame):
    """
    Insert data into the `port_cargo_type` junction table, mapping ports to their cargo types along with vessel composition percentages.

    :param data_frame: pandas.DataFrame
        DataFrame containing the data to be processed. The DataFrame must include the following columns:
        - `port_name`: Name of the port to be linked.
        - `annual_vessel_composition1` to `annual_vessel_composition5`: Names of cargo types associated with the port.
        - `%_of_total_ships1` to `%_of_total_ships5`: Corresponding vessel composition percentages.
    """
    with engine.connect() as connection:
        with connection.begin():
            for _, row in data_frame.iterrows():
                port_name = row["port_name"].strip()
                cargo_types_list = [
                    (row["annual_vessel_composition1"].strip(), row["%_of_total_ships1"]),
                    (row["annual_vessel_composition2"].strip(), row["%_of_total_ships2"]),
                    (row["annual_vessel_composition3"].strip(), row["%_of_total_ships3"]),
                    (row["annual_vessel_composition4"].strip(), row["%_of_total_ships4"]),
                    (row["annual_vessel_composition5"].strip(), row["%_of_total_ships5"]),
                ]

                # Fetch port UUID from country_ports
                port_result = connection.execute(
                    select(ports.c.uuid).where(ports.c.port_name == port_name)
                ).fetchone()

                if not port_result:
                    print(f"Port '{port_name}' not found in country_ports. Skipping row.")
                    continue

                port_uuid = str(port_result[0])

                for cargo_type_name, vessel_pct in cargo_types_list:
                    if not cargo_type_name or cargo_type_name == "-":
                        continue

                    # Fetch cargo type UUID from cargo_types
                    cargo_type_result = connection.execute(
                        select(cargo_types.c.uuid).where(cargo_types.c.cargo_type_name == cargo_type_name)
                    ).fetchone()

                    if not cargo_type_result:
                        # Insert the cargo type if it does not exist
                        try:
                            print(f"Cargo type '{cargo_type_name}' not found. Adding to cargo_types table.")
                            cargo_type_uuid = str(uuid.uuid4())
                            connection.execute(
                                insert(cargo_types).values(
                                    cargo_type_name=cargo_type_name,
                                )
                            )
                        except exc.IntegrityError:
                            print(f"Failed to insert cargo type '{cargo_type_name}'. Skipping.")
                            continue
                        except Exception as e:
                            print(f"Error inserting cargo type '{cargo_type_name}': {e}")
                            continue
                    else:
                        cargo_type_uuid = str(cargo_type_result[0])

                    # Insert into port_cargo_type table
                    try:
                        connection.execute(
                            insert(port_cargo_type).values(
                                port_id=port_uuid,
                                cargo_type_id=cargo_type_uuid,
                                vessel_composition_pct=str(vessel_pct).strip(),
                            )
                        )
                        print(f"Linked port '{port_name}' with cargo type '{cargo_type_name}' and vessel composition percentage '{vessel_pct}'.")
                    except exc.IntegrityError as e:
                        print(f"Duplicate entry for port '{port_name}' and cargo type '{cargo_type_name}': {e}")
                    except Exception as e:
                        print(f"Error linking port '{port_name}' with cargo type '{cargo_type_name}': {e}")

def process_single_route_row(row):
    """
    Process a single route row and insert associated chokepoints.

    This function processes a single row from the dataset, extracting the route name and passing it
    along with the row's data to the `insert_route_chokepoints` function for further processing.

    :param row: pandas.Series
        A single row of data representing a route. The row must include the following column:
        - `route_name`: Name of the route to be processed.

    :raises KeyError:
        If the `route_name` column is missing in the row.

    :return: None
    """
    route_name = row['route_name']
    insert_route_chokepoints(route_name, row)

# Main processing logic
def process_csv_insert_route_choke_point(csv_file):
    """
    Process a CSV file to extract and process route data.

    This function reads a CSV file into a pandas DataFrame and iterates through its rows, processing
    each route by calling `process_single_route_row`.

    :param csv_file: str
        The path to the CSV file containing route data. The CSV file should have a column named `route_name`.

    :raises FileNotFoundError:
        If the specified CSV file cannot be found.

    :raises pd.errors.EmptyDataError:
        If the CSV file is empty or cannot be parsed as a valid file.

    :raises KeyError:
        If the required columns (e.g., `route_name`) are missing in the CSV file.

    :return: None
    """
    data = pd.read_csv(csv_file)

    # Iterate over the rows and process each row
    for _, row in data.iterrows():
        process_single_route_row(row)

def insert_ports_route_junction_table(data_frame):
    """
    Insert data into the `routes_ports` junction table, mapping routes to their associated ports.
    
    This function ensures that routes and ports exist in their respective tables 
    and creates entries in the `routes_ports` table to link routes with their associated ports.
    
    :param data_frame: pandas.DataFrame
        DataFrame containing the data to be processed. The DataFrame must include:
        - `route_name`: The route name to be linked.
        - `ports`: A comma-separated list of port names associated with the route.
    
    :raises Exception:
        Handles database-related exceptions and prints error messages for each failure case.
    
    :return: None
    """
    with engine.connect() as connection:
        with connection.begin():  # Start transaction
            for _, row in data_frame.iterrows():
                route_name = row["route_name"]
                ports_list = row["ports"].split(",")  # Split ports by comma

                # Fetch route UUID from routes table
                route_result = connection.execute(
                    select(routes.c.uuid).where(routes.c.route_name == route_name)
                ).fetchone()

                if not route_result:
                    print(f"Route '{route_name}' not found in 'routes' table. Skipping row.")
                    continue

                route_uuid = str(route_result[0])  # Convert UUID to string

                for port_name in ports_list:
                    port_name = port_name.strip()  # Remove any leading/trailing spaces

                    # Fetch port UUID from country_ports table (now referred as 'ports')
                    port_result = connection.execute(
                        select(ports.c.uuid).where(ports.c.port_name == port_name)
                    ).fetchone()

                    if not port_result:
                        print(f"Port '{port_name}' not found in 'country_ports' table. Skipping port.")
                        continue

                    port_uuid = str(port_result[0])  # Convert UUID to string

                    # Prepare data for insertion into the junction table
                    new_entry = {
                       
                        "route_id": route_uuid,
                        "port_id": port_uuid,
                        
                    }

                    # Insert into 'routes_ports' table
                    try:
                        connection.execute(
                            insert(routes_ports).values(new_entry)
                        )
                        print(f"Successfully added route-port mapping for Route '{route_name}' -> Port '{port_name}'.")
                    except exc.IntegrityError:
                        print(f"Failed to insert route-port mapping for Route '{route_name}' and Port '{port_name}'. Duplicate entry.")
                    except Exception as e:
                        print(f"Error inserting route-port mapping: {e}")

# Main function
def main():
    # File path for the single CSV file
    cargo_type_choke_points_csv = "cargo_type_choke_points.csv"

    # Load data
    cargo_type_choke_points_data = load_data_from_csv(cargo_type_choke_points_csv)

    # Extract unique choke_points data
    choke_points_data = cargo_type_choke_points_data[
        ["primary_chokepoints",
        "latitude", 
        "longitude"]
    ].drop_duplicates()

    # Extract unique cargo_types data
    cargo_types_data = cargo_type_choke_points_data[
        ["vessel_composition_cargo_type"]
    ].drop_duplicates()

    # Extract junction table data
    junction_data = cargo_type_choke_points_data[
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
    insert_choke_points_cargo_types(junction_data)


    route_choke_points_csv = "route_choke_points_data.csv"

    # Load data
    route_choke_points_data = load_data_from_csv(route_choke_points_csv)

    # Extract routes data
    routes_data = route_choke_points_data[
        ["route_name", "importance", "market1", "market2"]
    ].drop_duplicates()

    # Insert into routes table
    insert_routes(routes_data)

    # Insert into routes_choke_point junction table
    process_csv_insert_route_choke_point(route_choke_points_csv)

    # File path for the ports CSV file
    ports_csv = "ports_country.csv"

    # Load ports data
    ports_data = load_data_from_csv(ports_csv)

    unique_ports_data = ports_data.drop_duplicates(subset=["port_name"])

    # Insert data into ports table
    insert_ports(unique_ports_data)


    industries_data = ports_data.drop_duplicates(subset=["country", "top1_industry", "top2_industry", "top3_industry"])
    insert_countries_port_industries(industries_data)

    insert_port_cargo_type(ports_data)
    
    # Pass the loaded data to the processing function
    insert_ports_route_junction_table(route_choke_points_data)

if __name__ == "__main__":
    main()