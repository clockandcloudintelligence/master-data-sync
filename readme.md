# Master Data Importer

This repository contains scripts for importing and processing raw material data, logistics data, and energy data. The files are organized following a structured folder hierarchy to ensure clarity and ease of navigation. For instance, scripts related to logistics are located under the logistics folder.

Additionally, the repository includes comprehensive documentation generated using Sphinx, providing detailed guidance on the functionality and usage of the scripts.

## Scripts

The data import process is organized into three main folders, each corresponding to a specific type of data. Here's the structure:

1. energy:
2. logistics:
   Inside the logistics folder we have following scripts:

   1. **logistics_data_importer_script.py**

      - The logistics_data_importer.py script is designed to handle the ingestion and processing of logistics-related data. It processes various CSV files, extracts relevant data, and populates multiple database tables, including junction tables. Below is a breakdown of the script's functionality:

3. raw_materials:
   Inside the raw_materials folder we have following scripts:

   1. **raw_material_importer_script.py**

      - **Purpose**: Imports raw materials and associated applications and countries from CSV files into a Supabase database.
      - **Functionality**:
      - Loads data from specified CSV files.
      - Cleans and filters the data.
      - Inserts data into Supabase tables.

   2. **commodity_update.py**

      - **Purpose**: Updates commodity data in the Supabase database based on specified criteria.
      - **Functionality**:
      - Connects to the Supabase database.
      - Retrieves and updates commodity records as necessary.

   3. **convert_prices_to_usd_price**

      - **Purpose**: Updates raw material price data in the Supabase database by calculating and storing their equivalent prices in USD.
      - **Functionality**:
      - Connects to the Supabase database using credentials from environment variables.
      - Defines SQLAlchemy models for ApiSource, RawMaterial, and RawMaterialPrice tables.
      - Retrieves the raw material price records for a specific API source ("Metals API").
      - Computes the price in USD for each raw material price record based on its original value.
      - Updates the database with the calculated price_in_usd values.

   4. **commodity_price_fetch**
      - **Purpose**: Fetches and stores raw material price data from various API sources (e.g., Metals API, Commodities API, Commoditic API) into a Supabase database.
      - **Functionality**:
      - Connects to the Supabase database using credentials loaded from environment variables.
      - Defines functions to fetch raw materials associated with a specified API source.
      - Fetches historical price data for raw materials from different APIs based on a given date range.
      - Stores the retrieved price data in the Supabase raw_material_prices table.
      - Processes the data in chunks for APIs that support it, handling each API source differently (e.g., Commoditic API fetches data in one request, others in 30-day chunks).
      - Supports multiple API sources and formats, including URL encoding and request construction for each API type.
      - The main function orchestrates fetching and storing data for all raw materials linked to the specified API source within a defined date range.

## Requirements

- Python 3.x
- Pandas
- Supabase Python client
- Dotenv
- Sphinx (for documentation)

## Installation

1. Clone the repository:

   ```bash
   git clone <repository_url>
   cd <repository_directory>

   ```

2. Install the required packages:
   pip install pandas supabase dotenv sphinx

3. Set up your Supabase environment variables in a .env file:

SUPABASE_URL=<your_supabase_url>
SUPABASE_KEY=<your_supabase_key>
COMMODITIES_API_KEY=<commodities_api_key>
TIME_SERIES_COMMODITIES_API_END_POINT=<time_series_commodities_api_end_point>
TIME_SERIES_METALS_API_END_POINT==<time_series_metals_api_end_point>
SUPABASE_DATABASE_URL=<your_supabase_url_with_psycopg2>
RUN_IMPORT_SCRIPT=true # if you want to run the importer
COMMODITIC_API_KEY=<your_commoditic_api_key>
COMMODITIC_API_END_POINT=<your_commoditic_endpoint>
METALS_API_KEY=<your_metals_api_key>
STRAPI_COUNTIRES_API_URL=<strapi_countries_api_url>
STRAPI_API_KEY= <strapi_api_key>

How to Run the Scripts
To run the scripts, use the following command:
python raw_material_importer_script.py or python3 raw_material_importer_script.py

You can also run the other scripts in a similar manner:
python commodity_update.py python3 commodity_update.py
python other_script.py or python3 other_script.py

Generating Documentation with Sphinx

1. Navigate to the directory where the scripts are located.'
2. Initialize Sphinx in your project directory:
   sphinx-quickstart

Follow the prompts to set up the documentation structure.

3. Configure conf.py to include the paths to your scripts. You may need to modify the sys.path in conf.py to ensure Sphinx can find your scripts:

import os
import sys
sys.path.insert(0, os.path.abspath('.'))

4. Create reStructuredText files for your scripts (e.g., raw_material_importer_script.rst, commodity_update.rst, etc.) in the source directory of your Sphinx setup.

5. Use the following command to generate the documentation:
   sphinx-build -b html source/ build/

6. The generated documentation can be found in the build/index.html directory. Open the index.html file in your web browser to view the documentation.
