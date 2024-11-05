# Raw Material Data Importer

This repository contains scripts for importing raw material data and associated applications and countries from CSV files into a Supabase database. It also includes documentation generated using Sphinx.

## Scripts

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

3. **other_script.py**
   - **Purpose**: Placeholder for additional functionality related to data processing.
   - **Functionality**: Further data manipulations and interactions with the Supabase database.

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
SUPABASE_DATABASE_URL=<your_supabase_url_with_psycopg2>
RUN_IMPORT_SCRIPT=true # if you want to run the importer

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
