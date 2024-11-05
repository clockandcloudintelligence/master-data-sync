raw_material_importer_script Module
===================================

**Purpose**:
The `raw_material_data_import.py` script is responsible for importing raw material data and associated applications and countries from CSV files into a Supabase database. This module helps maintain the integrity and availability of raw material data in a structured format for further analysis.

**Functionality**:
This script performs the following tasks:
- **Load CSV Data**: Reads raw material and country data from specified CSV files.
- **Data Filtering and Cleaning**: Cleans and filters the data to ensure only relevant and valid entries are processed.
- **Database Insertion**: Inserts processed data into various tables in Supabase, including `raw_materials`, `applications`, `industries`, and their respective junction tables.

**Usage**:
1. Ensure the environment variables `SUPABASE_URL` and `SUPABASE_KEY` are set.
2. Prepare the CSV files named `raw_material.csv` and `raw_material_country.csv`.
3. Run the script using the command:

.. code-block:: bash

    python raw_material_importer_script.py or python3 raw_material_importer_script.py

4. Monitor the console output for status updates on data insertion.

.. automodule:: raw_material_importer_script
    :members:
    :undoc-members:
    :show-inheritance: