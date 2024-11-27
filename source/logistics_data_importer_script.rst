logistics_data_importer_script Module
======================================

**Purpose**:
The logistics_data_importer.py script is designed to automate the import and processing of logistics-related data from CSV files into a structured database. This module ensures the consistent handling of logistics data, enabling streamlined analysis and integration.

**Functionality**:
The script processes and inserts the following types of logistics data into the database:
    - Choke Points and Cargo Types:
        Extracts and cleans choke point data from a CSV file.
        Maps choke points to cargo types, creating and maintaining relationships in the database.
        Routes and Choke Points:

    - Processes route data and links it to choke points.
    Inserts route and choke point relationships into the appropriate tables.
    Ports and Associated Data:

    - Handles port data, including country and industry associations.
    Maintains relationships between ports, countries, and industries for further use.

**Usage**:
1. Ensure that the required CSV files are available in the working directory:

2. cargo_type_choke_points.csv: For choke point and cargo type data.
route_choke_points_data.csv: For route and choke point relationships.
ports_country.csv: For port data and associated industries.
Run the script using the command:

.. code-block:: bash

    python3 logistics_data_importer.py 

4. Monitor the console output for status updates on data insertion.

.. automodule:: logistics_data_importer_script
    :members:
    :undoc-members:
    :show-inheritance: