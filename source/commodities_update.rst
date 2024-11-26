commodities_update Module
==========================

**Purpose**:
The `commodities_update.py` script was created to ensure that raw materials data in our database stays current with data from a designated commodities API. This script automates updates, reducing manual intervention and helping maintain the accuracy of dependent applications.

**Functionality**:
This script performs the following tasks:
- **Fetches Symbols**: Retrieves the latest symbols from the commodities API.
- **Database Synchronization**: Updates the `raw_materials` table with the latest symbols and links them to the appropriate API sources.
- **Data Validation**: Ensures that the database remains consistent and that updates are logged and committed only when all conditions are met.

**Usage**:
1. Ensure the environment variable `SUPABASE_DATABASE_URL` is set. Also ensure the api source name is defined inside the script as "Commodities API" or "Metals API"
2. Run the script using the command:

   .. code-block:: bash

      python commodities_update.py or  python3 commodities_update.py 

3. Review the console output for information on updated records.

.. automodule:: commodities_update
    :members:
    :undoc-members:
    :show-inheritance: