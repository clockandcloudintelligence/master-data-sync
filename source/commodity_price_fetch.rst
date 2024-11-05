commodity_price_fetch Module
=============================

**Purpose**:
The `commodities_price_fetch.py` script is designed to fetch historical price data for raw materials from the Commodities API and store it in a Supabase database. This automation ensures that price data remains up to date for analytical and reporting purposes.

**Functionality**:
This script performs the following tasks:
- **Fetches Raw Materials Symbols**: Retrieves the UUID and symbol for each raw material from the `raw_materials` table in Supabase.
- **Fetches Prices from API**: Queries the Commodities API for historical price data based on the fetched symbols and specified date ranges.
- **Stores Prices**: Inserts the fetched price data into the `raw_material_prices` table in Supabase.

**Usage**:
1. Ensure the environment variables `SUPABASE_URL`, `SUPABASE_KEY`, and `COMMODITIES_API_KEY` are set.
2. Run the script using the command:

.. code-block:: bash

    python commodity_price_fetch.py or  python3 commodity_price_fetch.py

3. Review the console output for information on stored prices.

.. automodule:: commodity_price_fetch
    :members:
    :undoc-members:
    :show-inheritance:
