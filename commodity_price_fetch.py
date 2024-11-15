import os
from dotenv import load_dotenv
import datetime
import requests
from supabase import create_client, Client
from urllib.parse import urlencode, quote

load_dotenv()

# Supabase configuration
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY") 
COMMODITIES_API_KEY = os.getenv("COMMODITIES_API_KEY") 

# Initialize Supabase client
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def fetch_raw_materials_by_api_source(api_source_name):
    """
    Fetch raw materials associated with a specific API source.

    Args:
        api_source_name (str): The name of the API source (e.g., "Commodities API").
    
    Returns:
        list: A list of dictionaries containing `uuid` and `symbol` for the raw materials.
    """
    # Fetch the UUID of the API source from the api_sources table
    api_source_response = supabase.table("api_sources").select("uuid").eq("name", api_source_name).execute()

    # Extract the UUID of the API source
    api_source_data = api_source_response.data
    if not api_source_data:
        print(f"No API source found with the name: {api_source_name}")
        return []

    api_source_uuid = api_source_data[0]["uuid"]

    # Fetch raw materials linked to the specific API source UUID
    raw_materials_response = (
        supabase.table("raw_materials")
        .select("uuid, symbol")
        .eq("api_source", api_source_uuid)
        .execute()
    )

    # Extract and return the raw materials
    raw_materials_data = raw_materials_response.data
    if not raw_materials_data:
        print(f"No raw materials found for API source: {api_source_name}")
        return []

    return raw_materials_data


def fetch_prices_from_api(symbols, start_date, end_date, api_source):
    """
    Fetch historical prices for given symbols from various APIs.

    Args:
        symbols (list): List of raw material symbols or names (e.g., ["gold", "molybdenum"]).
        start_date (str): Start date in 'YYYY-MM-DD' format.
        end_date (str): End date in 'YYYY-MM-DD' format.
        api_source (str): API source name ("Metals API", "Commodities API", "Commoditic API").

    Returns:
        dict: Historical price data indexed by date and symbol.
    """
    # API-specific configurations
    source_map = {
        "Metals API": {
            "url": os.getenv("TIME_SERIES_METALS_API_END_POINT"),
            "key": os.getenv("METALS_API_KEY"),
            "extra_params": {}
        },
        "Commodities API": {
            "url": os.getenv("TIME_SERIES_COMMODITIES_API_END_POINT"),
            "key": os.getenv("COMMODITIES_API_KEY"),
            "extra_params": {}
        },
        "Commoditic API": {
            "url": os.getenv("COMMODITIC_API_END_POINT"),
            "key": os.getenv("COMMODITIC_API_KEY"),
            "extra_params": {"category": "metals", "frequency": "day"}
        }
    }

    # Check if the API source is supported
    if api_source not in source_map:
        print(f"Unsupported API source: {api_source}. Skipping.")
        return {}

    # Extract API-specific configurations
    api_config = source_map[api_source]
    url = api_config["url"]
    api_key = api_config["key"]
    extra_params = api_config["extra_params"]

    price_data = {}
    for symbol in symbols:
        if api_source == "Commoditic API":
            # Params for Commoditic API
            params = {
                "key": api_key,
                "name": symbol,  # Commoditic API uses 'name' for raw material
                "date_from": start_date,
                "date_to": end_date
            }
            params.update(extra_params)  # Add category, frequency, etc.

            encoded_params = "&".join(f"{key}={quote(str(value))}" for key, value in params.items())
            full_url = f"{url}?{encoded_params}"
        else:
            # Params for Metals and Commodities API
            params = {
                "access_key": api_key,  # Commodities and Metals APIs use 'access_key'
                "start_date": start_date,
                "end_date": end_date,
                "symbols": symbol  # Metals and Commodities APIs use 'symbols'
            }
            params.update(extra_params)  # Add any extra params if needed

            # Let `requests` handle URL encoding
            full_url = url

     
        # Fetch data from the API
        response = requests.get(full_url if api_source == "Commoditic API" else url, params=params if api_source != "Commoditic API" else None)
        data = response.json()
        # Process response based on API structure
        if api_source == "Commoditic API":
            output = data.get("output", [])
            for entry in output:
                unit = entry.get("unit", "N/A")
                for price_entry in entry.get("prices", []):
                    date = price_entry["date"]
                    price = price_entry["price"]
                    # Store data in the dictionary with the date as the key
                    price_data.setdefault(date, {})[symbol] = {"price": price, "unit": unit}
        else:
            rates = data.get("rates", {})
            for date, daily_rates in rates.items():
                if symbol in daily_rates:
                    unit = "per ounce" if api_source == "Metals API" else "N/A"
                    price_data.setdefault(date, {})[symbol] = {"price": daily_rates[symbol], "unit": unit}

    return price_data
def store_prices(raw_material_id, date, price, unit):
    """
    Store price data in raw_material_prices table in Supabase.

    Args:
        raw_material_id (UUID): The unique identifier of the raw material.
        date (str): The date for which the price is recorded.
        price (float): The price of the raw material on the given date.
        unit (str): The unit of measurement for the price.
    """
    supabase.table("raw_material_prices").insert({
        "raw_material_id": raw_material_id,
        "price": price,
        "unit": unit,
        "recorded_at": date
    }).execute()


def main():
    """
    Main function to orchestrate the fetching and storing of raw material prices for all API sources.
    """
    # Date range: last 2 years
    end_date = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')  # Use day prior to today
    start_date = (datetime.datetime.now() - datetime.timedelta(days=365 * 2)).strftime('%Y-%m-%d')

    # List of API sources to process
    api_source = "Metals API"  # or "Commodities API"

   
    print(f"Processing API Source: {api_source}")

    # Fetch symbols from Supabase for the current API source
    raw_materials = fetch_raw_materials_by_api_source(api_source)

    for material in raw_materials:
        if api_source == "Commoditic API":
            symbol = material["raw_material_name"]
        else:
            symbol = material["symbol"]
        raw_material_id = material["uuid"]

        # Handle data fetching differently for "Commoditic API"
        if api_source == "Commoditic API":
            # Fetch all data in one request for the full date range
            prices_data = fetch_prices_from_api([symbol], start_date, end_date, api_source)

            if not prices_data:
                print(f"No data available for {symbol} from {start_date} to {end_date}.")
                continue

            # Store the data
            for date, symbols_data in prices_data.items():
                if symbol in symbols_data:
                    price = symbols_data[symbol]["price"]
                    unit = symbols_data[symbol]["unit"]
                    store_prices(raw_material_id, date, price, unit)
                    print(f"Stored price for {symbol} on {date}: {price} {unit}")

        else:
            # Fetch data in 30-day chunks for other APIs
            current_start_date = datetime.datetime.strptime(start_date, '%Y-%m-%d')
            end_date_obj = datetime.datetime.strptime(end_date, '%Y-%m-%d')

            while current_start_date < end_date_obj:
                current_end_date = current_start_date + datetime.timedelta(days=30)
                if current_end_date > end_date_obj:
                    current_end_date = end_date_obj

                # Format dates for the API request
                formatted_start_date = current_start_date.strftime('%Y-%m-%d')
                formatted_end_date = current_end_date.strftime('%Y-%m-%d')

                # Fetch prices for the single symbol in this 30-day chunk
                prices_data = fetch_prices_from_api([symbol], formatted_start_date, formatted_end_date, api_source)

                if not prices_data:
                    print(f"No data available for {symbol} from {formatted_start_date} to {formatted_end_date}.")
                    current_start_date = current_end_date
                    continue

                # Store the data
                for date, symbols_data in prices_data.items():
                    if symbol in symbols_data:
                        price = symbols_data[symbol]["price"]
                        unit = symbols_data[symbol]["unit"]
                        store_prices(raw_material_id, date, price, unit)
                        print(f"Stored price for {symbol} on {date}: {price} {unit}")

                current_start_date = current_end_date  # Move to next period

# Make sure to call the main function
if __name__ == "__main__":
    main()