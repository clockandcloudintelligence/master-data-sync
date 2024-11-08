import os
from dotenv import load_dotenv
import datetime
import requests
from supabase import create_client, Client

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
    Fetch historical prices from Metals or Commodities API for the given symbols.

    Args:
        symbols (list): A list of symbols to fetch prices for.
        start_date (str): The start date for the price data in 'YYYY-MM-DD' format.
        end_date (str): The end date for the price data in 'YYYY-MM-DD' format.
        api_source (str): The source of the API ('Metals API' or 'Commodities API').

    Returns:
        dict: A dictionary containing historical price data indexed by date.
    """
    # Set URL and API key based on the API source
    if api_source == "Commodities API":
        url = os.getenv("TIME_SERIES_COMMODITIES_API_END_POINT")
        api_key = os.getenv("COMMODITIES_API_KEY")
    elif api_source == "Metals API":
        url = os.getenv("TIME_SERIES_METALS_API_END_POINT")
        api_key = os.getenv("METALS_API_KEY")
    else:
        print(f"Unsupported API source: {api_source}. Skipping.")
        return {}

    params = {
        "access_key": api_key,
        "start_date": start_date,
        "end_date": end_date,
        "symbols": symbols,  
    }

    response = requests.get(url, params=params)
    data = response.json()
    

    # Extract rates and units from the API response
    rates = data.get("rates", {})
    
    # Handle case if rates are not available
    if not isinstance(rates, dict):
        print(f"Unexpected data format for rates: {type(rates)}. Skipping this period.")
        return {}

    price_data = {}
    for date, daily_rates in rates.items():
        price_data[date] = {}
        for symbol in symbols:
            if symbol in daily_rates:
                # For Metals API, default unit to 'per ounce'
                if api_source == "Metals API":
                    unit = "per ounce"
                # For Commodities API, fetch the correct unit from the response (if available)
                elif api_source == "Commodities API":
                    # Assuming that unit is present in the response, we will get it here
                    unit = data.get("unit", {}).get(symbol, "N/A")
                price_data[date][symbol] = {
                    "price": daily_rates[symbol],
                    "unit": unit  
                }
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
    Main function to orchestrate the fetching and storing of raw material prices.

    Attributes:
        end_date (str): The end date for fetching prices, set to one day before today.:no-index:
        start_date (str): The start date for fetching prices, set to two years before the current date.:no-index:
        raw_materials (list): The list of raw materials fetched from the Supabase.:no-index:
        current_start_date (datetime): The start date for the current iteration, initialized to the start_date.:no-index:
        end_date_obj (datetime): The end date as a datetime object for comparison.:no-index:
        current_end_date (datetime): The calculated end date for each 30-day chunk during iteration.:no-index:
        prices_data (dict): The fetched price data for each symbol within the date range.:no-index:
    """
    # Date range: last 2 years
    end_date = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')  # Use day prior to today
    start_date = (datetime.datetime.now() - datetime.timedelta(days=365 * 2)).strftime('%Y-%m-%d')

    api_source = "Metals API"  # or "Commodities API"
    # Fetch symbols from Supabase
    raw_materials = fetch_raw_materials_by_api_source(api_source)
    for material in raw_materials:
        symbol = material["symbol"]
        raw_material_id = material["uuid"]

        # Set the start date for looping through 30-day periods
        current_start_date = datetime.datetime.strptime(start_date, '%Y-%m-%d')
        end_date_obj = datetime.datetime.strptime(end_date, '%Y-%m-%d')

        # Loop through the entire date range in 30-day chunks
        while current_start_date < end_date_obj:
            # Define the end date for the current chunk (up to 30 days)
            current_end_date = current_start_date + datetime.timedelta(days=30)
            if current_end_date > end_date_obj:
                current_end_date = end_date_obj

            # Format dates for the API request
            formatted_start_date = current_start_date.strftime('%Y-%m-%d')
            formatted_end_date = current_end_date.strftime('%Y-%m-%d')

            # Fetch prices for the single symbol in this 30-day chunk
            prices_data = fetch_prices_from_api([symbol], formatted_start_date, formatted_end_date, api_source)

            # Check if prices_data is valid and contains the expected structure
            if not prices_data or not isinstance(prices_data, dict):
                print(f"No data available for {symbol} from {formatted_start_date} to {formatted_end_date}. Moving to the next period.")
                current_start_date = current_end_date
                continue  # Skip to the next period

            # Store the data for this symbol
            for date, symbols_data in prices_data.items():
                if symbol in symbols_data:
                    price = symbols_data[symbol]["price"]
                    unit = symbols_data[symbol]["unit"]  # Extract unit here
                    # Store each day's price for the symbol in the database
                    store_prices(raw_material_id, date, price, unit)
                    print(f"Stored price for {symbol} on {date}: {price} {unit}")

            # Move to the next 30-day period
            current_start_date = current_end_date

# Make sure to call the main function
if __name__ == "__main__":
    main()