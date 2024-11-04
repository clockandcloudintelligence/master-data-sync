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

def fetch_raw_materials_symbols():
    """Fetch symbols from the raw_materials table in Supabase."""
    response = supabase.table("raw_materials").select("uuid, symbol").execute()
    
    # Check if data is available
    if response.data:
        # Print and return a list of dictionaries containing all `uuid` and `symbol` values
        data_list = [
            {"uuid": item["uuid"], "symbol": item["symbol"]}
            for item in response.data
            if item["symbol"] and item["symbol"] != "NoSymbol"  # Exclude invalid symbols
        ]
        # print("Fetched data:", data_list)
        return data_list
    else:
        print("No data found in raw_materials table.")
        return []


def fetch_prices_from_api(symbols, start_date, end_date):
    """Fetch historical prices from the Commodities API for a list of symbols."""
    url = os.getenv("TIME_SERIES_COMMODITIES_API_END_POINT")  
    params = {
        "access_key": COMMODITIES_API_KEY,
        "start_date": start_date,
        "end_date": end_date,
        "symbols": symbol,
    }
    print("The item for which data is fetched", symbols)
    response = requests.get(url, params=params)
    
    data = response.json()
    if data.get("error"):
        print(f"Error fetching data: {data['error']}")
    return data.get("data", {}).get("rates", {})

def store_prices(raw_material_id, date, price):
    """Store price data in raw_material_prices table in Supabase."""
    supabase.table("raw_material_prices").insert({
        "raw_material_id": raw_material_id,
        "price": price,
        "recorded_at": date
    }).execute()


def main():
    # Date range: last 2 years
    end_date = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')  # Use day prior to today
    start_date = (datetime.datetime.now() - datetime.timedelta(days=365 * 2)).strftime('%Y-%m-%d')

    # Fetch symbols from Supabase
    raw_materials = fetch_raw_materials_symbols()

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
            prices_data = fetch_prices_from_api([symbol], formatted_start_date, formatted_end_date)

            # Check if prices_data is valid and contains the expected structure
            if not prices_data or not isinstance(prices_data, dict):
                print(f"No data available for {symbol} from {formatted_start_date} to {formatted_end_date}. Moving to the next period.")
                current_start_date = current_end_date
                continue  # Skip to the next period

            # Store the data for this symbol
            for date, symbols_data in prices_data.items():
                if symbol in symbols_data:
                    price = symbols_data[symbol]
                    # Store each day's price for the symbol in the database
                    store_prices(raw_material_id, date, price)
                    print(f"Stored price for {symbol} on {date}: {price}")

            # Move to the next 30-day period
            current_start_date = current_end_date

# Make sure to call the main function
if __name__ == "__main__":
    main()