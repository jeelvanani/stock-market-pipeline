import os
import json
import requests
from azure.storage.filedatalake import DataLakeServiceClient

# Replace with your Azure details
STORAGE_ACCOUNT_NAME = "{stockdatastorage}"
STORAGE_ACCOUNT_KEY = "{Account_key}"
FILE_SYSTEM_NAME = "raw-stock-data"  # Container name in ADLS Gen2
DIRECTORY_NAME = "raw-stock-data"
FILE_NAME = "stock_data.json"

# Stock API details (Replace with your API key)
API_KEY = "{apikey}"
STOCK_SYMBOL = "AAPL"
API_URL = f"https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol={STOCK_SYMBOL}&interval=5min&apikey=9G1VTZ2HC8BTVA5M"

def authenticate_datalake():
    """Authenticate with ADLS Gen2 using Storage Account Key."""
    try:
        service_client = DataLakeServiceClient(
            account_url=f"https://{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net",
            credential=STORAGE_ACCOUNT_KEY
        )
        return service_client
    except Exception as e:
        print(f"Error authenticating ADLS Gen2: {e}")
        return None

def fetch_stock_data():
    """Fetch stock data from API and save it locally."""
    response = requests.get(API_URL)
    data = response.json()

    with open(FILE_NAME, "w") as f:
        json.dump(data, f, indent=4)
    
    print("Stock data saved locally.")

def upload_to_adls():
    """Upload the stock data JSON file to ADLS Gen2."""
    try:
        service_client = authenticate_datalake()
        if not service_client:
            return
        
        file_system_client = service_client.get_file_system_client(file_system=FILE_SYSTEM_NAME)
        
        # Ensure directory exists
        directory_client = file_system_client.get_directory_client(DIRECTORY_NAME)
        directory_client.create_directory()  # Won't fail if already exists

        # Upload file
        file_client = directory_client.get_file_client(FILE_NAME)
        with open(FILE_NAME, "rb") as file:
            file_contents = file.read()
            file_client.upload_data(file_contents, overwrite=True)
        
        print(f"Stock data uploaded successfully to ADLS Gen2 in {DIRECTORY_NAME}/{FILE_NAME}")
    except Exception as e:
        print(f"Error uploading file to ADLS Gen2: {e}")

if __name__ == "__main__":
    fetch_stock_data()
    upload_to_adls()
