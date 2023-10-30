import pandas as pd
import requests
from datetime import datetime, timedelta
import os
import jwt  # PyJWT library
from tqdm import tqdm


# Function to fetch data from the API
def fetch_data(api_url, token):
    headers = {"Authorization": token}
    response = requests.get(api_url, headers=headers)
    return response.json()


# Function to generate a JWT token
def generate_token(id, email=None, name=None):
    token_params = {"id": id}
    if name:
        token_params["name"] = name
    if email:
        token_params["email"] = email
    token = jwt.encode(token_params, "wearable-hub", algorithm="HS256")
    token = f"Bearer {token}"
    return token


# Function to get users from an XLSX file
def get_users_from_sheet(file_path):
    df = pd.read_excel(file_path)
    return df[["id", "name", "email"]].values


# Function to append data to a new Excel file for each batch
def append_data_to_new_excel(data, sheet_name, output_file):
    try:
        data.to_csv(output_file)
    except PermissionError as e:
        print(f"Error: {e}. Please make sure the file is not open in another program.")
    except Exception as e:
        print(f"An error occurred while writing to the Excel file: {e}")


# Function to fetch and process data in batches
def process_data_in_batches(end_point, batch_size=10000):
    print("Reading Excel sheet")
    try:
        user_info_list = get_users_from_sheet("extract.xlsx")
    except FileNotFoundError as e:
        print(f"Error: {e}. Please check if the file path is correct.")
        return []
    print("processing data from sheet")
    # Initialize data list to store all user data
    for start in range(0, len(user_info_list), batch_size):
        batch = user_info_list[start : start + batch_size]
        data = []
        for user_info in tqdm(batch):
            try:
                user_id, user_name, user_email = user_info
                # print(f"Processing user: {user_name}")

                user_data = {}  # Dictionary to store user data

                current_date = datetime(2023,10,8)
                last_date = current_date - timedelta(days=1)

                # API URLs
                hr_url = f"https://prod-hub-api.boat-lifestyle.com/{end_point}?startDate={last_date}&endDate={current_date}"

                # Generate a JWT token
                token = generate_token(user_id, user_email, user_name)

                # Fetch heartbeat data
                # print(f"Fetching data for user: {user_name}")
                response = fetch_data(hr_url, token)

                user_data = {
                    "id": user_id,
                    "statusCode": response.get("statusCode", "None"),
                    "data": response.get("data", "None"),
                    "pagination": response.get("pagination", "None"),
                    "message": response.get("message", "None"),
                }

                data.append(user_data)
                # print(f"Processed data for user: {user_name}")

            except Exception as e:
                print(f"Error processing user: {user_info}. {str(e)}")
                continue

        print(f"Data processing completed. Data: {data}")
        output_file = f'{end_point.replace("/", "_")}_day_3_{start}.csv'
        df = pd.DataFrame(data)
        print(len(df))
        append_data_to_new_excel(df, end_point, output_file)
        print(f"Data appended to Excel file: {output_file}")


if __name__ == "__main__":
    try:
        endpoint_sheet_mapper = {
            'activity/backup/hr' : 'hr',
             'activity/backup/stress' : 'stress',
            'activity/backup/sleep' : 'sleep',
            'activity/backup/spo2' : 'spo2',
            "activity/backup/steps": "steps",
            "sports/backup": "sports",
        
        }

        for endpoint in endpoint_sheet_mapper:
            print("processing: ", endpoint)
            process_data_in_batches(endpoint)

    except Exception as e:
        print(f"An error occurred: {e}")
