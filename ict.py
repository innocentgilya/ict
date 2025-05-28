import requests
import pandas as pd
from PIL import Image
from io import BytesIO
import matplotlib.pyplot as plt
from datetime import date, timedelta
import json
import os
import time # For potential rate limit delays if fetching multiple periods

# --- Configuration ---
NASA_API_KEY = 'K6j65DejVOccSE318C6JX0Ny71plgHGMYLDsD62M'
CACHE_DIR = 'nasa_api_cache' # Directory to store cached asteroid data

# Create cache directory if it doesn't exist
if not os.path.exists(CACHE_DIR):
    os.makedirs(CACHE_DIR)
    print(f"Created cache directory: {CACHE_DIR}")

# --- 1. Astronomy Picture of the Day (APOD) ---
print("--- Retrieving Astronomy Picture of the Day (APOD) ---")

apod_url = f"https://api.nasa.gov/planetary/apod?api_key={NASA_API_KEY}"

try:
    response_apod = requests.get(apod_url)
    response_apod.raise_for_status() # Raise an exception for HTTP errors (4xx or 5xx)
    apod_data = response_apod.json()

    print(f"Title: {apod_data.get('title', 'N/A')}")
    print(f"Date: {apod_data.get('date', 'N/A')}")
    print(f"Explanation: {apod_data.get('explanation', 'N/A')[:200]}...") # Limit explanation for brevity

    image_url = apod_data.get('hdurl') or apod_data.get('url') # Prefer HD image, fallback to regular

    if image_url:
        print(f"Image URL: {image_url}")
        try:
            image_response = requests.get(image_url, stream=True) # Use stream=True for large images
            image_response.raise_for_status()
            img = Image.open(BytesIO(image_response.content))
            plt.figure(figsize=(10, 8))
            plt.imshow(img)
            plt.axis('off') # Hide axes ticks
            plt.title(f"APOD: {apod_data.get('title', 'No Title')}")
            plt.show()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching APOD image: {e}")
        except Exception as e:
            print(f"Error displaying APOD image: {e}")
    else:
        print("No image URL found for today's APOD.")

except requests.exceptions.RequestException as e:
    print(f"Error connecting to APOD API: {e}")
    if response_apod.status_code == 403:
        print("Error: Invalid API Key or rate limit exceeded for APOD. Please check your API key.")
    else:
        print(f"APOD API response error: {response_apod.status_code} - {response_apod.text}")
except json.JSONDecodeError:
    print("Error: Could not decode APOD API response as JSON.")
except Exception as e:
    print(f"An unexpected error occurred during APOD retrieval: {e}")

print("\n" + "="*80 + "\n") # Separator

# --- 2. Asteroids - NeoWs Data Retrieval ---
print("--- Retrieving Asteroids - NeoWs Data ---")

def get_asteroids_data_with_cache(start_date_obj, end_date_obj, api_key, cache_dir):
    """
    Fetches asteroid data from NASA's NeoWs API, with a local file cache.
    The NeoWs Feed API has a maximum 7-day range.
    """
    start_date_str = start_date_obj.strftime('%Y-%m-%d')
    end_date_str = end_date_obj.strftime('%Y-%m-%d')
    cache_file_path = os.path.join(cache_dir, f'neows_feed_{start_date_str}_{end_date_str}.json')

    # Check if data is in cache
    if os.path.exists(cache_file_path):
        print(f"Loading asteroid data from cache: {cache_file_path}")
        with open(cache_file_path, 'r') as f:
            return json.load(f)
    else:
        print(f"Fetching asteroid data from NASA API for {start_date_str} to {end_date_str}...")
        neows_feed_url = "https://api.nasa.gov/neo/rest/v1/feed"
        params = {
            'start_date': start_date_str,
            'end_date': end_date_str,
            'api_key': api_key
        }
        try:
            response_neows = requests.get(neows_feed_url, params=params)
            response_neows.raise_for_status() # Raise an exception for HTTP errors (4xx or 5xx)
            neows_data = response_neows.json()

            # Save to cache
            with open(cache_file_path, 'w') as f:
                json.dump(neows_data, f)
            print(f"Successfully fetched and cached data for {start_date_str} to {end_date_str}.")
            return neows_data
        except requests.exceptions.RequestException as e:
            print(f"Error connecting to NeoWs API for {start_date_str} to {end_date_str}: {e}")
            if response_neows.status_code == 403:
                print("Error: Invalid API Key or rate limit exceeded for NeoWs. Please check your API key.")
            else:
                print(f"NeoWs API response error: {response_neows.status_code} - {response_neows.text}")
            return None
        except json.JSONDecodeError:
            print(f"Error: Could not decode NeoWs API response as JSON for {start_date_str} to {end_date_str}.")
            return None
        except Exception as e:
            print(f"An unexpected error occurred during NeoWs retrieval for {start_date_str} to {end_date_str}: {e}")
            return None

# Define the date range for asteroid data (maximum 7 days per request)
# We'll fetch data for the next 7 days from today.
current_date = date.today()
asteroid_start_date = current_date
asteroid_end_date = current_date + timedelta(days=6) # 7 days total (today + 6 more days)

neows_data = get_asteroids_data_with_cache(asteroid_start_date, asteroid_end_date, NASA_API_KEY, CACHE_DIR)

asteroids_list = []

if neows_data:
    # Iterate through the dates and then through the near_earth_objects for each date
    for date_str, asteroids_on_day in neows_data.get('near_earth_objects', {}).items():
        for asteroid in asteroids_on_day:
            # Extract relevant information
            asteroid_id = asteroid.get('neo_reference_id')
            asteroid_name = asteroid.get('name')

            # Estimated diameter (minimal in kilometers)
            estimated_diameter_km = asteroid.get('estimated_diameter', {}).get('kilometers', {})
            min_estimated_diameter_km = estimated_diameter_km.get('estimated_diameter_min')

            # Absolute magnitude
            absolute_magnitude = asteroid.get('absolute_magnitude_h')

            # Relative velocity (km/s) from close_approach_data
            relative_velocity_kps = None
            if asteroid.get('close_approach_data'):
                # There can be multiple close approach data points; take the first one found
                for ca_data in asteroid['close_approach_data']:
                    if ca_data.get('relative_velocity', {}).get('kilometers_per_second'):
                        # Convert to float
                        relative_velocity_kps = float(ca_data['relative_velocity']['kilometers_per_second'])
                        break # Found it, move to next asteroid

            asteroids_list.append({
                'Asteroid ID': asteroid_id,
                'Asteroid name': asteroid_name,
                'The Minimal estimated diameter in Kilometre': min_estimated_diameter_km,
                'Absolute_magnitude': absolute_magnitude,
                'Relative_velocity(km/s)': relative_velocity_kps
            })

    # Create the pandas DataFrame
    asteroids_df = pd.DataFrame(asteroids_list)

    # --- 3. Data Pre-processing (Ensuring columns and types) ---
    print("\n--- Pre-processing Asteroids Data ---")

    desired_columns = [
        'Asteroid ID',
        'Asteroid name',
        'The Minimal estimated diameter in Kilometre',
        'Absolute_magnitude',
        'Relative_velocity(km/s)'
    ]

    # Select only the desired columns and reorder
    clean_asteroids_df = asteroids_df[desired_columns].copy()

    # Ensure numeric columns are of numeric type, coercing errors to NaN
    clean_asteroids_df['The Minimal estimated diameter in Kilometre'] = pd.to_numeric(
        clean_asteroids_df['The Minimal estimated diameter in Kilometre'], errors='coerce'
    )
    clean_asteroids_df['Absolute_magnitude'] = pd.to_numeric(
        clean_asteroids_df['Absolute_magnitude'], errors='coerce'
    )
    clean_asteroids_df['Relative_velocity(km/s)'] = pd.to_numeric(
        clean_asteroids_df['Relative_velocity(km/s)'], errors='coerce'
    )

    print("\n--- Cleaned Asteroids Data Info ---")
    clean_asteroids_df.info()

    print("\n--- Cleaned Asteroids Data (first 5 rows) ---")
    print(clean_asteroids_df.head())

    # --- 4. Export to CSV ---
    csv_file_name = 'nasa_asteroids_data.csv'
    clean_asteroids_df.to_csv(csv_file_name, index=False)
    print(f"\nAsteroids data successfully exported to '{csv_file_name}'")

else:
    print("No asteroid data retrieved due to API errors or no data available for the specified range.")
    clean_asteroids_df = pd.DataFrame() # Ensure DataFrame is initialized even if empty
    print("An empty DataFrame has been created.")

print("\n" + "="*80 + "\n")
print("Script execution complete.")