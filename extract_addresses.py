import pandas as pd
import re
from geopy.geocoders import GoogleV3
import time 
import json


def regex_extract_street_address(text):
    # Regular expression pattern to match a street address in the text
    pattern = r'\b\d+\s+[\w\s]+\b'  # Matches patterns like "123 Street Name"

    # Find all occurrences of the pattern in the text
    addresses = re.findall(pattern, text)

    # Return the first occurrence (if any)
    return addresses[0] if addresses else None

def regex_geocode_address(address, api_key):
    if address:
        geolocator = GoogleV3(api_key=api_key)
        location = geolocator.geocode(address)
        if location:
            return location.latitude, location.longitude
    return None, None

#This uses a regex search to pull out addresses
# if a value already exists in the regex_coordinates field, that line should be skipped 
def regex_process_csv(input_file, temp_file, api_key):
    # Read the CSV file into a pandas DataFrame
    df = pd.read_csv(input_file)

    # Apply the regex_extract_street_address function to 'Caption' column
    df['regex_street_address'] = df['Caption'].apply(lambda x: regex_extract_street_address(x) if isinstance(x, str) else None)
    
    # Add NY to end of address
    df['ny_regex_street_address'] = df['regex_street_address'].apply(lambda address: address + ", New York, New York" if address is not None else None)

    # Apply the regex_geocode_address function to get latitude and longitude for each address
    #df['regex_latitude'], df['regex_longitude'] = zip(*df['ny_regex_street_address'].map(lambda address: regex_geocode_address(address, api_key)))

    # only apply the regex_geocode_address function if the regex_coordinates field is empty
    for index, row in df.iterrows():
        #if pd.isnull(row['regex_coordinates']): #removing this because I no longer need to check if the regex_coordinates field is empty
            address = row['ny_regex_street_address']
            latitude, longitude = regex_geocode_address(address, api_key)
            df.at[index, 'regex_latitude'] = latitude
            df.at[index, 'regex_longitude'] = longitude

    # Combine values of field1 and field2 with a comma
    df['regex_coordinates'] = df.apply(lambda row: f"{row['regex_latitude']}, {row['regex_longitude']}", axis=1)

    # # Save the updated DataFrame back to the CSV file
    # df.to_csv(output_file, index=False)

    # Save the updated DataFrame to a temporary CSV file
    temp_file = 'data/temp_output.csv'
    df.to_csv(temp_file, index=False)

    return temp_file


def caption_parse_extract_address(temp_file, output_file, api_key):
    # Read CSV into a DataFrame
    df = pd.read_csv(temp_file)
    
    # Convert the 'float_field' to strings
    df['Caption'] = df['Caption'].astype(str)

    # Function to clean the caption
    def clean_caption(caption):
        # Replace " st." and " St." with " st"
        caption = caption.replace(" st.", " st").replace(" St.", " st")
        
        # Clean the caption using regex, removing all characters except letters and numbers
        cleaned_caption = re.sub(r'[^a-zA-Z0-9&#@!.,?]', ' ', caption)
        return cleaned_caption
    
    # Apply the clean_caption function to the 'Caption' column
    df['caption cleaned'] = df['Caption'].apply(clean_caption)
    
    # Function to split caption into parts by punctuation and by "at", "on", "of",
    # regardless of case and only if those words are separated by spaces on each side
    def split_caption_parts(caption):
        parts = re.split(r'[\s#.,!?]{2,}| at | on | of ' , caption, flags=re.IGNORECASE)
        return [part for part in parts if part]  # Remove empty parts
    
    # Apply the split_caption_parts function to the cleaned caption column
    df['caption parts'] = df['caption cleaned'].apply(split_caption_parts)
 
    # Fill null values with an empty string and then replace brackets and apostrophes using regex
    df['Caption Hashtags'] = df['Caption Hashtags'].fillna('').str.replace(r"['\[\]]", "", regex=True)



    # create a new field using the Caption Hashtag field, replacing each value with one from this dictionary if it exists
    # this is a way to standardize the burrough in which the curb alert was spotted, which is the only structured location data available
    burrough_mapping = {
    'stoopingbrooklyn': 'Brooklyn',
    'stoopingqueens': 'Queens',
    'stoopingbronx': 'Bronx',
    'stoopinghoboken': 'Hoboken',
    'stoopingnyc': 'New York City',
    'stooping': 'New York City'
    }

    df['burrough'] = df['Caption Hashtags'].replace(burrough_mapping, inplace=False)
    #if no burrough is listed, assume it's in NYC
    df['burrough'].fillna('New York City', inplace=True)


    # Geocode each part of each caption and get latitude and longitude
    def geocode_address(address, geolocator):
        if address:
            location = geolocator.geocode(address)
            if location:
                return location.latitude, location.longitude
        return None, None
    
    geolocator = GoogleV3(api_key=api_key)
    lat_long_pairs = []
    for index, row in df.iterrows():
        pair_list = []
        for part in row['caption parts']:
            # Add the burrogh to the end of the address 
            part = part.join(", " + row['burrough'] + ", New York")
            lat, long = geocode_address(part, geolocator)
            if lat is not None and long is not None:
                pair_list.append((lat, long))
        lat_long_pairs.append(pair_list)
        
    df['lat_long_pairs'] = lat_long_pairs

    # #Save the updated DataFrame back to the CSV file
    # df.to_csv(output_file, index=False)

    # Append the updated DataFrame to the existing CSV file
    #df.to_csv(output_file, mode='a', header=False, index=False)

    #create a new csv called to_be_proximity_checked.csv from the df
    df.to_csv(output_file, index=False)

    


if __name__ == "__main__":
    # Replace 'input.csv' and 'output.csv' with your input and output file names, and 'YOUR_API_KEY' with your Google Maps API key
    input_file = 'data/new_posts_list.csv'
    temp_file = 'data/temp_output.csv'
    output_file = 'data/to_be_proximity_checked.csv'

    with open('secrets.json') as f:
        secrets = json.load(f)
    api_key = secrets['google_places_api_key']



    empty_check = pd.read_csv(input_file)

    if empty_check.empty:
        print("No new posts")
        
    else:
        regex_process_csv(input_file, temp_file, api_key)
        caption_parse_extract_address(temp_file, output_file, api_key)

    # Wait for 30 minutes before running the scripts again
    #time.sleep(800)

    

