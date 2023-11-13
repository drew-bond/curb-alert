import pandas as pd
import numpy as np
from geopy.distance import geodesic
import json
import geopy
import logging
from send_email import send_email_if_condition_met
import ast

# Set up logging
logging.basicConfig(level=logging.INFO)

# Load configuration
with open('config.json', 'r') as f:
    config = json.load(f)

home_coordinates = (config['home_lat'], config['home_lon'])

def calculate_distance_regex(row):
    try:
        if 'nan' not in row['regex_coordinates']:
            lat1, lon1 = row['regex_coordinates'].split(',')
            return geopy.distance.distance(home_coordinates, (lat1, lon1)).miles
    except ValueError as e:
        logging.error(f"Error processing row: {str(e)}")
    return None

def calculate_distance_parsed(df):
    distances = []
    for coords in df['lat_long_pairs']:
        if pd.isnull(coords):
            distances.append(None)
        else:
            coords_list = ast.literal_eval(coords)
            distances.append([geodesic(home_coordinates, coord).miles for coord in coords_list])
    return distances

df = pd.read_csv('data/to_be_proximity_checked.csv')
df['regex_distance'] = df.apply(calculate_distance_regex, axis=1)
df['parsed_distance'] = calculate_distance_parsed(df)
df['regex_nearby'] = df['regex_distance'] < 1
df['parsed_nearby'] = df['parsed_distance'].apply(lambda x: any(i < 1 for i in x) if isinstance(x, list) else False)

# Append to existing CSV
with open('data/curb_alert_list.csv', 'a') as f:
    df.to_csv(f, mode='a', header=False, index=False)

# Save to new CSV
df.to_csv('data/proximity_checked.csv', index=False)

# Send email notifications
send_email_if_condition_met(df, 'regex_nearby', 'parsed_nearby', 'Caption')

# Update and deduplicate the full list
full_list = pd.read_csv('data/curb_alert_list.csv')
full_list['Date'] = pd.to_datetime(full_list['Date'])
full_list.sort_values(by=['Date'], ascending=False, inplace=True)
full_list.drop_duplicates(subset=['Post URL'], inplace=True)
full_list.to_csv('data/curb_alert_list.csv', index=False)
