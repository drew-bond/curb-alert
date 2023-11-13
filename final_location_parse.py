import pandas as pd

def process_csv(input_file, output_file):
        # Read the CSV file
    df = pd.read_csv(input_file)
    
    # Convert the first column to the desired format
    df['regex_coordinates'] = df['regex_coordinates'].apply(lambda x: [(float(x.split(",")[0].strip()), float(x.split(",")[1].strip()))])
    
    # Convert the second column from string to list of tuples
    df['lat_long_pairs'] = df['lat_long_pairs'].apply(eval)
    
    # Combine the two columns
    df['Combined'] = df['regex_coordinates'] + df['lat_long_pairs']
    
    # Filter out unwanted latitude and longitude pairs
    def filter_coordinates(coords_list):
        filtered_list = []
        for coord in coords_list:
            lat, lon = coord
            if 40.580 <= lat <= 40.875 and -74.052 <= lon <= -73.741:
                filtered_list.append(coord)
        return filtered_list
    
    df['Filtered Combined'] = df['Combined'].apply(filter_coordinates)
    
     # Remove existing double quotes from the Caption column
    df['Caption'] = df['Caption'].str.replace('"', '')

    # Select the required columns for the output
    df_output = df[['Media ID', 'Caption', 'Date', 'Post URL', 'Filtered Combined']]
    
    # Save the processed data to a new CSV file
    df_output.to_csv(output_file, index=False, )

# Example usage
input_file = 'data/curb_alert_list.csv'
output_file = 'data/final_location_parse.csv'
process_csv(input_file, output_file)

    