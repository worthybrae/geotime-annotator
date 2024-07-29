import s3fs
import streamlit as st
import pandas as pd
from typing import List
import math

    
def write_df() -> bool:
    try:
        s3 = s3fs.S3FileSystem(
            key=st.secrets["aws"]["PUBLIC_KEY"],
            secret=st.secrets["aws"]["PRIVATE_KEY"]
        )

        # Specify the S3 path to your CSV file
        s3_path = f"s3://a6dev/raw_input/{st.session_state['truncation']}/{st.session_state['minutes']}/{st.session_state['km_threshold']}/{st.session_state['device_id'].lower()}.csv"

        # Read the CSV file directly from S3 into a pandas DataFrame
        with s3.open(s3_path, 'w') as f:
            df = st.session_state['df']
            df.to_csv(f, index=False)
            return True
    except Exception as e:
        print(f'error writing csv: {e}')
        return False

def calculate_segments(segment: int, max_segments: int) -> List[int]:
    if segment == 0:
        segments = [segment, segment + 1]
    elif segment == max_segments:
        segments = [segment - 1, segment]
    else:
        segments = [segment - 1, segment, segment + 1]
    return segments

def calculate_zoom_level(distance, map_width=500):
    # Calculate the zoom level
    zoom_level = min(math.log2((map_width * 156543.03) / distance) - 1, 10)
    
    return zoom_level

def calculate_radius(zoom_level):
    # Base radius at zoom level 0
    base_radius = 1000
    # Adjust the radius inversely with zoom level
    return base_radius / (1.1 ** zoom_level)

def format_minutes(minutes: int) -> str:
    if math.isnan(minutes):
        return "<1 min"
    days, mins = divmod(minutes, 1440)  # 1440 minutes in a day
    hours, mins = divmod(minutes, 60)   # 60 minutes in an hour
    if days > 0:
        if days == 1:
            return f"1 day"
        else:
            return f"{days:,.0f} days"
    elif hours > 0:
        if hours == 1:
            return f"1 hr"
        else:
            return f"{hours:,.0f} hrs"
    else:
        if minutes <= 1:
            return f"1 min"
        else:
            return f"{minutes:,.0f} mins"
    
def format_speed(km, min):
    if math.isnan(km) or math.isnan(min):
        return f"0.0km / min"
    elif min <= 1:
        return f"{km:,.1f}km / min"
    else:
        return f"{km/min:,.1f}km / min"