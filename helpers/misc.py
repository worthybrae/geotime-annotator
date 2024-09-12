import s3fs
import streamlit as st
import pandas as pd
from typing import List
import math
import time
import csv
import threading
from haversine import haversine


   
def write_df_async():
    df = st.session_state['df']
    truncation = st.session_state['truncation']
    mins = st.session_state['minutes']
    threshold = st.session_state['km_threshold']
    device_id = st.session_state['device_id'].lower()
    aws_key = st.secrets["aws"]["PUBLIC_KEY"]
    aws_secret = st.secrets["aws"]["PRIVATE_KEY"]

    def _write_df():
        try:
            s3 = s3fs.S3FileSystem(
                key=aws_key,
                secret=aws_secret
            )

            s3_path = f"s3://a6dev-mltraining/raw_input/{truncation}/{mins}/{threshold}/{device_id}.csv"

            with s3.open(s3_path, 'w') as f:
                df.to_csv(f, index=False)
            print(f"file updated: {s3_path}")
        except Exception as e:
            print(f'error writing csv: {e}')

    # Start the write operation in a separate thread
    thread = threading.Thread(target=_write_df)
    thread.start()

    # Return immediately with some identifier (in this case, the thread object)
    return thread


def format_minutes(minutes: int) -> str:
    if math.isnan(minutes):
        return "<1m"
    days, _ = divmod(minutes, 1440)  # 1440 minutes in a day
    hours, _ = divmod(minutes, 60)   # 60 minutes in an hour
    if days > 0:
        return f"{days:,.0f}d"
    elif hours > 0:
        return f"{hours:,.0f}hr"
    else:
        return f"{minutes:,.0f}m"
    
def alt_format_minutes(minutes: int) -> str:
    if math.isnan(minutes):
        return "<1m"
    days, _ = divmod(minutes, 1440)  # 1440 minutes in a day
    hours, _ = divmod(minutes, 60)   # 60 minutes in an hour
    if days > 0:
        return f"{days:,.0f}", 'days'
    elif hours > 0:
        return f"{hours:,.0f}", 'hrs'
    else:
        return f"{minutes:,.0f}", 'mins'
    

def calculate_zoom_level(distance, map_width=500):
    # Calculate the zoom level
    zoom_level = min(math.log2((map_width * 156543.03) / distance) - 1, 10)
    
    return zoom_level
 
def calculate_radius(zoom_level):
    # Base radius at zoom level 0
    base_radius = 1000
    # Adjust the radius inversely with zoom level
    return base_radius / (1.1 ** zoom_level)
 
def human_format(num):
    magnitude = 0
    while abs(num) >= 1000:
        magnitude += 1
        num /= 1000.0
    # add more suffixes if you need them
    return '%.0f%s' % (num, ['', 'K', 'M', 'G', 'T', 'P'][magnitude])

# Calculate distance using Haversine formula
def calculate_distance(lat1, lon1, lat2, lon2):
    return haversine((lat1, lon1), (lat2, lon2))


def add_meta() -> bool:
    s3 = s3fs.S3FileSystem(
        key=st.secrets["aws"]["PUBLIC_KEY"],
        secret=st.secrets["aws"]["PRIVATE_KEY"]
    )
    s3_path = f's3://a6dev-mltraining/annotations.csv'

    # New row to add
    new_row = [st.session_state['device_id'], st.session_state['user_id'], time.time() - st.session_state['start'], st.session_state['stats']['locates'], st.session_state['stats']['max_segment']]

    # Append the new row directly to the file
    with s3.open(s3_path, 'a') as f:
        writer = csv.writer(f)
        writer.writerow(new_row)

def calculate_segments(segment: int, max_segments: int) -> List[int]:
    if segment == 0:
        segments = [segment, segment + 1]
    elif segment == max_segments:
        segments = [segment - 1, segment]
    else:
        segments = [segment - 1, segment, segment + 1]
    return segments


   
def format_speed(km, min):
    if math.isnan(km) or math.isnan(min):
        return f"0.0km/m"
    elif min <= 1:
        return f"{km:,.0f}km/m"
    else:
        return f"{km/min:,.0f}km/m"
    

def assign_color(fraud):
    if pd.isna(fraud):
        return 'gray'
    elif fraud:
        return 'green'
    else:
        return 'red'