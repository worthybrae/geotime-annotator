import streamlit as st
import pandas as pd
import uuid
import time
import s3fs
import numpy as np
import snowflake.connector
import pydeck as pdk
import streamlit_shortcuts
from streamlit_extras.keyboard_text import key, load_key_css
from typing import List, Dict, Optional, Tuple
from helpers.misc import write_df, format_minutes, calculate_zoom_level, calculate_radius, format_speed


# API Functions
def start() -> bool:
    try:
        s3 = s3fs.S3FileSystem(
            key=st.secrets["aws"]["PUBLIC_KEY"],
            secret=st.secrets["aws"]["PRIVATE_KEY"]
        )

        # Specify the S3 path to your CSV file
        s3_path = f"s3://a6dev/raw_input/{st.session_state['truncation']}/{st.session_state['minutes']}/{st.session_state['km_threshold']}/{st.session_state['device_id'].lower()}.csv"

        # Read the CSV file directly from S3 into a pandas DataFrame
        with s3.open(s3_path, 'rb') as f:
            df = pd.read_csv(f)
  
            max_segment = df['segment'].max()

            grouped_df = df.groupby(['segment']).agg({
                'id': 'count',  # Count total rows
                'min_duration': 'max',
                'dist_travelled': 'max',
                'start_lat': 'max',
                'start_lon': 'max',
                'end_lat': 'max',
                'end_lon': 'max',
                'min_jump': 'max',
                'km_jump': 'max',
                'speed_flag': 'max',
                'conflict_flag': 'max',
                'supplier_flag': 'max',
                'fraud': 'max',
                'supply_id': pd.Series.nunique,
                'dupes': 'sum'
            }).reset_index()

            grouped_df['quality'] = (
                25 * (np.minimum(grouped_df['supply_id'], 3) / 3) +
                30 * (np.minimum(grouped_df['min_duration'], 60) / 60) + 
                25 * (np.minimum(grouped_df['id'], 50) / 50) + 
                10 * grouped_df['speed_flag'] +
                10 * grouped_df['supplier_flag']
            )
            avg_quality = np.average(grouped_df['quality'], weights=grouped_df['id'])
            null_fraud_row = df[df['fraud'].isnull()].head(1)
            starting_segment = int(null_fraud_row['segment'].iloc[0])
            annotated = max(0, max_segment - int(grouped_df['fraud'].isna().sum()))
            total_dupes_sum = grouped_df['dupes'].sum()
            
            stats = {
                'max_segment': max_segment,
                'current_segment': starting_segment,
                'annotated': annotated,
                'duplicates': total_dupes_sum - len(df),
                'locates': total_dupes_sum,
                'avg_quality': avg_quality
            }

            st.session_state['stats'] = stats
            st.session_state['df'] = df
            st.session_state['segment_df'] = grouped_df
            st.session_state['annotations'] = {}
            return True
    except Exception as e:
        print(f'error reading s3 file: {e}')
        return False

def query(device_id: str, minutes: int, truncation: int, km_threshold: int) -> bool:
    conn = snowflake.connector.connect(
        user=st.secrets["database"]["SF_USERNAME"],
        password=st.secrets["database"]["SF_PASSWORD"],
        account=st.secrets["database"]["SF_ACCOUNT"],
        warehouse=st.secrets["database"]["SF_WAREHOUSE"],
        database=st.secrets["database"]["SF_DATABASE"],
        schema=st.secrets["database"]["SF_SCHEMA"]
    )
    cursor = conn.cursor()
    
    # Define the query
    query = f"""
    with all_data as (
        select 
            lower(idfa) as id, 
            timestamp,
            latitude,
            longitude,
            supply_id,
            created_at,
            md5_hex(concat(time_slice(timestamp, {minutes}, 'minute'), trunc(latitude, {truncation}), trunc(longitude, {truncation}))) as rh,
            row_number() over (partition by rh order by created_at) as rn,
            count(1) over (partition by rh) as dupes
        from
            singularity.public.h4_maid_clustered
        where
            (idfa like '{device_id[:3].lower()}%' or idfa like '{device_id[:3].upper()}%') and
            lower(idfa) = '{device_id.lower()}'
    ), filtered_data as (
        select
            id,
            timestamp,
            latitude,
            longitude,
            supply_id,
            dupes
        from
            all_data
        where
            rn = 1
    ), enriched_data as (
        select
            id,
            timestamp,
            latitude,
            longitude,
            supply_id,
            dupes,
            datediff('minutes', lag(timestamp) over (order by timestamp, latitude, longitude), timestamp) as min_since_last_locate,
            haversine(latitude, longitude, lag(latitude) over (order by timestamp, latitude, longitude), lag(longitude) over (order by timestamp, latitude, longitude)) as km_since_last_locate,
            haversine(
                min(latitude) over (partition by time_slice(timestamp, 1, 'minute')), 
                min(longitude) over (partition by time_slice(timestamp, 1, 'minute')), 
                max(latitude) over (partition by time_slice(timestamp, 1, 'minute')), 
                max(longitude) over (partition by time_slice(timestamp, 1, 'minute')) 
            ) as km_in_min,
            case when km_in_min > 5 then 1 else 0 end as conflict_flag,
            case when km_since_last_locate / (case when min_since_last_locate = 0 then 1 else min_since_last_locate end) > 15 then 1 else 0 end as speed_flag,
            case when supply_id in (
                '655_b852fe3fb8d68718ab608856c4fc237df27f675aca72e8a460af7803a1d96cfe',
                '736',
                '655_4e567743ecb692dda46f2e522b3ac87e03c41923ebb412706b3f8b6e21b30a7a',
                '655_69769d3ee6342143cc160181a43c120611d955a9c4cf9231aad2c25d4aad30e6',
                '640',
                '540_1017',
                '655_dfdb5a8916bacb39d11662091c900f758e5421aea9d963f9bd5b2a622f340c69',
                '655_cd3486226b51f06416269883783fa30eba7e4bdddb08dd5d89101d005d69696a',
                '655_c33c26e692091d10bf9f8a968528b707ec582060258f4c6433881fed07db5116',
                '655_55a1568daa2956cf6c03905f8cdde49e773fb1b0252ad1f2832043c634f3a373',
                '655_b0c4b5cfc184851f773b18548ee9ad730ab3a5d4092ebc11e813e76c94e6500d',
                '655_6fe308aba1bc04440517c9122f99116df29b7160b992a74f730fa89271c7f30e',
                '655_c0aaf897b3556033606d5e900a0a8f2bc62bcce61bb98c926b64972d931c3c23'
            ) then 1 else 0 end as supplier_flag,
        from
            filtered_data
    ), grouped_data as (
        select
            *,
            sum(case when km_since_last_locate > {km_threshold} then 1 else 0 end) over (order by timestamp, latitude, longitude) as location_group
        from
            enriched_data
    )
        select
            id,
            timestamp,
            latitude,
            longitude,
            supply_id,
            location_group as segment,
            sum(km_since_last_locate) over (partition by location_group) as dist_travelled,
            first_value(latitude) over (partition by location_group order by timestamp, latitude, longitude) as start_lat,
            last_value(latitude) over (partition by location_group order by timestamp, latitude, longitude) as end_lat,
            first_value(longitude) over (partition by location_group order by timestamp, latitude, longitude) as start_lon,
            last_value(longitude) over (partition by location_group order by timestamp, latitude, longitude) as end_lon,
            first_value(min_since_last_locate) over (partition by location_group order by timestamp, latitude, longitude) as min_jump,
            first_value(km_since_last_locate) over (partition by location_group order by timestamp, latitude, longitude) as km_jump,
            sum(min_since_last_locate) over (partition by location_group order by timestamp, latitude, longitude) as min_duration,
            speed_flag,
            conflict_flag,
            supplier_flag,
            dupes
        from
            grouped_data
    """
    
    # Execute the query asynchronously
    cursor.execute_async(query)
    query_id = cursor.sfqid
    while conn.is_still_running(conn.get_query_status(query_id)):
        time.sleep(1)
    time.sleep(5)
    
    # Fetch the results
    cursor.get_results_from_sfqid(query_id)
    results = cursor.fetchall()
    
    # Define column names
    columns = [
        'id', 
        'timestamp', 
        'latitude', 
        'longitude', 
        'supply_id', 
        'segment',
        'dist_travelled',
        'start_lat', 
        'end_lat',
        'start_lon',
        'end_lon',
        'min_jump', 
        'km_jump',
        'min_duration',
        'speed_flag',
        'conflict_flag',
        'supplier_flag',
        'dupes'
        ]
    
    # Convert results to DataFrame
    df = pd.DataFrame(results, columns=columns)
    df['fraud'] = None        
    max_segment = df['segment'].max()
    np.max

    grouped_df = df.groupby(['segment']).agg({
        'id': 'count',  # Count total rows
        'min_duration': 'max',
        'dist_travelled': 'max',
        'start_lat': 'max',
        'start_lon': 'max',
        'end_lat': 'max',
        'end_lon': 'max',
        'min_jump': 'max',
        'km_jump': 'max',
        'speed_flag': 'max',
        'conflict_flag': 'max',
        'supplier_flag': 'max',
        'fraud': 'max',
        'supply_id': pd.Series.nunique,
        'dupes': 'sum'
    }).reset_index()
    total_dupes_sum = grouped_df['dupes'].sum()
    grouped_df['quality'] = (
        25 * (np.minimum(grouped_df['supply_id'], 3) / 3) +
        30 * (np.minimum(grouped_df['min_duration'], 60) / 60) + 
        25 * (np.minimum(grouped_df['id'], 50) / 50) + 
        10 * grouped_df['speed_flag'] +
        10 * grouped_df['supplier_flag']
    )
    avg_quality = np.average(grouped_df['quality'], weights=grouped_df['id'])

    stats = {
        'max_segment': max_segment,
        'current_segment': 0,
        'annotated': 0,
        'duplicates': total_dupes_sum - len(df),
        'locates': total_dupes_sum,
        'avg_quality': avg_quality
    }

    st.session_state['stats'] = stats
    st.session_state['df'] = df
    st.session_state['segment_df'] = grouped_df
    st.session_state['annotations'] = {}
    
    # Create an S3 filesystem object with your credentials
    outcome = write_df()
    print(f'csv saved? {outcome}')
    
    # Close the cursor and connection
    cursor.close()
    conn.close()
    return True

# Create color column based on segment values
def get_color(segment):
    colors = {
        'current': [46, 41, 78],  # Hex: #2e294e
        'previous': [244, 96, 54],  # Hex: #f46036
        'next': [27, 153, 139],  # Hex: #1b998b
        'default': [105, 105, 105, 0.25]  # Darker gray with 0.25 opacity
    }
    if segment == st.session_state['stats']['current_segment']:
        return colors['current']
    elif segment == st.session_state['stats']['current_segment'] - 1:
        return colors['previous']
    elif segment == st.session_state['stats']['current_segment'] + 1:
        return colors['next']
    else:
        return colors['default']

# Callbacks
def previous_callback():
    if 0 < st.session_state['stats']['current_segment']:
        st.session_state['stats']['current_segment'] -= 1
        st.session_state['rerun'] = True

def next_callback():
    if st.session_state['stats']['current_segment'] + 1 < st.session_state['stats']['max_segment']:
        st.session_state['stats']['current_segment'] += 1
        st.session_state['rerun'] = True

def valid_callback():
    if st.session_state['stats']['current_segment'] in st.session_state['annotations']:
        current_annotation = st.session_state['annotations'][st.session_state['stats']['current_segment']]
        if current_annotation:
            pass
        else:
            df = st.session_state['df']
            grouped_df = st.session_state['segment_df']
            st.session_state['annotations'][st.session_state['stats']['current_segment']] = True
            df.loc[df['segment'] == st.session_state['stats']['current_segment'], 'fraud'] = True
            grouped_df.loc[grouped_df['segment'] == st.session_state['stats']['current_segment'], 'fraud'] = True

        st.session_state['stats']['current_segment'] = min(st.session_state['stats']['current_segment'] + 1, st.session_state['stats']['max_segment'])
        st.session_state['rerun'] = True
        return
    else:
        df = st.session_state['df']
        grouped_df = st.session_state['segment_df']
        st.session_state['annotations'][st.session_state['stats']['current_segment']] = True
        df.loc[df['segment'] == st.session_state['stats']['current_segment'], 'fraud'] = True
        grouped_df.loc[grouped_df['segment'] == st.session_state['stats']['current_segment'], 'fraud'] = True
        st.session_state['stats']['annotated'] += 1
    
    if len(st.session_state['annotations']) > 10:
        outcome = write_df()
        if outcome:
            st.session_state['annotations'] = {}
    
    st.session_state['stats']['current_segment'] = min(st.session_state['stats']['current_segment'] + 1, st.session_state['stats']['max_segment'])
    st.session_state['rerun'] = True

def invalid_callback():
    if st.session_state['stats']['current_segment'] in st.session_state['annotations']:
        current_annotation = st.session_state['annotations'][st.session_state['stats']['current_segment']]
        if current_annotation:
            df = st.session_state['df']
            grouped_df = st.session_state['segment_df']
            st.session_state['annotations'][st.session_state['stats']['current_segment']] = False
            df.loc[df['segment'] == st.session_state['stats']['current_segment'], 'fraud'] = False
            grouped_df.loc[grouped_df['segment'] == st.session_state['stats']['current_segment'], 'fraud'] = False
        else:
            pass

        st.session_state['stats']['current_segment'] = min(st.session_state['stats']['current_segment'] + 1, st.session_state['stats']['max_segment'])
        st.session_state['rerun'] = True
        return
    else:
        df = st.session_state['df']
        grouped_df = st.session_state['segment_df']
        st.session_state['annotations'][st.session_state['stats']['current_segment']] = False
        df.loc[df['segment'] == st.session_state['stats']['current_segment'], 'fraud'] = False
        grouped_df.loc[grouped_df['segment'] == st.session_state['stats']['current_segment'], 'fraud'] = False
        st.session_state['stats']['annotated'] += 1
    
    if len(st.session_state['annotations']) > 10:
        outcome = write_df()
        if outcome:
            st.session_state['annotations'] = {}
    
    st.session_state['stats']['current_segment'] = min(st.session_state['stats']['current_segment'] + 1, st.session_state['stats']['max_segment'])
    st.session_state['rerun'] = True

def refresh_callback():
    start()
    st.session_state['rerun'] = True

# Function to create a Folium map
def render_map():

    if 'device_id' not in st.session_state:
        st.subheader('Definitions')
        st.text('device id: the device you are looking to analyze')
        st.text('truncation: the number of decimal places in the latitutde and longitude values used to determine duplication')
        st.text('minutes: the minute interval used to determine duplication')
        st.text('km threshold: the km threshold used to create a new segment')
        st.text('segment: a group of locates that are grouped together by their geo-time position')
        return
    else:
        grouped_df = st.session_state['segment_df']
        filtered_df = grouped_df[grouped_df['segment'].isin([x for x in range(max(0, st.session_state['stats']['current_segment'] - 2), min(st.session_state['stats']['current_segment'] + 3, st.session_state['stats']['max_segment']))])].copy()
        filtered_df.loc[:, 'color'] = filtered_df['segment'].apply(get_color)
        filtered_df = filtered_df.sort_values(by=['segment']).reset_index(drop=True)
        target_segment = filtered_df.loc[filtered_df['segment'] == st.session_state['stats']['current_segment']]

        # Calculate the approximate distance in degrees for latitude and longitude

        zoom_level = calculate_zoom_level(target_segment['km_jump'].values[0])
        radius = calculate_radius(zoom_level)

        point_layer = pdk.Layer(
            'ScatterplotLayer',
            filtered_df,
            get_position=['start_lon', 'start_lat'],
            get_color='color',
            get_radius=radius,
            pickable=True
        )

        line_df = pd.DataFrame({
            'source_lon': filtered_df['start_lon'],
            'source_lat': filtered_df['start_lat'],
            'target_lon': filtered_df['start_lon'].shift(-1),
            'target_lat': filtered_df['start_lat'].shift(-1),
            'color': filtered_df['color']
        })

        # Define the line layer using the original filtered_df
        line_layer = pdk.Layer(
            'LineLayer',
            data=line_df,
            get_source_position=['source_lon', 'source_lat'],
            get_target_position=['target_lon', 'target_lat'],
            get_color='color',
            get_width=1,
            pickable=True
        )

        view_state = pdk.ViewState(
            latitude=target_segment['start_lat'].values[0],
            longitude=target_segment['start_lon'].values[0],
            zoom=zoom_level,
            pitch=0,
        )

        deck = pdk.Deck(
            layers=[point_layer, line_layer],
            initial_view_state=view_state,
            map_style='light'
        )

        st.pydeck_chart(deck)
        pass

# Function to create sidebar
def render_sidebar():
    # Sidebar for user inputs
    with st.sidebar:   
        if 'device_id' not in st.session_state:
            device_id = st.text_input("enter device id:", '00000000-0000-0000-0000-000000000000', max_chars=36)
            truncation = st.number_input("enter truncation:", value=4, min_value=3, max_value=6)
            minutes = st.number_input("enter minutes:", value=1, min_value=1, max_value=60)
            km_threshold = st.number_input("enter km threshold:", value=5, min_value=1, max_value=1000)
            if st.button('search'):
                try:
                    uuid.UUID(device_id)
                except ValueError:
                    st.error(f"Device ID entered is not in uuid format")
                    return False
                with st.spinner('getting data from snowflake...'):
                    outcome = start()
                    if outcome:
                        st.session_state['device_id'] = device_id
                        st.session_state['truncation'] = truncation
                        st.session_state['minutes'] = minutes
                        st.session_state['km_threshold'] = km_threshold
                        st.session_state['rerun'] = True 
                    else:
                        q_outcome = query(device_id, minutes, truncation, km_threshold)
                        if q_outcome:
                            st.session_state['device_id'] = device_id
                            st.session_state['truncation'] = truncation
                            st.session_state['minutes'] = minutes
                            st.session_state['km_threshold'] = km_threshold
                            st.session_state['rerun'] = True 
        else:
            st.subheader(f"{st.session_state['device_id']}")
            if 'stats' in st.session_state:
                with st.container(border=True):
                    st.text(f"locates: {st.session_state['stats']['locates']:,.0f}")
                    st.text(f"duplication: {st.session_state['stats']['duplicates']/st.session_state['stats']['locates']*100:,.1f}%")
                    st.text(f"position ({st.session_state['stats']['current_segment']:,.0f}/{st.session_state['stats']['max_segment']:,.0f})")
                    st.progress(st.session_state['stats']['current_segment']/st.session_state['stats']['max_segment'])
                    st.text(f"reviewed ({st.session_state['stats']['annotated']:,.0f}/{st.session_state['stats']['max_segment']:,.0f})")
                    st.progress(st.session_state['stats']['annotated']/st.session_state['stats']['max_segment']) 
 

            with st.container(height=300):
                st.text('navigation')
                load_key_css()
                col1, col2 = st.columns([4, 1])
                with col1:
                    st.text('mark current locate good')
                    st.text('mark current locate bad')
                    st.text('go to next locate')
                    st.text('go to previous locate')
                with col2: 
                    key("↑")
                    key("↓")
                    key("→")
                    key("←")
                st.divider()
                col3, col4, col5, col6 = st.columns(4)
                with col3:
                    streamlit_shortcuts.button('↑', on_click=valid_callback, shortcut="ArrowUp")
                with col4:
                    streamlit_shortcuts.button('↓', on_click=invalid_callback, shortcut="ArrowDown")   
                with col5:
                    streamlit_shortcuts.button("→", on_click=next_callback, shortcut="ArrowRight")
                with col6:
                    streamlit_shortcuts.button("←", on_click=previous_callback, shortcut="ArrowLeft")  

def render_stats():
    df = st.session_state['df']
    grouped_df = st.session_state['segment_df']
    col1, col2, col3 = st.columns(3)  
    with col1:
        with st.container(height=325):
            if st.session_state['stats']['current_segment'] - 1 >= 0:
                target_segment = grouped_df.loc[grouped_df['segment'] == st.session_state['stats']['current_segment'] - 1]
                if st.session_state['stats']['current_segment'] - 1 in st.session_state['annotations']:
                    if st.session_state['annotations'][st.session_state['stats']['current_segment'] - 1] == True:
                        st.subheader('previous ✅')
                    else:
                        st.subheader('previous ❌')
                else:
                    target_segment = grouped_df.loc[grouped_df['segment'] == st.session_state['stats']['current_segment'] - 1]
                    fraud_val = target_segment['fraud'].values[0]
                    if fraud_val == True:
                        st.subheader('previous ✅')
                    elif fraud_val == False:
                        st.subheader('previous ❌')
                    else:
                        st.subheader('previous ❔')
                scol1, scol2 = st.columns(2)
                with scol1:
                    st.text(f"locates: {target_segment['id'].values[0]:,.0f}")
                    st.text(f"time seen: {format_minutes(target_segment['min_duration'].values[0])}")
                with scol2:
                    st.text(f"speed: {format_speed(target_segment['km_jump'].values[0], target_segment['min_jump'].values[0])}")
                    st.text(f"apps: {target_segment['supply_id'].values[0]:,.0f} apps")
                if target_segment['quality'].values[0] >= 0:
                    st.text(f"quality: {target_segment['quality'].values[0]:,.1f}%")
                    st.progress(target_segment['quality'].values[0]/100) 
                else:
                    st.text(f"quality: 0%")
                    st.progress(0) 
                sub1, sub2, sub3 = st.columns(3)
                with sub1:
                    if int(target_segment['speed_flag'].values[0]) == 1:
                        st.metric('speed', '✈️')
                with sub2:
                    if int(target_segment['conflict_flag'].values[0]) == 1:
                        st.metric('teleport', '❓')
                with sub3:
                    if int(target_segment['supplier_flag'].values[0]) == 1:
                        st.metric('bad app', '⚠️')
    with col2:
        with st.container(height=325):
            target_segment = grouped_df.loc[grouped_df['segment'] == st.session_state['stats']['current_segment']]
            if st.session_state['stats']['current_segment'] in st.session_state['annotations']:
                if st.session_state['annotations'][st.session_state['stats']['current_segment']] == True:
                    st.subheader('current ✅')
                else:
                    st.subheader('current ❌')
            else:
                fraud_val = target_segment['fraud'].values[0]
                if fraud_val == True:
                    st.subheader('current ✅')
                elif fraud_val == False:
                    st.subheader('current ❌')
                else:
                    st.subheader('current ❔')
            scol1, scol2 = st.columns(2)
            with scol1:
                st.text(f"locates: {target_segment['id'].values[0]:,.0f}")
                st.text(f"time seen: {format_minutes(target_segment['min_duration'].values[0])}")
            with scol2:
                st.text(f"speed: {format_speed(target_segment['km_jump'].values[0], target_segment['min_jump'].values[0])}")
                st.text(f"apps: {target_segment['supply_id'].values[0]:,.0f} apps")
            if target_segment['quality'].values[0] >= 0:
                st.text(f"quality: {target_segment['quality'].values[0]:,.1f}%")
                st.progress(target_segment['quality'].values[0]/100) 
            else:
                st.text(f"quality: 0%")
                st.progress(0) 
            sub1, sub2, sub3 = st.columns(3)
            with sub1:
                if int(target_segment['speed_flag'].values[0]) == 1:
                    st.metric('speed', '✈️')
            with sub2:
                if int(target_segment['conflict_flag'].values[0]) == 1:
                    st.metric('teleport', '❓')
            with sub3:
                if int(target_segment['supplier_flag'].values[0]) == 1:
                    st.metric('bad app', '⚠️')
    with col3:
        with st.container(height=325):
            if st.session_state['stats']['current_segment'] + 1 <= st.session_state['stats']['max_segment']:
                target_segment = grouped_df.loc[grouped_df['segment'] == st.session_state['stats']['current_segment'] + 1]
                if st.session_state['stats']['current_segment'] + 1 in st.session_state['annotations']:
                    if st.session_state['annotations'][st.session_state['stats']['current_segment'] + 1] == True:
                        st.subheader('next ✅')
                    else:
                        st.subheader('next ❌')
                else:
                    fraud_val = target_segment['fraud'].values[0]
                    if fraud_val == True:
                        st.subheader('next ✅')
                    elif fraud_val == False:
                        st.subheader('next ❌')
                    else:
                        st.subheader('next ❔')
                scol1, scol2 = st.columns(2)
                with scol1:
                    st.text(f"locates: {target_segment['id'].values[0]:,.0f}")
                    st.text(f"time seen: {format_minutes(target_segment['min_duration'].values[0])}")
                with scol2:
                    st.text(f"speed: {format_speed(target_segment['km_jump'].values[0], target_segment['min_jump'].values[0])}")
                    st.text(f"apps: {target_segment['supply_id'].values[0]:,.0f} apps")
                if target_segment['quality'].values[0] >= 0:
                    st.text(f"quality: {target_segment['quality'].values[0]:,.1f}%")
                    st.progress(target_segment['quality'].values[0]/100) 
                else:
                    st.text(f"quality: 0%")
                    st.progress(0)  
                sub1, sub2, sub3 = st.columns(3)
                with sub1:
                    if int(target_segment['speed_flag'].values[0]) == 1:
                        st.metric('speed', '✈️')
                with sub2:
                    if int(target_segment['conflict_flag'].values[0]) == 1:
                        st.metric('teleport', '❓')
                with sub3:
                    if int(target_segment['supplier_flag'].values[0]) == 1:
                        st.metric('bad app', '⚠️')

# Streamlit app
def main():
    render_sidebar()

    render_map()
    if 'stats' in st.session_state:
        render_stats()

    # Add keyboard shortcuts
    if st.session_state.get('rerun', False):
        st.session_state['rerun'] = False
        st.rerun()
    
if __name__ == "__main__":
    st.set_page_config(layout="wide")
    main()




