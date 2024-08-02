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
import plotly.graph_objects as go
from helpers.misc import write_df, format_minutes, calculate_zoom_level, calculate_radius, format_speed, add_meta


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
                'timestamp': 'min',
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
                'has_742': 'max',
                'dupes': 'sum'
            }).reset_index()

            grouped_df['min_duration'] = grouped_df['min_duration'].fillna(0)
            grouped_df['min_jump'] = grouped_df['min_jump'].fillna(0)

            grouped_df['quality'] = (
                20 * (np.minimum(grouped_df['has_742'], 3) / 3) +
                30 * (np.minimum(grouped_df['min_duration'] - grouped_df['min_jump'], 240) / 240) + 
                20 * (np.minimum(grouped_df['id'], 100) / 100) + 
                30 * grouped_df['speed_flag']
            )

            grouped_df = grouped_df.sort_values(by=['segment']).reset_index(drop=True)

            avg_quality = np.average(grouped_df['quality'], weights=grouped_df['id'])

            null_fraud_row = grouped_df[grouped_df['fraud'].isnull()].head(1)

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
            st.session_state['start'] = time.time()
            
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
    df['has_742'] = 0
    df.loc[df['supply_id'] == '742', 'has_742'] = 1  
    df.loc[df['has_742'] == 1, 'fraud'] = True      
    max_segment = df['segment'].max()

    grouped_df = df.groupby(['segment']).agg({
        'id': 'count',  # Count total rows
        'timestamp': 'min',
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
        'has_742': 'max',
        'dupes': 'sum'
    }).reset_index()
    grouped_df['min_duration'] = grouped_df['min_duration'].fillna(0)
    grouped_df['min_jump'] = grouped_df['min_jump'].fillna(0)
    total_dupes_sum = grouped_df['dupes'].sum()
    grouped_df['quality'] = (
        20 * (np.minimum(grouped_df['has_742'], 3) / 3) +
        30 * (np.minimum(grouped_df['min_duration'] - grouped_df['min_jump'], 240) / 240) + 
        20 * (np.minimum(grouped_df['id'], 100) / 100) + 
        30 * grouped_df['speed_flag']
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
    st.session_state['start'] = time.time()
    
    # Create an S3 filesystem object with your credentials
    outcome = write_df()
    print(f'csv saved? {outcome}')
    
    # Close the cursor and connection
    cursor.close()
    conn.close()
    return True

# Create color column based on segment values
def get_map_color(segment):
    colors = {
        'current': '#254441',
        'previous': '#ff6f59',  
        'next': '#43aa8b'
    }
    if segment == st.session_state['stats']['current_segment']:
        return colors['current']
    elif segment < st.session_state['stats']['current_segment']:
        return colors['previous']
    else:
        return colors['next']
    
def get_matrix_color(fraud):
    if pd.isna(fraud):
        return 'rgba(163, 186, 195, .1)'  # Example with 50% opacity
    elif fraud == True:
        return 'rgba(67, 170, 139, .75)'  # Example with 80% opacity
    else:
        return 'rgba(255, 111, 89, .75)'  # Example with 70% opacity

# Callbacks
def previous_callback():
    st.session_state['stats']['current_segment'] = max(0, st.session_state['stats']['current_segment']-1)
    st.session_state['rerun'] = True

def next_callback():
    st.session_state['stats']['current_segment'] = min(st.session_state['stats']['max_segment'], st.session_state['stats']['current_segment']+1)
    st.session_state['rerun'] = True

def valid_callback():
    if st.session_state['stats']['current_segment'] in st.session_state['annotations']:
        current_annotation = st.session_state['annotations'][st.session_state['stats']['current_segment']]
        if current_annotation:
            pass
        else:
            st.session_state['annotations'][st.session_state['stats']['current_segment']] = True
            st.session_state['df'].loc[st.session_state['df']['segment'] == st.session_state['stats']['current_segment'], 'fraud'] = True
            st.session_state['segment_df'].loc[st.session_state['segment_df']['segment'] == st.session_state['stats']['current_segment'], 'fraud'] = True
        st.session_state['stats']['current_segment'] = min(st.session_state['stats']['current_segment'] + 1, st.session_state['stats']['max_segment'])    
        st.session_state['rerun'] = True
        return
    else:
        st.session_state['annotations'][st.session_state['stats']['current_segment']] = True
        st.session_state['df'].loc[st.session_state['df']['segment'] == st.session_state['stats']['current_segment'], 'fraud'] = True
        st.session_state['segment_df'].loc[st.session_state['segment_df']['segment'] == st.session_state['stats']['current_segment'], 'fraud'] = True
        st.session_state['stats']['annotated'] = st.session_state['stats']['max_segment'] - st.session_state['segment_df']['fraud'].isna().sum()
        if st.session_state['stats']['annotated'] == st.session_state['stats']['max_segment']:
            st.balloons()
            time.sleep(5)
            add_meta()

    if len(st.session_state['annotations']) > 10:
        outcome = write_df()
        if outcome:
            st.session_state['annotations'] = {}
    elif int(st.session_state['stats']['annotated']) == int(st.session_state['stats']['max_segment']):
        outcome = write_df()
        if outcome:
            st.session_state['annotations'] = {}
    
    st.session_state['stats']['current_segment'] = min(st.session_state['stats']['current_segment'] + 1, st.session_state['stats']['max_segment'])
    st.session_state['rerun'] = True

def invalid_callback():
    if st.session_state['stats']['current_segment'] in st.session_state['annotations']:
        current_annotation = st.session_state['annotations'][st.session_state['stats']['current_segment']]
        if current_annotation:
            st.session_state['annotations'][st.session_state['stats']['current_segment']] = False
            st.session_state['df'].loc[st.session_state['df']['segment'] == st.session_state['stats']['current_segment'], 'fraud'] = False
            st.session_state['segment_df'].loc[st.session_state['segment_df']['segment'] == st.session_state['stats']['current_segment'], 'fraud'] = False
        else:
            pass

        st.session_state['stats']['current_segment'] = min(st.session_state['stats']['current_segment'] + 1, st.session_state['stats']['max_segment'])
        st.session_state['rerun'] = True
        return
    else:
        st.session_state['annotations'][st.session_state['stats']['current_segment']] = False
        st.session_state['df'].loc[st.session_state['df']['segment'] == st.session_state['stats']['current_segment'], 'fraud'] = False
        st.session_state['segment_df'].loc[st.session_state['segment_df']['segment'] == st.session_state['stats']['current_segment'], 'fraud'] = False
        st.session_state['stats']['annotated'] = st.session_state['stats']['max_segment'] - st.session_state['segment_df']['fraud'].isna().sum()
        if st.session_state['stats']['annotated'] == st.session_state['stats']['max_segment']:
            st.balloons()
            time.sleep(5)
            add_meta()

    if len(st.session_state['annotations']) > 10:
        outcome = write_df()
        if outcome:
            st.session_state['annotations'] = {}
    elif int(st.session_state['stats']['annotated']) == int(st.session_state['stats']['max_segment']):
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
        with st.container(border=True):
            st.subheader('definitions')
            st.text('device id: the device you are looking to analyze')
            st.text('truncation: the number of decimal places in the latitutde and longitude values used to determine duplication')
            st.text('minutes: the minute interval used to determine duplication')
            st.text('km threshold: the km threshold used to create a new segment')
            st.text('segment: a group of locates that are grouped together by their geographical position')
        s3 = s3fs.S3FileSystem(
            key=st.secrets["aws"]["PUBLIC_KEY"],
            secret=st.secrets["aws"]["PRIVATE_KEY"]
        )

        # Specify the S3 path to your CSV file
        s3_path = f"s3://a6dev/annotations.csv"

        # Read the CSV file directly from S3 into a pandas DataFrame
        with s3.open(s3_path, 'rb') as f:
            df = pd.read_csv(f)
            result = df.groupby('user_id').agg(
                devices=('device_id', 'nunique'),
                total_seconds=('seconds', 'sum'),
                total_locates=('locates', 'sum')
            ).reset_index()

            # Order by total_locates in descending order
            result = result.sort_values(by='total_locates', ascending=False)
            result['rank'] = result.index + 1
            result['minutes'] = (result['total_seconds'] / 60).round().astype(int)

            # Rename the columns
            result = result[['rank', 'user_id', 'devices', 'minutes', 'total_locates']]
            result.columns = ['Rank', 'User', 'Devices', 'Minutes', 'Locates']
        with st.container(border=True):
            st.subheader('leaderboard')
            st.table(result.set_index('Rank'))
        return
    else:
        grouped_df = st.session_state['segment_df']
        filtered_df = grouped_df[grouped_df['segment'].isin([x for x in range(max(0, st.session_state['stats']['current_segment'] - 51), min(st.session_state['stats']['current_segment'] + 51, st.session_state['stats']['max_segment']+1))])].copy()
        filtered_df = filtered_df.sort_values(by=['segment']).reset_index(drop=True)
        current_segment = filtered_df.loc[filtered_df['segment'] == st.session_state['stats']['current_segment']]
        current_segment = filtered_df.loc[filtered_df['segment'] == st.session_state['stats']['current_segment']+1]
        filtered_df = grouped_df[grouped_df['segment'].isin([x for x in range(max(0, st.session_state['stats']['current_segment'] - 51), min(st.session_state['stats']['current_segment'] + 51, st.session_state['stats']['max_segment']+1))])].copy()
        filtered_df = filtered_df.sort_values(by=['segment']).reset_index(drop=True)

        if pd.isna(current_segment['km_jump'].values[0]):
            zoom_level = calculate_zoom_level(next_segment['km_jump'].values[0])
            radius = calculate_radius(zoom_level)
        else:
            # Calculate the approximate distance in degrees for latitude and longitude
            zoom_level = calculate_zoom_level(current_segment['km_jump'].values[0])
            radius = calculate_radius(zoom_level)

        points = []
        lines = []

        cp = []

        for _, segment in filtered_df.iterrows():
            segment_color = [37, 68, 65]  # Default color (current segment)
            
            if segment['segment'] < st.session_state['stats']['current_segment']:
                segment_color = [255, 111, 89]  # Previous segments
            elif segment['segment'] > st.session_state['stats']['current_segment']:
                segment_color = [67, 170, 139]  # Next segments
            
            # Add start point
            if segment_color == [37, 68, 65]:
                cp.append({
                    'latitude': segment['start_lat'],
                    'longitude': segment['start_lon'],
                    'color': segment_color
                })
                
                # Add end point
                cp.append({
                    'latitude': segment['end_lat'],
                    'longitude': segment['end_lon'],
                    'color': segment_color
                })
            else:
                points.append({
                    'latitude': segment['start_lat'],
                    'longitude': segment['start_lon'],
                    'color': segment_color
                })
                
                # Add end point
                points.append({
                    'latitude': segment['end_lat'],
                    'longitude': segment['end_lon'],
                    'color': segment_color
                })
            
            # Add line for the segment
            lines.append({
                'source_lon': segment['start_lon'],
                'source_lat': segment['start_lat'],
                'target_lon': segment['end_lon'],
                'target_lat': segment['end_lat'],
                'color': segment_color
            })
            
            # Add line connecting to the next segment if it exists
            if _ < len(filtered_df) - 1:
                next_segment = filtered_df.iloc[_ + 1]
                lines.append({
                    'source_lon': segment['end_lon'],
                    'source_lat': segment['end_lat'],
                    'target_lon': next_segment['start_lon'],
                    'target_lat': next_segment['start_lat'],
                    'color': segment_color
                })

        point_layer = pdk.Layer(
            'ScatterplotLayer',
            points,
            get_position='[longitude, latitude]',
            get_color='color',
            get_radius=radius,
            pickable=True
        )

        cpoint_layer = pdk.Layer(
            'ScatterplotLayer',
            cp,
            get_position='[longitude, latitude]',
            get_color='color',
            get_radius=radius*2,
            pickable=True
        )

        # Define the line layer using the original filtered_df
        line_layer = pdk.Layer(
            'LineLayer',
            data=lines,
            get_source_position=['source_lon', 'source_lat'],
            get_target_position=['target_lon', 'target_lat'],
            get_color='color',
            get_width=1,
            pickable=True
        )

        view_state = pdk.ViewState(
            latitude=current_segment['start_lat'].values[0],
            longitude=current_segment['start_lon'].values[0],
            zoom=zoom_level,
            pitch=0,
        )

        deck = pdk.Deck(
            layers=[cpoint_layer, point_layer, line_layer],
            initial_view_state=view_state,
            map_style='light'
        )

        st.pydeck_chart(deck)
        pass

# Function to create sidebar
def render_sidebar():
    # Sidebar for user inputs
    with st.sidebar:   
        if 'df' not in st.session_state:
            user_id = st.text_input('enter username:', 'w71')
            device_id = st.text_input("enter device id:", '00000000-0000-0000-0000-000000000000', max_chars=36)
            truncation = st.number_input("enter truncation:", value=4, min_value=3, max_value=6)
            minutes = st.number_input("enter minutes:", value=1, min_value=1, max_value=60)
            km_threshold = st.number_input("enter km threshold:", value=10, min_value=1, max_value=1000)
            if st.button('search'):
                try:
                    uuid.UUID(device_id)
                    st.session_state['user_id'] = user_id
                    st.session_state['device_id'] = device_id
                    st.session_state['truncation'] = truncation
                    st.session_state['minutes'] = minutes
                    st.session_state['km_threshold'] = km_threshold
                except ValueError:
                    st.error(f"Device ID entered is not in uuid format")
                    return False
                with st.spinner('checking if data exists...'):
                    outcome = start()
                if outcome:
                    st.session_state['rerun'] = True 
                else:
                    with st.spinner('querying data from snowflake...'):
                        q_outcome = query(device_id, minutes, truncation, km_threshold)
                    if q_outcome:
                        st.session_state['rerun'] = True 
        else:
            st.subheader(f"{st.session_state['device_id']}")
            if 'stats' in st.session_state:
                with st.container(border=True):
                    st.text(f"locates: {st.session_state['stats']['locates']:,.0f}")
                    st.text(f"duplication: {st.session_state['stats']['duplicates']/st.session_state['stats']['locates']*100:,.1f}%")
                    st.text(f"quality: {st.session_state['stats']['avg_quality']:,.1f}%")
                    st.text(f"position: {st.session_state['stats']['current_segment']/st.session_state['stats']['max_segment']*100:,.1f}%")
                    st.progress(st.session_state['stats']['current_segment']/st.session_state['stats']['max_segment'])
                    st.text(f"reviewed: {st.session_state['stats']['annotated']/st.session_state['stats']['max_segment']*100:,.1f}%")
                    st.progress(st.session_state['stats']['annotated']/st.session_state['stats']['max_segment']) 
                with st.container(border=True):
                    grouped_df = st.session_state['segment_df']
                    filtered_df = grouped_df[grouped_df['segment'].isin([x for x in range(max(0, st.session_state['stats']['current_segment'] - 51), min(st.session_state['stats']['current_segment'] + 51, st.session_state['stats']['max_segment']+1))])].copy()
                    filtered_df.loc[:, 'matrix_color'] = filtered_df['fraud'].apply(get_matrix_color)
                    filtered_df.loc[filtered_df['segment'] == st.session_state['stats']['current_segment'], 'matrix_color'] = 'rgba(255, 209, 102, 255)'
                    filtered_df['size'] = 8
                    filtered_df.loc[filtered_df['segment'] == st.session_state['stats']['current_segment'], 'size'] = 24
                    
                    scatter = go.Scatter3d(
                        x=filtered_df['start_lon'],
                        y=filtered_df['start_lat'],
                        z=filtered_df['timestamp'],
                        mode='markers',
                        marker=dict(
                            size=filtered_df['size'],
                            color=filtered_df['matrix_color']
                        )
                    )

                    # Create the line trace
                    line = go.Scatter3d(
                        x=filtered_df['start_lon'],
                        y=filtered_df['start_lat'],
                        z=filtered_df['timestamp'],
                        mode='lines',
                        line=dict(color='rgba(100, 100, 100, 0.5)', width=4)
                    )

                    # Combine both traces
                    fig = go.Figure(data=[scatter, line])

                    # Update layout
                    fig.update_layout(
                        showlegend=False,  # This removes the legend
                        scene=dict(
                            xaxis_title='lon',
                            yaxis_title='lat',
                            zaxis_title='',
                            xaxis=dict(title_font=dict(size=14)),
                            yaxis=dict(title_font=dict(size=14)),
                            zaxis=dict(title_font=dict(size=14)),
                            dragmode='orbit'
                        ),
                        scene_camera=dict(
                            up=dict(x=0, y=0, z=1),
                            center=dict(x=0, y=0, z=0),
                            eye=dict(x=1.5, y=1.5, z=1.5)
                        )
                    )

                    st.text('locate explorer')
                    # Display the plot in Streamlit
                    st.plotly_chart(fig)

            # Nav
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
                    st.text(f"apps: {target_segment['supply_id'].values[0]:,.0f}")
                    st.text(f"quality: {target_segment['quality'].values[0]:,.0f}%")
                with scol2:
                    st.text(f"speed: {format_speed(target_segment['km_jump'].values[0], target_segment['min_jump'].values[0])}")
                    st.text(f"time seen: {format_minutes(target_segment['min_duration'].values[0]-target_segment['min_jump'].values[0])}")
                    st.text(f"distance: {target_segment['dist_travelled'].values[0]-target_segment['km_jump'].values[0]:,.0f}km")
                if target_segment['quality'].values[0] >= 0:          
                    st.progress(target_segment['quality'].values[0]/100) 
                else:
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
                st.text(f"apps: {target_segment['supply_id'].values[0]:,.0f}")
                st.text(f"quality: {target_segment['quality'].values[0]:,.0f}%")
            with scol2:
                st.text(f"speed: {format_speed(target_segment['km_jump'].values[0], target_segment['min_jump'].values[0])}")
                st.text(f"time seen: {format_minutes(target_segment['min_duration'].values[0]-target_segment['min_jump'].values[0])}")
                st.text(f"distance: {target_segment['dist_travelled'].values[0]-target_segment['km_jump'].values[0]:,.0f}km")
            if target_segment['quality'].values[0] >= 0:          
                st.progress(target_segment['quality'].values[0]/100) 
            else:
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
                    st.text(f"apps: {target_segment['supply_id'].values[0]:,.0f}")
                    st.text(f"quality: {target_segment['quality'].values[0]:,.0f}%")
                with scol2:
                    st.text(f"speed: {format_speed(target_segment['km_jump'].values[0], target_segment['min_jump'].values[0])}")
                    st.text(f"time seen: {format_minutes(target_segment['min_duration'].values[0]-target_segment['min_jump'].values[0])}")
                    st.text(f"distance: {target_segment['dist_travelled'].values[0]-target_segment['km_jump'].values[0]:,.0f}km")
                if target_segment['quality'].values[0] >= 0:         
                    st.progress(target_segment['quality'].values[0]/100) 
                else:
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






