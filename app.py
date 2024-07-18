import streamlit as st
import os
import pandas as pd
import uuid
import time
import snowflake.connector
import pydeck as pdk
import streamlit_shortcuts
from streamlit_extras.keyboard_text import key, load_key_css


# API Functions
def start(device_id):
    file_path = os.path.join('data', device_id, 'results.csv')
    
    # Check if the file exists
    if not os.path.exists(file_path):
        print(f"Device ID that is being read doesn't exist")
    
    try:
        # Load the CSV file into a DataFrame
        df = pd.read_csv(file_path)
        
        # Find the first row with a null value in the 'fraud' column
        null_fraud_row = df[df['fraud'].isnull()].head(1)
        
        if null_fraud_row.empty:
            print(f"Device ID that is being annotated doesn't exist")
        
        # Get the index of the first row with a null fraud value
        starting_row_number = int(null_fraud_row.index[0])
        
        return starting_row_number, len(df)
    except Exception as e:
        print(f"Unexpected error: {e}")

def read_data(device_id, row_number):
    file_path = os.path.join('data', device_id, 'results.csv')
    
    # Check if the file exists
    if not os.path.exists(file_path):
        print(f"Device ID that is being read doesn't exist")
    try:
        df = pd.read_csv(file_path)
        
        # Check if the row number is within the range
        if row_number < 0 or row_number >= len(df):
            print(f"Row number {row_number} is out of range.")
        
        # Calculate the start and end range for slicing
        start_index = max(row_number - 50, 0)
        end_index = min(row_number + 50, len(df) - 1)

        df['current'] = False
        df.at[row_number, 'current'] = True
        total_rows = len(df)

        stats = {
            'rows': total_rows,
            'current': row_number,
            'viewed': total_rows - int(df['fraud'].isna().sum())
        }
        
        # Slice the DataFrame
        sliced_df = df.iloc[start_index:end_index+1]

        sliced_df = sliced_df.replace({float('inf'): None, float('-inf'): None}).fillna('')
        
        # Convert the sliced DataFrame to a dictionary
        result = sliced_df.to_dict(orient='records')
        
        return result, stats
    except Exception as e:
        print(f"Unexpected error: {e}")

def annotate(device_id, start_row, end_row, validity):
    file_path = os.path.join('data', device_id, 'results.csv')
    
    # Check if the file exists
    if not os.path.exists(file_path):
        print(f"File {file_path} does not exist.")
        print(f"Device ID that is being annotated doesn't exist")
    
    try:
        # Load the CSV file into a DataFrame
        df = pd.read_csv(file_path)
        
        for i in range(start_row, end_row + 1):            
            # Update the 'fraud' column for the specified row
            df.at[int(i), 'fraud'] = validity
        
        # Save the updated DataFrame back to the CSV file
        df.to_csv(file_path, index=False)
        
        return True
    except Exception as e:
        print(f"Unexpected error: {e}")

def get_data(device_id, truncation, minutes):
    try:
        uuid.UUID(device_id)
    except ValueError:
        print(f"Device ID entered is not in uuid format")
    
    # Define the file path
    file_path = os.path.join('data', device_id, 'results.csv')

    if os.path.exists(file_path):
        df = pd.read_csv(file_path)
        total_rows = len(df)
        null_fraud_row = df[df['fraud'].isnull()].head(1)
        starting_row_number = int(null_fraud_row.index[0])

        stats = {
            'rows': total_rows,
            'current': starting_row_number,
            'viewed': total_rows - int(df['fraud'].isna().sum())
        }
        print(stats)
        return stats
    
    # Create directories if they do not exist
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    
    try:
        # Connect to Snowflake
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
                horizontal_accuracy,
                supply_id,
                ip_address,
                created_at,
                md5_hex(concat(time_slice(timestamp, {minutes}, 'minute'), trunc(latitude, {truncation}), trunc(longitude, {truncation}))) as rh,
                row_number() over (partition by rh order by created_at) as rn
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
                horizontal_accuracy,
                supply_id,
                ip_address
            from
                all_data
            where
                rn = 1
        )
        select
            id,
            timestamp,
            latitude,
            longitude,
            horizontal_accuracy,
            supply_id,
            ip_address,
            datediff('minutes', lag(timestamp) over (order by timestamp, latitude, longitude), timestamp) as min_since_last_locate,
            haversine(latitude, longitude, lag(latitude) over (order by timestamp, latitude, longitude), lag(longitude) over (order by timestamp, latitude, longitude)) as km_since_last_locate,
            case when min_since_last_locate > 1440 then true else false end as time_flag,
            case when km_since_last_locate > 100 then true else false end as distance_flag,
            case when km_since_last_locate / (case when min_since_last_locate = 0 then 1 else min_since_last_locate end) > 15 then true else false end as speed_flag,
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
            ) then true else false end as supplier_flag
        from
            filtered_data
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
        columns = ['id', 'timestamp', 'latitude', 'longitude', 'horizontal_accuracy', 'supply_id', 'ip_address', 'min_since_last_locate', 'km_since_last_locate', 'time_flag', 'distance_flag', 'speed_flag', 'supplier_flag']
        
        # Convert results to DataFrame
        df = pd.DataFrame(results, columns=columns)

        stats = {
            'rows': len(df),
            'current': 0,
            'viewed': 0
        }
        
        # Add 'fraud' column with all values as None
        df['fraud'] = None
        df['ip_address'] = df['ip_address'].replace('<nil>', '')
        df['horizontal_accuracy'] = df['horizontal_accuracy'].replace('', 0).fillna(0).astype(float)
        
        df = df.sort_values(by=['timestamp', 'latitude', 'longitude'])
        
        # Save DataFrame to CSV in the 'data/{device_id}' folder
        df.to_csv(file_path, index=False)
        
        # Close the cursor and connection
        cursor.close()
        conn.close()
        
        return stats
    except Exception as e:
        print(f"Unexpected error: {e}")


# APP Functions
def calculate_zoom(max_distance_km):
    # A simple heuristic to determine zoom level based on distance
    if max_distance_km > 200:
        return 5
    elif max_distance_km > 50:
        return 6
    elif max_distance_km > 20:
        return 7
    elif max_distance_km > 10:
        return 8
    else:
        return 9

def calculate_radius(zoom_level):
    # Adjust the radius based on the zoom level
    base_radius = 100
    return base_radius * (14 - zoom_level + 1)   # Example scaling formula

# Function to create a Folium map
def render_map():
    if 'device_id' in st.session_state:
        if 'stats' not in st.session_state or 'current' not in st.session_state['stats']:
            st_rn = start(st.session_state['device_id'])
            st.session_state['stats']['current'] = st_rn  # Initialize with defaults

        data, stats = read_data(st.session_state['device_id'], st.session_state['stats']['current'])
        st.session_state['stats'].update(stats)

        map_center = [data[min(50, st.session_state['stats']['current'])]['latitude'], data[min(50, st.session_state['stats']['current'])]['longitude']]
        if st.session_state['stats']['current'] == 0:
            max_distance_km = 10
        else:
            max_distance_km = data[min(50, st.session_state['stats']['current'])].get('km_since_last_locate', 0)
        
        dynamic_zoom = calculate_zoom(max_distance_km)
        dynamic_radius = calculate_radius(dynamic_zoom)

        points = []
        for idx, entry in enumerate(data):
            if idx == 50:
                entry['color'] = [0, 0, 255, 255]  # Blue color
                entry['radius'] = dynamic_radius * 4  # Larger radius for the 50th point
            elif idx <= 50:
                opacity = int(255 * (1 - (50 - idx) / 50))
                entry['color'] = [255, 0, 0, opacity]  # Red with decreasing opacity
                entry['radius'] = dynamic_radius
            elif idx > 50 and idx <= 100:
                green_intensity = int(255 * (1 - (idx - 50) / 50))
                entry['color'] = [0, green_intensity, 0, 255]  # Green with decreasing intensity
                entry['radius'] = dynamic_radius
            else:
                entry['color'] = [0, 0, 255, 255]  # Default blue color for other points
                entry['radius'] = dynamic_radius

            points.append({
                'latitude': entry['latitude'],
                'longitude': entry['longitude'],
                'color': entry['color'],
                'radius': entry['radius']
            })

        # Define the layer for the points
        point_layer = pdk.Layer(
            'ScatterplotLayer',
            data=points,
            get_position='[longitude, latitude]',
            get_radius='radius',
            get_fill_color='color',
            pickable=True
        )

        # Define the layer for the lines
        line_data = []
        for i in range(len(points) - 1):
            if points[i]['color'][0] == 255 or points[i + 1]['color'][0] == 255:
                color = [255, 0, 0]  # Red line
            elif points[i]['color'][1] > 0 and points[i]['color'][1] <= 255:
                color = [0, points[i]['color'][1], 0]  # Green line
            else:
                color = [0, 0, 255]  # Blue line, although we want to avoid this case

            line_data.append({
                'start': [points[i]['longitude'], points[i]['latitude']],
                'end': [points[i + 1]['longitude'], points[i + 1]['latitude']],
                'color': color
            })

        line_layer = pdk.Layer(
            'LineLayer',
            data=line_data,
            get_source_position='start',
            get_target_position='end',
            get_color='color',
            get_width=2,
        )

        view_state = pdk.ViewState(
            latitude=map_center[0],
            longitude=map_center[1],
            zoom=dynamic_zoom,
            pitch=0,
        )

        # Create the deck.gl map
        deck = pdk.Deck(
            layers=[point_layer, line_layer],
            initial_view_state=view_state,
            map_style='light',
            tooltip={"text": "{latitude}, {longitude}"}
        )
        st.pydeck_chart(deck)
        st.session_state['current_row'] = data[min(50, st.session_state['stats']['current'])]

# Function to create sidebar
def render_sidebar():
    # Sidebar for user inputs
    with st.sidebar:
        if 'device_id' not in st.session_state:
            device_id = st.text_input("enter device id:", '00000000-0000-0000-0000-000000000000', max_chars=36)
            truncation = st.number_input("enter truncation:", value=4, min_value=3, max_value=5)
            minutes = st.number_input("enter minutes:", value=5, min_value=1, max_value=60)
            if st.button('search'):
                with st.spinner('getting data from snowflake...'):
                    stats = get_data(device_id, truncation, minutes)
                if stats['rows'] > 0:
                    st.session_state['stats'] = stats
                    st.session_state['current_row'] = {}
                    st.session_state['device_id'] = device_id
                    st.session_state['rerun'] = True
                    
        else:
            st.subheader(f"{st.session_state['device_id']}")
            if 'stats' in st.session_state:
                with st.container(border=True):
                    if st.session_state['current_row'] != {}:
                        st.text(f"lat: {st.session_state['current_row']['latitude']}")
                        st.text(f"lon: {st.session_state['current_row']['longitude']}")
                        st.text(f"time: {st.session_state['current_row']['timestamp']}")
                        st.text(f"acc: {st.session_state['current_row']['horizontal_accuracy']}")
                        st.text(f"ip: {st.session_state['current_row']['ip_address']}")
                        if st.session_state['current_row']['fraud'] == False:
                            st.text(f"fraud: ❌")
                        elif st.session_state['current_row']['fraud'] == True:
                            st.text(f"fraud: ✅")
                        else:
                            st.text(f"fraud: ❓")
                    st.text(f"progress ({st.session_state['stats']['current']:,.0f}/{st.session_state['stats']['rows']:,.0f})")
                    progress_bar = st.progress(st.session_state['stats']['current']/st.session_state['stats']['rows'])
                    st.text(f"annotated ({st.session_state['stats']['viewed']:,.0f}/{st.session_state['stats']['rows']:,.0f})")
                    coverage_bar = st.progress(st.session_state['stats']['viewed']/st.session_state['stats']['rows'])
            with st.container(height=300):
                st.text('navigation')
                load_key_css()
                col1, col2 = st.columns([4, 1])
                with col1:
                    st.text('mark current locate good')
                    st.text('mark current locate bad')
                    st.text('go to next locate')
                    st.text('go to previous locate')
                    st.text('mark next 50 locates good')
                with col2: 
                    key("↑")
                    key("↓")
                    key("→")
                    key("←")
                    key("⇧+→")
                st.divider()
                col3, col4, col5, col6, col7 = st.columns(5)
                with col3:
                    streamlit_shortcuts.button(key("↑"), on_click=valid_callback, shortcut="ArrowUp")
                with col4:
                    streamlit_shortcuts.button(key("↓"), on_click=invalid_callback, shortcut="ArrowDown")   
                with col5:
                    streamlit_shortcuts.button(key("→"), on_click=next_callback, shortcut="ArrowRight")
                with col6:
                    streamlit_shortcuts.button(key("←"), on_click=previous_callback, shortcut="ArrowLeft")  
                with col7:
                    streamlit_shortcuts.button(key("⇧+→"), on_click=soar_callback, shortcut="Shift+ArrowUp")

def render_stats():
    data = st.session_state['current_row']
    col1, col2, col3, col4 = st.columns(4)  
    with col1:
        try:
            st.metric('km since last locate', f"{data['km_since_last_locate']:,.1f} km")
        except:
            pass
    with col2:
        try:
            st.metric('min since last locate', f"{data['min_since_last_locate']:,.0f} min") 
        except:
            pass
    with col3:
        try:
            st.metric('km / min', f"{data['km_since_last_locate']/max(data['min_since_last_locate'], 1):,.1f} km")          
        except:
            pass
    with col4:
        st.metric('supplier', f"{data['supply_id']}")          

    flags = {}
    if data['time_flag']:
        flags['time jump'] = '⏰'
    if data['distance_flag']:
        flags['distance jump'] = '👟'
    if data['speed_flag']:
        flags['speed jump'] = '✈️' 
    if data['supplier_flag']:
        flags['bad supplier'] = '⚠️'  

    if len(flags) > 0:
        cols = st.columns(len(flags)) 
        for i, k in enumerate(flags):
            with cols[i]:
                st.metric(f'{k}', f'{flags[k]}')


# Shortcut callback functions
def previous_callback():
    if 0 < st.session_state['stats']['current']:
        st.session_state['stats']['current'] -= 1
        st.session_state['rerun'] = True

def next_callback():
    if st.session_state['stats']['current'] + 1 < st.session_state['stats']['rows']:
        st.session_state['stats']['current'] += 1
        st.session_state['rerun'] = True

def fall_callback():
    if 0 < st.session_state['stats']['current'] - 50:
        st.session_state['stats']['current'] -= 50
        st.session_state['rerun'] = True

def jump_callback():
    if st.session_state['stats']['current'] + 50 < st.session_state['stats']['rows']:
        st.session_state['stats']['current'] += 50
        st.session_state['rerun'] = True

def dive_callback():
    if 0 < st.session_state['stats']['current'] - 50:
        outcome = annotate(
            st.session_state['device_id'],
            st.session_state['stats']['current'],
            st.session_state['stats']['current']-49,
            False
        )
        if outcome:
            st.session_state['stats']['current'] -= 50
            st.session_state['rerun'] = True

def soar_callback():
    if st.session_state['stats']['current'] + 50 < st.session_state['stats']['rows']:
        outcome = annotate(
            st.session_state['device_id'],
            st.session_state['stats']['current'],
            st.session_state['stats']['current']+49,
            False
        )
        if outcome:
            st.session_state['stats']['current'] += 50
            st.session_state['rerun'] = True

def valid_callback():
    annotate(
        st.session_state['device_id'],
        st.session_state['stats']['current'],
        st.session_state['stats']['current'],
        False
    )
    st.session_state['stats']['current'] = min(st.session_state['stats']['current'] + 1, st.session_state['stats']['rows'] - 1)
    st.session_state['rerun'] = True

def invalid_callback():
    annotate(
        st.session_state['device_id'],
        st.session_state['stats']['current'],
        st.session_state['stats']['current'],
        False
    )
    st.session_state['stats']['current'] = min(st.session_state['stats']['current'] + 1, st.session_state['stats']['rows'] - 1)
    st.session_state['rerun'] = True

def refresh_callback():
    st_rn = start(st.session_state['device_id'])
    st.session_state['stats']['current'] = st_rn
    st.session_state['rerun'] = True

# Streamlit app
def main():
    render_sidebar()
    with st.container(height=550):
        render_map()
    with st.container(height=200):
        render_stats()

    # Add keyboard shortcuts
    if st.session_state.get('rerun', False):
        st.session_state['rerun'] = False
        st.rerun()
    
if __name__ == "__main__":
    main()




