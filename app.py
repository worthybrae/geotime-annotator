import streamlit as st
import pandas as pd
import uuid
import time
import s3fs
from sharehousepy import SnowflakeConnection, Query
import snowflake.connector
import pydeck as pdk
import streamlit_shortcuts
from streamlit_extras.keyboard_text import key, load_key_css
import plotly.graph_objects as go
from helpers.misc import write_df_async, format_minutes, calculate_zoom_level, calculate_radius, format_speed, add_meta, human_format, calculate_distance, alt_format_minutes


# API Functions
def start() -> bool:
    try:
        s3 = s3fs.S3FileSystem(
            key=st.secrets["aws"]["PUBLIC_KEY"],
            secret=st.secrets["aws"]["PRIVATE_KEY"]
        )

        # Specify the S3 path to your CSV file
        s3_path = f"s3://a6dev-mltraining/raw_input/{st.session_state['truncation']}/{st.session_state['minutes']}/{st.session_state['km_threshold']}/{st.session_state['device_id'].lower()}.csv"

        # Read the CSV file directly from S3 into a pandas DataFrame
        with s3.open(s3_path, 'rb') as f:
            df = pd.read_csv(f)
  
            max_segment = df['segment'].max()

            grouped_df = df.groupby(['segment']).agg({
                'id': 'count',  # Count total rows
                'timestamp': 'min',
                'start_lat': 'max',
                'start_lon': 'max',
                'start_time': 'max',
                'end_lat': 'max',
                'end_lon': 'max',
                'end_time': 'max',
                'locates': 'max',
                'min_seen': 'max',
                'coverage_percent': 'max',
                'km_travelled': 'max',
                'fraud': 'max',
                'supply_id': pd.Series.nunique,
                'has_742': 'max'
            }).reset_index()


            grouped_df = grouped_df.sort_values(by=['segment']).reset_index(drop=True)

            null_fraud_row = grouped_df[grouped_df['fraud'].isnull()].head(1)

            starting_segment = int(null_fraud_row['segment'].iloc[0])

            annotated = max(0, max_segment - int(grouped_df['fraud'].isna().sum()))

            total_dupes_sum = grouped_df['locates'].sum() - len(grouped_df)
            
            stats = {
                'max_segment': max_segment,
                'current_segment': starting_segment,
                'annotated': annotated,
                'duplicates': total_dupes_sum - len(df),
                'locates': total_dupes_sum
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
    
    sf_con = SnowflakeConnection(
        username=st.secrets["database"]["SF_USERNAME"], 
        password=st.secrets["database"]["SF_PASSWORD"],
        account=st.secrets["database"]["SF_ACCOUNT"],
        database=st.secrets["database"]["SF_DATABASE"],
        schema=st.secrets["database"]["SF_SCHEMA"],
        warehouse_sm=st.secrets["database"]["SF_WAREHOUSE_LG"],
        warehouse_lg=st.secrets["database"]["SF_WAREHOUSE_LG"],
        geo_table=st.secrets["database"]["SF_GEO_TABLE"],
        id_table=st.secrets["database"]["SF_ID_TABLE"],
    )
    q = Query(
        query=f"""
        with all_data as (
            select 
                lower(idfa) as id, 
                timestamp,
                latitude,
                longitude,
                supply_id,
                md5_hex(concat(time_slice(timestamp, {minutes}, 'minute'), trunc(latitude, {truncation}), trunc(longitude, {truncation}))) as rh,
                count(1) over (partition by rh) as dupes
            from
                singularity.public.h4_maid_clustered
            where
                (idfa like '{device_id[:3].lower()}%' or idfa like '{device_id[:3].upper()}%') and
                lower(idfa) = '{device_id.lower()}'
            union all
            select 
                lower(idfa) as id, 
                timestamp,
                latitude,
                longitude,
                supply_id,
                md5_hex(concat(time_slice(timestamp, {minutes}, 'minute'), trunc(latitude, {truncation}), trunc(longitude, {truncation}))) as rh,
                count(1) over (partition by rh) as dupes
            from
                singularity.public.locations_purge
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
                rh,
                dupes - 1 as duplicates
            from
                all_data
        ), enriched_data as (
            select
                id,
                timestamp,
                latitude,
                longitude,
                supply_id,
                rh,
                duplicates,
                datediff('minutes', lag(timestamp) over (order by timestamp, latitude, longitude), timestamp) as min_since_last_locate,
                haversine(latitude, longitude, lag(latitude) over (order by timestamp, latitude, longitude), lag(longitude) over (order by timestamp, latitude, longitude)) as km_since_last_locate
            from
                filtered_data
        ), grouped_data as (
            select
                *,
                sum(case when km_since_last_locate > {km_threshold} then 1 else 0 end) over (order by timestamp, latitude, longitude) as segment
            from
                enriched_data
        ), twice_enriched as (
            select
                id,
                timestamp,
                latitude,
                longitude,
                supply_id,
                segment,
                first_value(latitude) over (partition by segment order by timestamp, latitude, longitude) as start_lat,
                last_value(latitude) over (partition by segment order by timestamp, latitude, longitude) as end_lat,
                first_value(longitude) over (partition by segment order by timestamp, latitude, longitude) as start_lon,
                last_value(longitude) over (partition by segment order by timestamp, latitude, longitude) as end_lon,
                first_value(timestamp) over (partition by segment order by timestamp, latitude, longitude) as start_time,
                last_value(timestamp) over (partition by segment order by timestamp, latitude, longitude) as end_time,
                sum(min_since_last_locate) over (partition by segment) as total_mins,
                sum(km_since_last_locate) over (partition by segment) as total_kms,
                first_value(km_since_last_locate) over (partition by segment order by timestamp, latitude, longitude) as km_since_last_segment,
                first_value(min_since_last_locate) over (partition by segment order by timestamp, latitude, longitude) as min_since_last_segment,
                sum(duplicates) over (partition by segment) as segment_duplicates,
                count(distinct date_trunc('mins', timestamp)) over (partition by segment) as covered_segment_mins
            from
                grouped_data
        )
        select
            id,
            timestamp,
            latitude,
            longitude,
            supply_id,
            segment,
            segment_duplicates as locates,
            total_mins - min_since_last_segment as min_seen,
            (covered_segment_mins / (min_seen + 1)) * 100 as coverage_percent,
            total_kms - km_since_last_segment as km_travelled,
            start_lat,
            start_lon,
            start_time,
            end_lat,
            end_lon,
            end_time
        from
            twice_enriched
        """
    )
    
    sf_con.run(q)
    
    # Define column names
    columns = [
        'id',
        'timestamp',
        'latitude',
        'longitude',
        'supply_id',
        'segment',
        'locates',
        'min_seen',
        'coverage_percent',
        'km_travelled',
        'start_lat',
        'start_lon',
        'start_time',
        'end_lat',
        'end_lon',
        'end_time'
    ]

    # Convert results to DataFrame
    df = pd.DataFrame(q.results, columns=columns)

    # Convert the time column to datetime
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    
    # Create 'fraud' column
    df['fraud'] = None

    # Use 742 as source of truth and automark
    df['has_742'] = 0
    df.loc[df['supply_id'] == '742', 'has_742'] = 1  
    df.loc[df['has_742'] == 1, 'fraud'] = True  

    # Find max segment    
    max_segment = df['segment'].max()
    
    # Generate grouped dataframe
    grouped_df = df.groupby(['segment']).agg({
        'id': 'count',  # Count total rows
        'timestamp': 'min',
        'start_lat': 'max',
        'start_lon': 'max',
        'start_time': 'max',
        'end_lat': 'max',
        'end_lon': 'max',
        'end_time': 'max',
        'locates': 'max',
        'min_seen': 'max',
        'coverage_percent': 'max',
        'km_travelled': 'max',
        'fraud': 'max',
        'supply_id': pd.Series.nunique,
        'has_742': 'max'
    }).reset_index()

    # Count total duplicate locates
    total_dupes_sum = grouped_df['locates'].sum() - len(grouped_df)

    stats = {
        'max_segment': max_segment,
        'current_segment': 0,
        'annotated': 0,
        'duplicates': total_dupes_sum,
        'locates': total_dupes_sum + len(df)
    }

    st.session_state['stats'] = stats
    st.session_state['df'] = df
    st.session_state['segment_df'] = grouped_df
    st.session_state['annotations'] = {}
    st.session_state['start'] = time.time()
    
    # Create an S3 filesystem object with your credentials
    outcome = write_df_async()
    print(f'csv saved? {outcome}')
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

def update_annotation(is_valid):
    current_segment = st.session_state['stats']['current_segment']
    
    # Create masks for the current segment
    df_mask = st.session_state['df']['segment'] == current_segment
    segment_df_mask = st.session_state['segment_df']['segment'] == current_segment
    
    # Update annotations and dataframes using masks
    st.session_state['annotations'][current_segment] = is_valid
    st.session_state['df'].loc[df_mask, 'fraud'] = is_valid
    st.session_state['segment_df'].loc[segment_df_mask, 'fraud'] = is_valid

    # Update stats
    st.session_state['stats']['annotated'] = st.session_state['stats']['max_segment'] - st.session_state['segment_df']['fraud'].isna().sum()

    # Check if all segments are annotated
    if st.session_state['stats']['annotated'] == st.session_state['stats']['max_segment']:
        st.balloons()
        st.session_state['rerun'] = True
        add_meta()
        

    # Write to dataframe if conditions are met
    elif len(st.session_state['annotations']) > 10 or int(st.session_state['stats']['annotated']) == int(st.session_state['stats']['max_segment']):
        if write_df_async():
            st.session_state['annotations'] = {}

    # Move to next segment
    st.session_state['stats']['current_segment'] = min(current_segment + 1, st.session_state['stats']['max_segment'])
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
        s3_path = f"s3://a6dev-mltraining/annotations.csv"

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
        filtered_df = grouped_df[
            (grouped_df['segment'].isin([x for x in range(max(0, st.session_state['stats']['current_segment'] - 51), min(st.session_state['stats']['current_segment'] + 52, st.session_state['stats']['max_segment']+1))])) &
            ((grouped_df['segment'] >= st.session_state['stats']['current_segment']) | (grouped_df['fraud'] != False))
        ].copy()
        filtered_df = filtered_df.sort_values(by=['segment']).reset_index(drop=True)
        current_segment = filtered_df.loc[filtered_df['segment'] == st.session_state['stats']['current_segment']]

        zoom_level = calculate_zoom_level(100)
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


# Function to create sidebar
def render_sidebar():
    # Sidebar for user inputs
    with st.sidebar:   
        if 'df' not in st.session_state:
            user_id = st.text_input('enter username:', 'w71')
            device_id = st.text_input("enter device id:", '00000000-0000-0000-0000-000000000000')
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
                    streamlit_shortcuts.button('↑', on_click=lambda: update_annotation(True), shortcut="ArrowUp")
                with col4:
                    streamlit_shortcuts.button('↓', on_click=lambda: update_annotation(False), shortcut="ArrowDown")   
                with col5:
                    streamlit_shortcuts.button("→", on_click=next_callback, shortcut="ArrowRight")
                with col6:
                    streamlit_shortcuts.button("←", on_click=previous_callback, shortcut="ArrowLeft")  

def render_stats():
    grouped_df = st.session_state['segment_df']
    col1, col2, col3 = st.columns(3)  
    with col1:
        with st.container(height=325):
            prev_filtered_df = grouped_df[(grouped_df['fraud'] != False) & (grouped_df['segment'] < st.session_state['stats']['current_segment'])]
            if prev_filtered_df.empty:
                pass 
            else:
                closest_segment = int(prev_filtered_df['segment'].max())
                target_segment = prev_filtered_df.loc[prev_filtered_df['segment'] == closest_segment]
                fraud_val = target_segment['fraud'].values[0]

                two_lag_df = prev_filtered_df[prev_filtered_df['segment'] < closest_segment]
                if two_lag_df.empty:
                    tl_segment = pd.DataFrame({})
                else:
                    tln_segment = int(two_lag_df['segment'].max())
                    tl_segment = two_lag_df.loc[two_lag_df['segment'] == tln_segment]

                if fraud_val == True:
                    st.subheader('previous ✅')
                elif fraud_val == False:
                    st.subheader('previous ❌')
                else:
                    st.subheader('previous ❔')
                scol1, scol2, scol3 = st.columns(3)
                with scol1:
                    st.metric('locates', f"{human_format(target_segment['id'].values[0])}")
                    st.metric('time seen', f"{format_minutes(target_segment['min_seen'].values[0])}")
                    if not tl_segment.empty:
                        distance = calculate_distance(
                            tl_segment['end_lat'].values[0],
                            tl_segment['end_lon'].values[0],
                            target_segment['start_lat'].values[0],
                            target_segment['start_lon'].values[0]
                        )
                        st.metric('km sll', f"{human_format(distance)}")
                with scol2:
                    st.metric('duplicates', f"{human_format(target_segment['locates'].values[0])}")
                    st.metric('coverage %', f"{target_segment['coverage_percent'].values[0]:,.0f}%")
                    if not tl_segment.empty:
                        time_diff = (target_segment['start_time'].values[0] - tl_segment['end_time'].values[0])
                        time_diff_minutes = time_diff.astype('timedelta64[m]').astype(int)
                        time_fmt, time_unit = alt_format_minutes(time_diff_minutes)
                    
                        st.metric(f'{time_unit} sll', f"{time_fmt}")
                with scol3:
                    st.metric('apps', f"{target_segment['supply_id'].values[0]}")
                    st.metric('km travelled', f"{human_format(target_segment['km_travelled'].values[0])}")
                    if not tl_segment.empty:
                        mph = (distance / time_diff_minutes) * 60 * 0.621371
                        st.metric('mph sll', f"{mph:,.0f}")
                                                    
    with col2:
        with st.container(height=325):
            target_segment = grouped_df.loc[grouped_df['segment'] == st.session_state['stats']['current_segment']]
            fraud_val = target_segment['fraud'].values[0]

            two_lag_df = prev_filtered_df[prev_filtered_df['segment'] < st.session_state['stats']['current_segment']]
            if two_lag_df.empty:
                tl_segment = pd.DataFrame({})
                    
            else:
                tln_segment = int(two_lag_df['segment'].max())
                tl_segment = two_lag_df.loc[two_lag_df['segment'] == tln_segment]

            if fraud_val == True:
                st.subheader('current ✅')
            elif fraud_val == False:
                st.subheader('current ❌')
            else:
                st.subheader('current ❔')
            scol1, scol2, scol3 = st.columns(3)
            with scol1:
                st.metric('locates', f"{human_format(target_segment['id'].values[0])}")
                st.metric('time seen', f"{format_minutes(target_segment['min_seen'].values[0])}")
                if not tl_segment.empty:
                    distance = calculate_distance(
                        tl_segment['end_lat'].values[0],
                        tl_segment['end_lon'].values[0],
                        target_segment['start_lat'].values[0],
                        target_segment['start_lon'].values[0]
                    )
                    st.metric('km sll', f"{human_format(distance)}")
            with scol2:
                st.metric('duplicates', f"{human_format(target_segment['locates'].values[0])}")
                st.metric('coverage %', f"{target_segment['coverage_percent'].values[0]:,.0f}%")
                if not tl_segment.empty:
                    time_diff = (target_segment['start_time'].values[0] - tl_segment['end_time'].values[0])
                    time_diff_minutes = time_diff.astype('timedelta64[m]').astype(int)
                    time_fmt, time_unit = alt_format_minutes(time_diff_minutes)
                    
                    st.metric(f'{time_unit} sll', f"{time_fmt}")
            with scol3:
                st.metric('apps', f"{target_segment['supply_id'].values[0]}")
                st.metric('km travelled', f"{human_format(target_segment['km_travelled'].values[0])}")
                if not tl_segment.empty:
                    if time_diff_minutes > 0:
                        mph = (distance / time_diff_minutes) * 60 * 0.621371
                    else:
                        mph = distance * 60 * 0.621371
                    st.metric('mph sll', f"{mph:,.0f}")
    with col3:
        with st.container(height=325):
            if st.session_state['stats']['current_segment'] + 1 <= st.session_state['stats']['max_segment']:
                target_segment = grouped_df.loc[grouped_df['segment'] == st.session_state['stats']['current_segment'] + 1]
                fraud_val = target_segment['fraud'].values[0]
                
                two_lag_df = prev_filtered_df[prev_filtered_df['segment'] < st.session_state['stats']['current_segment'] + 1]
                if two_lag_df.empty:
                    tl_segment = pd.DataFrame({})
                else:
                    tln_segment = int(two_lag_df['segment'].max())
                    tl_segment = two_lag_df.loc[two_lag_df['segment'] == tln_segment]
                
                if fraud_val == True:
                    st.subheader('next ✅')
                elif fraud_val == False:
                    st.subheader('next ❌')
                else:
                    st.subheader('next ❔')
                scol1, scol2, scol3 = st.columns(3)
                with scol1:
                    st.metric('locates', f"{human_format(target_segment['id'].values[0])}")
                    st.metric('time seen', f"{format_minutes(target_segment['min_seen'].values[0])}")
                    if not tl_segment.empty:
                        distance = calculate_distance(
                            tl_segment['end_lat'].values[0],
                            tl_segment['end_lon'].values[0],
                            target_segment['start_lat'].values[0],
                            target_segment['start_lon'].values[0]
                        )
                        st.metric('km sls', f"{human_format(distance)}")
                with scol2:
                    st.metric('duplicates', f"{human_format(target_segment['locates'].values[0])}")
                    st.metric('time coverage', f"{target_segment['coverage_percent'].values[0]:,.0f}%")
                    if not tl_segment.empty:
                        time_diff = (target_segment['start_time'].values[0] - tl_segment['end_time'].values[0])
                        time_diff_minutes = time_diff.astype('timedelta64[m]').astype(int)
                        time_fmt, time_unit = alt_format_minutes(time_diff_minutes)
                    
                        st.metric(f'{time_unit} sll', f"{time_fmt}")
                with scol3:
                    st.metric('apps', f"{target_segment['supply_id'].values[0]}")
                    st.metric('km travelled', f"{human_format(target_segment['km_travelled'].values[0])}")
                    if not tl_segment.empty:
                        mph = (distance / time_diff_minutes) * 60 * 0.621371
                        st.metric('mph sll', f"{mph:,.0f}")

# Streamlit app
def main():
    render_sidebar()

    
    if 'stats' in st.session_state:
        with st.expander('map', expanded=True):
            render_map()
            render_stats()
    
    with st.expander('Results', expanded=False):
        if 'df' in st.session_state:
            st.write("Download the annotated dataframe:")
            
            # Convert dataframe to CSV
            csv = st.session_state['df'].to_csv(index=False)
            
            # Create a download button
            st.download_button(
                label="Download CSV",
                data=csv,
                file_name="annotated_dataframe.csv",
                mime="text/csv",
            )
        else:
            st.write("No data available for download.")

    # Add keyboard shortcuts
    if st.session_state.get('rerun', False):
        st.session_state['rerun'] = False
        st.rerun()
    
if __name__ == "__main__":
    st.set_page_config(layout="wide")
    main()






