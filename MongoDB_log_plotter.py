# -*- coding: utf-8 -*-
"""
@author: zelmar@michelini.com.uy
"""


import json
import pandas as pd
import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import plotly.express as px
from plotly.subplots import make_subplots
from dash import dash_table
from tqdm import tqdm
from dateutil import parser
import sys

log_file_path = sys.argv[1]


def parse_log_line(line):
    try:
        log_data = json.loads(line)
        return log_data
    except json.JSONDecodeError:
        return None

def filter_slow_queries(log_data):
    return log_data.get('msg') == 'Slow query'

def filter_connections(log_data):
    return log_data.get('msg') == 'Connection accepted'

def extract_duration(log_data):
    return round(log_data.get('attr', {}).get('durationMillis', 0))

def extract_timestamp(log_data):
    return log_data.get('t', {}).get('$date')

def convert_timestamp(timestamp_str):
    try:
        # Try parsing with UTC offset
        timestamp = parser.isoparse(timestamp_str)
    except ValueError:
        try:
            # Try parsing without UTC offset
            timestamp = parser.isoparse(timestamp_str + "+00:00")
        except ValueError:
            # If both attempts fail, return None or handle the case as needed
            return None

    return timestamp

# Add a new function to filter error messages
def filter_information(log_data):
    return 'error' in json.dumps(log_data, default=str)

# Add a new function to extract information message
def extract_information_message(log_data):
    return log_data.get('msg', 'Unknown Information')



def process_log_lines(lines):
    mongodb_info = {
        'mongodb_version': "No data",
        'node_name': "No data",
        'replica_set_name': "No data",
        'os_version': "No data"
    }

    slow_queries_data = {
        'Timestamp': [],
        'Duration (ms)': [],
        'Namespace': [],
        'Command': []
    }

    connection_data = {
        'Timestamp': [],
        'ConnectionCount': []
    }
    
    information_data = {
        'Timestamp': [],
        'Information Message': [],
        'Command': []
    }
    
    for line in tqdm(lines, desc="Processing Log Data", unit=" lines"):
        log_data = parse_log_line(line)

        if not log_data or not isinstance(log_data, dict):
            continue

        if log_data.get('msg') == 'Build Info':
            mongodb_info['mongodb_version'] = log_data.get('attr', {}).get('buildInfo', {}).get('version')
        elif log_data.get('msg') == 'Process Details':
            mongodb_info['node_name'] = log_data.get('attr', {}).get('host')
        elif log_data.get('msg') == 'Node is a member of a replica set':
            mongodb_info['replica_set_name'] = log_data.get('attr', {}).get('config', {}).get('_id')
        elif log_data.get('msg') == 'Operating System':
            mongodb_info['os_version'] = log_data.get('attr', {}).get('os', {}).get('version')

        if log_data.get('msg') == 'Slow query':
            duration = extract_duration(log_data)
            timestamp_str = extract_timestamp(log_data)
            ns = log_data.get('attr', {}).get('ns', 'Unknown')
            command = log_data.get('attr', {}).get('command', 'No command available')

            slow_queries_data['Timestamp'].append(convert_timestamp(timestamp_str))
            slow_queries_data['Duration (ms)'].append(duration)
            slow_queries_data['Namespace'].append(ns)
            slow_queries_data['Command'].append(command)

        elif log_data.get('msg') == 'Connection accepted':
            timestamp_str = extract_timestamp(log_data)
            timestamp = convert_timestamp(timestamp_str)
            connection_count = log_data.get('attr', {}).get('connectionCount', 0)

            connection_data['Timestamp'].append(timestamp)
            connection_data['ConnectionCount'].append(connection_count)

        if filter_information(log_data):
            timestamp_str = extract_timestamp(log_data)
            info_msg = extract_information_message(log_data)
            command = log_data.get('attr', {}).get('command', 'No command available')
            
            information_data['Timestamp'].append(convert_timestamp(timestamp_str))
            information_data['Information Message'].append(info_msg)
            information_data['Command'].append(info_msg)

    return mongodb_info, slow_queries_data, connection_data, information_data


def read_lines_with_progress(log_file_path):
    lines = []
    
    # Get the total number of lines in the file
    total_lines = sum(1 for _ in open(log_file_path, 'r', encoding='utf-8', errors='replace'))

    # Use tqdm to display a progress bar
    with open(log_file_path, 'r', encoding='utf-8', errors='replace') as file:
        for line in tqdm(file, total=total_lines, desc="Reading Lines"):
            lines.append(line.strip())  # Adjust as needed based on your requirements

    return lines

lines = read_lines_with_progress(log_file_path)


mongodb_info, slow_queries_data, connection_data, information_data = process_log_lines(lines)

# Convert to DataFrames
df_slow_queries = pd.DataFrame(slow_queries_data)
df_connections = pd.DataFrame(connection_data)
df_information = pd.DataFrame(information_data)

# Create subplots with shared_xaxes=True 
fig = make_subplots(rows=3, cols=1, shared_xaxes=True, subplot_titles=['Slow Queries', 'Connections', 'Errors'], vertical_spacing = 0.05)



#use a sample of the dataset only to plot
if len(df_slow_queries.index) > 10000:
    df_slow_queries_sample = df_slow_queries.sample(10000)
else:
    df_slow_queries_sample = df_slow_queries

# Add scatter plot for Slow Queries
scatter_fig = px.scatter(
    df_slow_queries_sample,
    x='Timestamp',
    y='Duration (ms)',
    color='Namespace',
    labels={'Timestamp': 'Timestamp', 'Duration (ms)': 'Duration (ms)'},
    hover_data={'Timestamp': '|%Y-%m-%d %H:%M:%S.%f|', 'Namespace': True, 'Command': False},
    ).update_layout(
    yaxis=dict(tickformat="%0d"),
    xaxis=dict(rangeslider_visible=False),
    height=500  # Adjust the height as needed
    ).update_traces(marker=dict(size=8), selector=dict(mode='markers'))

# Extract scatter trace from Plotly Express figure
a = df_slow_queries_sample['Namespace'].nunique()
for i in range(a-1):
   # print(i)
    scatter_trace = scatter_fig['data'][i]
    fig.add_trace(scatter_trace, row=1, col=1)



# Add line plot for Connection Counts
line_trace = px.line(
    df_connections,
    x='Timestamp',
    y='ConnectionCount',
    labels={'Timestamp': 'Timestamp', 'ConnectionCount': 'Connection Count'},
    hover_data={'Timestamp': '|%Y-%m-%d %H:%M:%S.%f|', 'ConnectionCount': True},
    ).update_layout(
    yaxis=dict(tickformat="%0d"),
    xaxis=dict(rangeslider_visible=False),
    height=600  # Adjust the height as needed
    )['data'][0]

        
fig.add_trace(line_trace, row=2, col=1)  # Add line plot to subplot


information_fig = px.scatter(
    df_information,
    x='Timestamp',
    y='Information Message',
    color='Information Message',
    labels={'Timestamp': 'Timestamp', 'Information Message': 'Information Message'},
    hover_data={'Timestamp': '|%Y-%m-%d %H:%M:%S.%f|', 'Information Message': True},
    ).update_layout(
    yaxis=dict(tickformat="%0d"),
    xaxis=dict(rangeslider_visible=False),
    height=500  # Adjust the height as needed
    ).update_traces(marker=dict(size=8), selector=dict(mode='markers'))

# fig.update_layout(
#     showlegend=True,
#     yaxis=dict(tickformat="%0d"),
#     xaxis=dict(rangeslider_visible=False),
#     height=600,  # Adjust the height as needed
# )
        
        
information_fig.update_traces(marker=dict(size=8, symbol="diamond"))
        

# Add scatter plot for Information
a = df_information['Information Message'].nunique()
for i in range(a-1):
   # print(i)
    information_trace = information_fig['data'][i]
    fig.add_trace(information_trace, row=3, col=1)

# fig.update_layout(
#     showlegend=True,
#     height=800,  # Adjust the height as needed
# )

fig.update_layout(
    showlegend=True,
    yaxis=dict(tickformat="%0d"),
    xaxis=dict(rangeslider_visible=False),
    height=800,  # Adjust the height as needed
)

# Define Dash app layout
app = dash.Dash(__name__)

# Define the app layout
app.layout = html.Div([
    # Scatter plot
    dcc.Graph(id='slow-queries-scatter', figure=fig),

    # Pre tag for displaying queries
    html.Pre(id='query-display', style={'textAlign': 'left', 'fontSize': 13}),
    
    # Section for slow queries
    html.Div([
        

        # Two-column layout
        html.Div([
            # First column for the DataTable
            
            html.Div([
                html.Hr(),
                html.H3("Slow queries", style={'margin-top': '20px'}),
                dash_table.DataTable(
                    id='aggregated-table',
                    columns=[
                        {'name': col, 'id': col} for col in df_slow_queries.groupby('Namespace')
                                                        .agg({'Command': 'size', 'Duration (ms)': 'mean'})
                                                        .round(0)
                                                        .rename(columns={'Command': 'count', 'Duration (ms)': 'mean duration (ms)'})
                                                        .sort_values(['count'], ascending=False)
                                                        .reset_index()
                                                        .columns
                    ],
                    data=df_slow_queries.groupby('Namespace')
                                        .agg({'Command': 'size', 'Duration (ms)': 'mean'})
                                        .round(0)
                                        .rename(columns={'Command': 'count', 'Duration (ms)': 'mean duration (ms)'})
                                        .sort_values(['count'], ascending=False)
                                        .reset_index()
                                        .to_dict('records'),
                    style_table={'width': '100%'},
                    style_cell={'maxWidth': 0},
                ),
            ], style={'width': '50%', 'display': 'inline-block', 'vertical-align': 'top'}),

            # Second column for additional information
            html.Div([
                html.Hr(),
                html.H3("Additional Information", style={'margin-top': '20px'}),
                html.Table([
                    html.Tr([html.Td("MongoDB Version:"), html.Td(mongodb_info['mongodb_version'])]),
                    html.Tr([html.Td("Node Name:"), html.Td(mongodb_info['node_name'])]),
                    html.Tr([html.Td("Replica Set Name:"), html.Td(mongodb_info['replica_set_name'])]),
                    html.Tr([html.Td("OS Version:"), html.Td(mongodb_info['os_version'])]),
                    html.Tr([html.Td("Slow queries:"), html.Td(len(df_slow_queries.index))]),
                    html.Tr([html.Td("Slow queries plotted:"), html.Td(round(len(df_slow_queries_sample.index)))]),
                ]),
            ], style={'width': '40%', 'display': 'inline-block', 'vertical-align': 'top', 'marginLeft': '20px'}),
            
        ]),
    ]),
])


@app.callback(
    Output('query-display', 'children'),
    Input('slow-queries-scatter', 'clickData'),
    Input('information-scatter', 'clickData')  # Adjust the ID here
)
def display_query_slow_queries(clickData_slow, clickData_info):
    if clickData_slow is not None and clickData_slow['points']:
        point_data = clickData_slow['points'][0]
        timestamp = point_data['x']
        duration = point_data['y']
        namespace = point_data['customdata'][0] if 'customdata' in point_data else 'No namespace available'
        command = point_data['customdata'][1] if len(point_data['customdata']) > 1 else 'No command available'
        return f"Timestamp: {timestamp}\nDuration: {duration} ms\nNamespace: {namespace}\nCommand: {command}"
    
    elif clickData_info is not None and clickData_info['points']:
        point_data_info = clickData_info['points'][0]
        timestamp_info = point_data_info['x']
        info_msg = point_data_info['y']
        command_info = point_data_info['customdata'][0] if 'customdata' in point_data_info else 'No command available'
        return f"Timestamp: {timestamp_info}\nInformation Message: {info_msg}\nCommand: {command_info}"
    
app.run_server(debug=False)