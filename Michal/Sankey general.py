import pandas as pd
import plotly.graph_objects as go
from snowflake_access.SQLUtils import query_on_multiple_cids
import pandas as pd
import plotly.graph_objects as go
from dash import dcc, html, Dash
from dash.dependencies import Input, Output


def create_sankey(data):
    """
    Create an interactive Sankey diagram with filters for transition_per and device selection.

    Parameters:
        data (list of dict or pandas DataFrame): Input data with columns
            'from_page', 'to_page', 'transition_per', 'device', and 'session_type'.

    Returns:
        Dash app: A Dash app to render the Sankey diagram.
    """
    # Convert the input to a DataFrame
    try:
        data_table = pd.DataFrame(data)
    except Exception as e:
        raise ValueError(f"Failed to convert input to DataFrame: {e}")

    # Validate that necessary columns exist
    required_columns = {'from_page', 'to_page', 'transition_per', 'device', 'session_type'}
    if not required_columns.issubset(data_table.columns):
        raise ValueError(f"Input data must include columns: {required_columns}")

    # Create unique labels for pages
    unique_pages = pd.concat([data_table['from_page'], data_table['to_page']]).unique().tolist()
    label_dict = {page: i for i, page in enumerate(unique_pages)}

    # Map labels to indices
    data_table['source'] = data_table['from_page'].map(label_dict)
    data_table['target'] = data_table['to_page'].map(label_dict)

    # Define link colors based on session_type
    def get_link_color(row):
        return 'rgba(45,114,188,0.5)' if row[
                                          'session_type'] == 'no_uid' else 'rgba(45,114,188,0.5)'  # Red for "no_uid", light blue otherwise

    data_table['link_color'] = data_table.apply(get_link_color, axis=1)

    # Define node colors
    node_colors = ['rgba(45,114,188,0.8)' for _ in unique_pages]  # Light blue nodes

    # Initialize Dash app
    app = Dash(__name__)

    # Layout with a Sankey diagram, a range slider, and a device dropdown
    app.layout = html.Div([
        html.H1("Flow Diagram for Cedric including no_UID"),
        html.Div([
            dcc.Dropdown(
                id='device-dropdown',
                options=[
                    {'label': device, 'value': device} for device in data_table['device'].unique()
                ],
                value=data_table['device'].unique()[0],
                style={'width': '200px'}  # Shorter width for the dropdown
            )
        ], style={'margin-bottom': '20px'}),
        dcc.RangeSlider(
            id='range-slider',
            min=data_table['transition_per'].min(),
            max=data_table['transition_per'].max(),
            step=0.5,
            marks={int(i): str(int(i)) for i in data_table['transition_per'].unique()},
            value=[1.5, data_table['transition_per'].max()]
        ),
        html.Div(id='slider-output'),
        dcc.Graph(id='sankey-graph')
    ])

    @app.callback(
        Output('sankey-graph', 'figure'),
        [Input('device-dropdown', 'value'),
         Input('range-slider', 'value')]
    )
    def update_sankey(selected_device, range_values):
        # Filter data based on the selected device and range slider values
        filtered_table = data_table[
            (data_table['device'] == selected_device) &
            (data_table['transition_per'] >= range_values[0]) &
            (data_table['transition_per'] <= range_values[1])
            ]

        # Create Sankey diagram
        sankey_fig = go.Figure(go.Sankey(
            node=dict(
                pad=15,
                thickness=20,
                line=dict(color="black", width=0.5),
                label=unique_pages,
                color=node_colors
            ),
            link=dict(
                source=filtered_table['source'],  # Indices of source nodes
                target=filtered_table['target'],  # Indices of target nodes
                value=filtered_table['transition_per'],  # Flow values
                color=filtered_table['link_color']  # Link colors based on session_type
            )
        ))

        # Update layout
        # sankey_fig.update_layout(
        #     title_text="User Journey Flow with no_UID flows",
        #     font_size=12
        # )

        return sankey_fig

    return app


# Example usage
if __name__ == "__main__":
    # Example data
    query = """
   select * from(
    select from_page,to_page,count(distinct sid) as sid_count,count(*) as transition_count,session_type,sm_device_source as device,
    (COUNT(*) / SUM(COUNT(*)) OVER (partition by sm_device_source)) * 100 AS transition_per,
    (COUNT(distinct sid) / SUM(COUNT(distinct sid)) OVER (partition by session_type,sm_device_source)) * 100 AS SID_per
    from (
    with t1 as (SELECT
        to_date(starttime) as date,
        sid,
        application as from_page,
        case when LEAD(application) OVER (PARTITION BY sid ORDER BY CONTEXT_START_TIME) is null then application else LEAD(application) OVER (PARTITION BY sid ORDER BY CONTEXT_START_TIME)
   end AS to_page  from contexts
    where to_date(starttime) between '2024-08-01' and '2024-08-04' ),
    t2 as (select sid,sm_device_source,session_type from sessions where to_date(starttime) between '2024-08-01' and '2024-08-04' and sm_device_source in ('android','ios','js')

    )
    select t1.*,t2.session_type,t2.sm_device_source from t1 inner join t2 on t2.sid=t1.sid

    ) where 

    from_page is not null 
    and to_page is not null  group by all) where transition_per>0.1



    """

    # Execute the query on multiple CIDs
    data = query_on_multiple_cids(query, ['cyclops'])
    data.to_csv('/Users/michal.reuveni/Desktop/Pycharm results/cyclops2024.csv')

    # Create the Sankey diagram
    sankey_app = create_sankey(data)
    sankey_app.run_server(port=8080, debug=True)

    # Show the figure in a browser or PyCharm viewer
    sankey_figure.show()
