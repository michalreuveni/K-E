import pandas as pd
import plotly.graph_objects as go
from dash import dcc, html, Dash
from dash.dependencies import Input, Output
from snowflake_access.SQLUtils import query_on_multiple_cids
from dash.dependencies import Input, Output
from dash import callback_context  # Import for handling callbacks


def create_sankey(data):
    """
    Create an interactive Sankey diagram with 'from_page' and 'to_page' as nodes and 'wup_rate' as the flow width.
    """
    try:
        data_table = pd.DataFrame(data)
    except Exception as e:
        raise ValueError(f"Failed to convert input to DataFrame: {e}")

    required_columns = {'from_page', 'to_page', 'wups_rate_gt40'}
    if not required_columns.issubset(data_table.columns):
        raise ValueError(f"Input data must include columns: {required_columns}")

    # Create unique node labels
    unique_pages = pd.concat([data['from_page'], data['to_page']]).unique().tolist()
    label_dict = {page: i for i, page in enumerate(unique_pages)}

    # Map labels to indices
    data['source'] = data['from_page'].map(label_dict)
    data['target'] = data['to_page'].map(label_dict)

    # Initialize Dash app
    app = Dash(__name__)

    app.layout = html.Div([
        html.H1("Sankey Diagram of User Flow"),
        dcc.Graph(
            id='sankey-graph',
            figure=go.Figure(go.Sankey(
                node=dict(
                    pad=15,
                    thickness=20,
                    line=dict(color="black", width=0.5),
                    label=unique_pages,
                    color=['rgba(45,114,188,0.8)'] * len(unique_pages)
                ),
                link=dict(
                    source=data['source'],
                    target=data['target'],
                    value=data['wups_rate_gt40'],
                    hovertemplate='From: %{source.label} <br>To: %{target.label} <br>wups_rate_gt40: %{value}<extra></extra>'
                )
            ))
        )
    ])

    return app  # Ensure the function returns the Dash app


if __name__ == "__main__":
    query = """
   SELECT
  *
FROM
  (
    SELECT
      from_page,
      to_page,
      COUNT(DISTINCT sid) AS sid_count,
      
      avg(wup_rate) as avg_wup_rate,
      100.0 * COUNT_IF (
   wup_rate > 40
  ) / COUNT(*) AS wups_rate_gt40,
      sm_device_source AS device
     
    FROM
      (
        WITH t1 AS (
          SELECT
            TO_DATE (starttime) AS date,
            sid,
            application AS from_page,
            CASE
              WHEN LEAD (application) OVER (
                PARTITION BY sid
                ORDER BY
                  CONTEXT_START_TIME
              ) IS NULL THEN application
              ELSE LEAD (application) OVER (
                PARTITION BY sid
                ORDER BY
                  CONTEXT_START_TIME
              )
            END AS to_page
          FROM
            contexts
          WHERE
            TO_DATE (starttime) >= CURRENT_DATE - 3
        ),
        t2 AS (
          SELECT
            sid,
            sm_device_source,
            CASE
              WHEN (LAST_WUP_TIMESTAMP - start_time) = 0 THEN 0
              ELSE TOTAL_NUM_OF_WUPS / NULLIF((LAST_WUP_TIMESTAMP - start_time), 0)
            END AS wup_rate,
            CASE when TOTAL_NUM_OF_WUPS / (LAST_WUP_TIMESTAMP - start_time)>40 then 1 else 0
            END AS wup_rate_40,
          FROM
            sessions
          WHERE
            TO_DATE (starttime) >= CURRENT_DATE - 3
            AND product = 'ATO' and sm_device_source='js'
        )
        SELECT
          t1.*,
          t2.wup_rate,
          t2.sm_device_source
        FROM
          t1
          INNER JOIN t2 ON t2.sid = t1.sid
      )
    WHERE
      NOT from_page IS NULL
      AND NOT to_page IS NULL
    GROUP BY
      ALL
  )  where avg_wup_rate>0 having sid_count>1000 order by sid_count desc limit 30;

    """
    data = query_on_multiple_cids(query, ['alice'])
    print(data)
    sankey_app = create_sankey(data)
    sankey_app.run_server(port=8051, debug=True)
