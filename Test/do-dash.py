import asyncio
from pathlib import Path

import polars as pl
from dash import Dash, dcc, html, Input, Output
import plotly.graph_objects as go

# First run sim-live-data.py to create the data_live.txt live file

data_file = "data_live.txt"
n_cols = 10
agg_period = 5

# ============================================================
# Shared queue
# ============================================================

# An asyncio queue will be used in the producer-consumer process
queue = asyncio.Queue()

# DataFrame where each row contains a 10-values vector with 5-seconds means
agg_df = pl.DataFrame(
    schema= {f"v{i}": pl.Float64 for i in range(n_cols)}
)

# Latest mean vector to show in Dash
vector_list = [0.0] * 10

# ============================================================
# asyncio producer
# ============================================================

# - Read new lines from a live data file
# - Every 5 lines (5 seconds) aggregate column-wise and adds the resulting vector
#   to the shared queue
async def tail_file_producer(path: str, data_queue: asyncio.Queue):

    file = Path(path)

    print("Waiting for:", data_file)

    while not file.exists():
        await asyncio.sleep(0.5)

    with open(path, "r") as f:

        print("Opened:", data_file)

        # Only process new lines
        f.seek(0, 2)

        # Initialize the buffer and iterate until is filled with 5 data-rows
        buffer = []

        while True:
            line = f.readline()

            if not line:
                await asyncio.sleep(0.2)
                continue

            # Extract values from the read line
            values = [float(x) for x in line.strip().split(",")]

            # Add values to the buffer
            buffer.append(values)

            print("Read values added to buffer:", values)

            if len(buffer) == agg_period:
                # Create a DataFrame with this 5-row buffer
                df = pl.DataFrame(buffer, schema=[f"v{i}" for i in range(n_cols)], orient="row")

                # Aggregates and calculates the mean of each column
                agg = df.select([pl.col(c).mean().alias(c) for c in df.columns])

                # Convert single-row DataFrame to list
                vector = agg.row(0)

                print("Aggregated values added to queue:", values)

                await data_queue.put(vector)
                buffer = []

# ============================================================
# asyncio consumer
# ============================================================

# Monitors the shared queue that contains the aggregated vectors
# and appends these vectors to a DataFrame.
async def consumer(data_queue: asyncio.Queue):

    global agg_df, vector_list

    while True:
        vector_tuple = await data_queue.get()

        vector_list = list(vector_tuple)
        print("Consumed:", vector_list)

        new_row = pl.DataFrame([vector_list], schema=agg_df.columns, orient="row")
        agg_df = pl.concat([agg_df, new_row], how="vertical")

        data_queue.task_done()

# ============================================================
# Dash app
# ============================================================

app = Dash(__name__)

app.layout = html.Div([
    html.H3("Mean values in a 5-second window"),
    dcc.Graph(id="live-graph"),
    dcc.Interval(id="interval", interval=5000, n_intervals=0),
])

@app.callback(
    Output("live-graph", "figure"),
    Input("interval", "n_intervals"),
)
def update_graph(n):
    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=list(range(10)),
            y=vector_list,
            mode="lines+markers",
        )
    )
    fig.update_layout(
        xaxis_title="Index",
        yaxis_title="Mean values",
        title="Latest 5-second window",
    )
    return fig

# ============================================================
# Main
# ============================================================

async def main():
    print("Main")

    asyncio.create_task(tail_file_producer(data_file, queue))
    asyncio.create_task(consumer(queue))

    # Run Dash in a worker thread so asyncio loop can continue running
    await asyncio.to_thread(
        app.run,
        host="127.0.0.1",
        port=8059,
        debug=False,
    )

# Note: if app.run yields an error try changing the server port

if __name__ == '__main__':
    asyncio.run(main())