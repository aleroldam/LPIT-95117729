import asyncio
from pathlib import Path
import re
from datetime import datetime
import time

import polars as pl
import pandas as pd
from dash import Dash, dcc, html, Input, Output
import plotly.express as px

# ==============================================================================
# Configuración
# ==============================================================================
LOG_FILE = "cu-lan-ho-live.log"
PARQUET_FILE = "volumenes_agregados.parquet"

# PATRONES REGEX ACTUALIZADOS
# 1. Patrón SDAP (Volumen)
PATTERN_SDAP = re.compile(
    r'^(?P<timestamp>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}).*\[SDAP\s*\].*ue=(?P<ue_id>\d+).*DL:.*pdu_len=(?P<dl_vol>\d+)'
)

# 2. Patrón de Contexto (PCI, PLMN, RNTI)
# Extrae ue_id, plmn, pci y rnti de la misma línea de creación [CU-UEMNG]
PATTERN_CONTEXT = re.compile(
    r'ue=(?P<ue_id>\d+).*?plmn=(?P<plmn>\d+).*?pci=(?P<pci>\d+).*?rnti=(?P<rnti>0x[0-9a-fA-F]+)'
)

# Colas y Variables Globales
data_queue = asyncio.Queue()
raw_buffer = [] 

# Diccionario para mantener el estado de cada UE (PLMN, PCI, RNTI)
ue_metadata = {} 

# Esquema actualizado para incluir metadatos
agg_df = pl.DataFrame(schema={
    "time_window": pl.Datetime, 
    "ue_id": pl.Utf8, 
    "dl_vol_sum": pl.Float64,
    "pci": pl.Utf8,
    "plmn": pl.Utf8,
    "rnti": pl.Utf8
})

# ==============================================================================
# OBJETIVO 1 & 2: Productor - Leer, Identificar y Asociar Metadatos
# ==============================================================================
async def log_reader_producer(path: str, queue: asyncio.Queue):
    file = Path(path)
    print(f"Esperando log: {path}...")

    while not file.exists():
        await asyncio.sleep(0.5)

    with open(path, "r", encoding="utf-8") as f:
        f.seek(0, 2)
        while True:
            line = f.readline()
            if not line:
                await asyncio.sleep(0.1)
                continue

            # A. Buscar información de contexto (PCI, PLMN, RNTI)
            match_ctx = PATTERN_CONTEXT.search(line)
            if match_ctx:
                d = match_ctx.groupdict()
                uid = d['ue_id']
                if uid not in ue_metadata:
                    ue_metadata[uid] = {"pci": "N/A", "plmn": "N/A", "rnti": "N/A"}
                
                # Actualizar solo el campo que ha venido en la línea
                if d.get('pci'): ue_metadata[uid]['pci'] = d['pci']
                if d.get('plmn'): ue_metadata[uid]['plmn'] = d['plmn']
                if d.get('rnti'): ue_metadata[uid]['rnti'] = d['rnti']

            # B. Buscar volumen SDAP
            match_sdap = PATTERN_SDAP.search(line)
            if match_sdap:
                data = match_sdap.groupdict()
                uid = data["ue_id"]
                
                # Recuperar metadatos guardados o usar N/A
                meta = ue_metadata.get(uid, {"pci": "N/A", "plmn": "N/A", "rnti": "N/A"})
                
                record = {
                    "timestamp": datetime.fromisoformat(data["timestamp"]),
                    "ue_id": f"UE_{uid}",
                    "dl_vol": float(data["dl_vol"]),
                    "pci": meta['pci'],
                    "plmn": meta['plmn'],
                    "rnti": meta['rnti']
                }
                await queue.put(record)

# ==============================================================================
# OBJETIVO 3 & 4: Agregación (Incluyendo metadatos)
# ==============================================================================
async def data_aggregator_consumer(queue: asyncio.Queue):
    global raw_buffer, agg_df
    last_agg_time = time.time()

    while True:
        try:
            record = await asyncio.wait_for(queue.get(), timeout=0.5)
            raw_buffer.append(record)
            queue.task_done()
        except asyncio.TimeoutError:
            pass

        if time.time() - last_agg_time >= 1.0:
            if raw_buffer:
                df_raw = pl.DataFrame(raw_buffer)
                df_raw = df_raw.with_columns(pl.col("timestamp").dt.truncate("1s").alias("time_window"))
                
                # Agregamos sumando volumen pero manteniendo PCI, PLMN y RNTI (usamos el último conocido)
                df_grouped = (
                    df_raw.group_by(["time_window", "ue_id"])
                    .agg([
                        pl.col("dl_vol").sum().alias("dl_vol_sum"),
                        pl.col("pci").last(),
                        pl.col("plmn").last(),
                        pl.col("rnti").last()
                    ])
                )
                agg_df = pl.concat([agg_df, df_grouped], how="vertical")
                raw_buffer = []
            last_agg_time = time.time()

# ==============================================================================
# OBJETIVO 5: Representar en Dash con info de PLMN, PCI, RNTI
# ==============================================================================
app = Dash(__name__)
app.layout = html.Div([
    html.H3("Monitorización SDAP DL - Info de Red por UE"),
    dcc.Graph(id="live-graph"),
    dcc.Interval(id="interval-component", interval=1000, n_intervals=0)
])

@app.callback(
    Output("live-graph", "figure"),
    Input("interval-component", "n_intervals")
)
def update_graph(n):
    global agg_df
    if agg_df.is_empty():
        return px.line(title="Esperando datos...")

    # Consolidar y ordenar
    consolidated_df = (
        agg_df.group_by(["time_window", "ue_id"])
        .agg([
            pl.col("dl_vol_sum").sum(),
            pl.col("pci").last(),
            pl.col("plmn").last(),
            pl.col("rnti").last()
        ]).sort("time_window")
    )

    df_pd = consolidated_df.to_pandas()
    
    # Crear una columna combinada para la leyenda
    df_pd["UE_Info"] = (
        df_pd["ue_id"] + 
        " (RNTI: " + df_pd["rnti"] + 
        ", PCI: " + df_pd["pci"] + 
        ", PLMN: " + df_pd["plmn"] + ")"
    )

    # Gráfico con la nueva etiqueta informativa
    fig = px.line(
        df_pd, 
        x="time_window", 
        y="dl_vol_sum", 
        color="UE_Info", # <--- Aquí mostramos toda la info
        markers=True,
        title="Tráfico DL con Metadatos de Red",
        labels={"time_window": "Tiempo", "dl_vol_sum": "Bytes"}
    )
    fig.update_layout(yaxis_rangemode="tozero", legend=dict(orientation="h", y=-0.2))
    return fig

# [Resto de funciones: parquet_saver y main permanecen igual...]
async def parquet_saver():
    global agg_df
    while True:
        await asyncio.sleep(30)
        if not agg_df.is_empty():
            agg_df.write_parquet(PARQUET_FILE)

async def main():
    print("Iniciando Pipeline con Metadatos (PCI/PLMN/RNTI)...")
    asyncio.create_task(log_reader_producer(LOG_FILE, data_queue))
    asyncio.create_task(data_aggregator_consumer(data_queue))
    asyncio.create_task(parquet_saver())
    await asyncio.to_thread(app.run, host="127.0.0.1", port=8050, debug=False)

if __name__ == '__main__':
    asyncio.run(main())