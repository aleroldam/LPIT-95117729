import asyncio
from pathlib import Path
import re
from datetime import datetime
import time

import polars as pl
from dash import Dash, dcc, html, Input, Output
import plotly.express as px

# Configuración
LOG_FILE = "cu-lan-ho-live.log"
PARQUET_FILE = "volumenes_agregados.parquet"


# OBJETIVO 2: Identificar los mensajes SDAP (Tráfico DL)
# ==============================================================================
PATTERN_SDAP = re.compile(
    r'^(?P<timestamp>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6})\s+\[SDAP\s*\]\s+\[.\]\s+ue=(?P<ue_id>\d+).*DL:.*pdu_len=(?P<dl_vol>\d+)'
)

# Colas y Variables Globales Compartidas
data_queue = asyncio.Queue()

# OBJETIVO 3: Almacenamiento de datos crudos (en memoria temporalmente por eficiencia)
raw_buffer = [] 

# OBJETIVO 4: DataFrame de datos agregados (Global para que Dash lo lea)
# Esquema: time_window (datetime), ue_id (str), dl_vol_sum (float)
agg_df = pl.DataFrame(schema={"time_window": pl.Datetime, "ue_id": pl.Utf8, "dl_vol_sum": pl.Float64})

# ==============================================================================
# OBJETIVO 1 & 2: Productor - Leer en tiempo real e identificar
# ==============================================================================
async def log_reader_producer(path: str, queue: asyncio.Queue):
    file = Path(path)
    print(f"Esperando a que se genere el log: {path}...")

    while not file.exists():
        await asyncio.sleep(0.5)

    with open(path, "r", encoding="utf-8") as f:
        print(f"Log {path} abierto. Leyendo en tiempo real...")
        f.seek(0, 2)  # Ir al final del archivo (tail -f)

        while True:
            line = f.readline()
            if not line:
                await asyncio.sleep(0.1)
                continue

            # Buscar patrón SDAP en la línea
            match = PATTERN_SDAP.search(line)
            if match:
                data = match.groupdict()
                # Parsear los datos
                record = {
                    "timestamp": datetime.fromisoformat(data["timestamp"]),
                    "ue_id": f"UE_{data['ue_id']}",
                    "dl_vol": float(data["dl_vol"])
                }
                await queue.put(record)

# ==============================================================================
# OBJETIVO 3 & 4: Consumidor - Guardar crudo y Agregar cada 1 segundo
# ==============================================================================
async def data_aggregator_consumer(queue: asyncio.Queue):
    global raw_buffer, agg_df
    
    last_agg_time = time.time()

    while True:
        # Recibir dato de la cola (espera máximo 0.5s para no bloquear la agregación)
        try:
            record = await asyncio.wait_for(queue.get(), timeout=0.5)
            raw_buffer.append(record)  # Guardar crudo (Objetivo 3)
            queue.task_done()
        except asyncio.TimeoutError:
            pass # No llegaron datos nuevos en este medio segundo, continuamos

        current_time = time.time()
        
        # Agregar cada 1 segundo (Objetivo 4)
        if current_time - last_agg_time >= 1.0:
            if raw_buffer:
                # Convertir buffer crudo a DataFrame
                df_raw = pl.DataFrame(raw_buffer)
                
                # Truncar los timestamps al segundo para agrupar
                df_raw = df_raw.with_columns(
                    pl.col("timestamp").dt.truncate("1s").alias("time_window")
                )
                
                # Agrupar por ventana de 1 segundo y por UE, sumar el volumen
                df_grouped = (
                    df_raw.group_by(["time_window", "ue_id"])
                    .agg(pl.col("dl_vol").sum().alias("dl_vol_sum"))
                )

                # Unir al DataFrame global agregado
                agg_df = pl.concat([agg_df, df_grouped], how="vertical")
                
                # Limpiar el buffer crudo para el siguiente segundo
                raw_buffer = []
                
            last_agg_time = current_time

# ==============================================================================
# OBJETIVO 6: Guardar en Parquet cada 30 segundos
# ==============================================================================
async def parquet_saver():
    global agg_df
    while True:
        await asyncio.sleep(30) # Esperar 30 segundos
        
        if not agg_df.is_empty():
            print(f"[{datetime.now().time()}] Guardando evolución en {PARQUET_FILE}...")
            # Guardar sobrescribiendo o añadiendo (Polars no añade a parquet nativamente fácil, sobrescribimos el histórico)
            agg_df.write_parquet(PARQUET_FILE)
            print("Guardado exitoso.")

# ==============================================================================
# OBJETIVO 5: Representar en tiempo real con Dash
# ==============================================================================
app = Dash(__name__)

app.layout = html.Div([
    html.H3("Evolución de Volúmenes DL SDAP por UE"),
    dcc.Graph(id="live-graph"),
    dcc.Interval(id="interval-component", interval=1000, n_intervals=0) # Actualizar cada 1s
])

@app.callback(
    Output("live-graph", "figure"),
    Input("interval-component", "n_intervals")
)
def update_graph(n):
    global agg_df
    if agg_df.is_empty():
        return px.line(title="Esperando datos...")

    # --- LA SOLUCIÓN ESTÁ AQUÍ ---
    # Ordenar estrictamente el DataFrame de Polars por tiempo
    sorted_df = agg_df.sort("time_window")
    # -----------------------------

    # Convertir a Pandas el DataFrame ya ordenado
    df_pd = sorted_df.to_pandas()
    
    # Crear gráfico de líneas (ahora ya vendrá ordenado)
    fig = px.line(
        df_pd, 
        x="time_window", 
        y="dl_vol_sum", 
        color="ue_id",
        markers=True,
        title="Tráfico DL Agregado (1s) por User Equipment (Corregido)",
        labels={"time_window": "Tiempo", "dl_vol_sum": "Volumen Total (Bytes)"}
    )
    return fig

# ==============================================================================
# Main Loop
# ==============================================================================
async def main():
    print("Iniciando Pipeline Fase 1...")
    
    # Iniciar las tareas asíncronas en segundo plano
    asyncio.create_task(log_reader_producer(LOG_FILE, data_queue))
    asyncio.create_task(data_aggregator_consumer(data_queue))
    asyncio.create_task(parquet_saver())

    # Ejecutar Dash en un hilo separado
    await asyncio.to_thread(
        app.run,
        host="127.0.0.1",
        port=8050,
        debug=False,
    )

if __name__ == '__main__':
    asyncio.run(main())