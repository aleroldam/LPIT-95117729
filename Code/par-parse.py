import polars as pl

# Configuración
PARQUET_FILE = "volumenes_agregados.parquet"

def leer_datos_parquet():
    try:
        # 1. Leer el archivo
        # Polars lee Parquet de forma casi instantánea
        df = pl.read_parquet(PARQUET_FILE)
        
        print("--- Primeras 5 filas del archivo ---")
        print(df.head())
        
        print("\n--- Información del esquema (Columnas y tipos) ---")
        print(df.schema)
        
        # 2. Ejemplo de análisis rápido: Suma total de bytes por UE
        resumen = (
            df.group_by("ue_id")
            .agg(pl.col("dl_vol_sum").sum().alias("Total_Bytes"))
            .sort("Total_Bytes", descending=True)
        )
        
        print("\n--- Volumen total acumulado por cada UE ---")
        print(resumen)

    except FileNotFoundError:
        print(f"Error: El archivo {PARQUET_FILE} no existe todavía.")
    except Exception as e:
        print(f"Ocurrió un error: {e}")

if __name__ == "__main__":
    leer_datos_parquet()