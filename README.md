TL;DR:

Polygon.io API
     │
     ▼
Python (requests + pagination)
     │
     ▼
tickers.csv
     │
     ▼
Pandas DataFrame
     │
     ▼
Snowflake (STOCK_TICKERS table)

📊 Stock Tickers Data Pipeline (Polygon → CSV → Snowflake)
📌 Descripción general

Este proyecto implementa un pipeline de ingesta y carga de datos financieros que:

Extrae tickers activos del mercado de acciones desde la API de Polygon.io
Maneja paginación y límites de la API (Free Tier)
Normaliza los datos y los guarda en un archivo CSV
Carga los datos en Snowflake usando un esquema explícito
Registra la ejecución del pipeline para trazabilidad y monitoreo

Este pipeline está diseñado para ejecutarse de forma programada (Task Scheduler / cron)

# Libraries Used

import os # gets into local system
import time   # lets you pause execution between API calls (rate limits)
import csv    # to write ticker data into CSV format
import requests  # makes HTTP calls to Polygon.io
import traceback  # prints full error tracebacks when exceptions occur
from datetime import datetime  # used to generate timestamps
from dotenv import load_dotenv  # load secrets (.env file)
import snowflake.connector  # official Snowflake connector
from snowflake.connector.pandas_tools import write_pandas  # helper to load DataFrames
import pandas as pd  # DataFrame handling
