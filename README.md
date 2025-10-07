TL;DR:
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