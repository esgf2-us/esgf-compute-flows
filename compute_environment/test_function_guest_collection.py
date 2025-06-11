from globus_compute_sdk import Client, Executor
import xarray as xr
from dotenv import load_dotenv
import os

# Extract variables from .env file
load_dotenv()
endpoint_uuid = os.getenv("ENDPOINT_UUID")
function_uuid = os.getenv("FUNCTION_UUID")

# Create Globus SDK Executor (currently using your own user credentials)
gcc = Client()
gce = Executor(endpoint_id=endpoint_uuid, client=gcc, amqp_port=443)

# Prepare payload for ESGF ingest-wxt
data = {
    "node": "DKRZ",
    "start_date":"1990-01-01",
    "end_date":"2000-01-01",
    "lat_min":0,
    "lat_max":35,
    "lon_min":65,
    "lon_max":100,
    "average_frequency":"year",
    "experiment_id":["historical"],
    "variable_id":["tas"],
    "member_id":["r1i1p1f1"],
    "table_id":["Amon"],
    "institution_id":["MIROC"],
}

# Start the task
future = gce.submit_to_registered_function(function_uuid, kwargs=data)

# Wait and print the result
result = future.result()
print(f"URL to access the results: {result}")