# Compute Environment

Instructions on how the compute environment can be setup (using Python 3.12.4).

## ESGF Environment

**Using Conda**:
The following has been tested on a local machine (MacOS):

```bash
# Clone ESGF repo to access the environment file
git clone https://github.com/ESGF2-US/esgf-compute-flows
cd esgf-compute-flows/compute_environment

# Create the virtual environment (tested with Python 3.12.4)
conda env create -f environment.yml
conda activate esgf-compute-flows-dev

# Downgrade cryptography to avoid having the warning messages
pip install cryptography==42.0.0
```

**Using Miniconda**:
To create your miniconda3 environment, isolate the `.sh` file that fits the operating system hosting your web portal (find the complete list [here](https://repo.anaconda.com/miniconda/)).

Create the miniconda3 folder.
```bash
YOUR_SELECTED_FILE="<your-selected-file.sh>"
wget https://repo.anaconda.com/miniconda/$YOUR_SELECTED_FILE
chmod +x $YOUR_SELECTED_FILE
./$YOUR_SELECTED_FILE -b -p ./miniconda3/
rm $YOUR_SELECTED_FILE
```

## Globus Compute Function

Register the ESGF WPS function.
```bash
python register_functions/run_wps.py
```

This will generate a text file with the function UUID (`average_subset_by_time.txt`).

## Globus Compute Endpoint

Create an endpoint using the `esgf_wps_local_config.yaml` config file. Make sure to 1) customize the `worker_init` field to activate your environment and 2) add the function UUIDs in the `allowed_functions` field.
```bash
globus-compute-endpoint configure --endpoint-config endpoint_configs/YOUR-TARGER-CONFIE-FILE.yaml esgf_wps
globus-compute-endpoint start esgf_wps
```

To check your compute endpoint uuids, execute the following command:
```bash
globus-compute-endpoint list
```

## Test Framework

To test the Globus Compute framework, create an `.env` file with your Globus Compute endpoint and function UUIDs, and the full path (without `~/`) of your output directory (`odir`).
```bash
ENDPOINT_UUID="your-compute-endpoint-uuid"
FUNCTION_UUID="your-compute-function-uuid"
ODIR="your-full-path-output-directory"
```

Install dotenv to allow the testing function to access the environment variables in the `.env` file.
```bash
pip install python-dotenv
```

Run the test script.
```bash
python test_function.py
```

This should print the list of output files and an example of a generated dataset. The Globus Compute function itself returns the list of output files.