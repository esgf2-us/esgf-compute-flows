"""
Runs the web processing services (WPS) for ESGF

Author:
    Max Grover - 4.2.2025
    Updated by Benoit Cote - 4.2.2025

Note:
    Same as run_wps.py from 4.2.2025, but adding mechanism to share 
    result file with user through a shared local Guest Collection.
    odir argument has been removed to make sure the results always
    get written in the Guest Collection.
"""

# Import Globus Compute SDK
import globus_compute_sdk

def average_subset_by_time(node="DKRZ",
                           start_date="1990-01-01",
                           end_date="2000-01-01",
                           lat_min=0,
                           lat_max=35,
                           lon_min=65,
                           lon_max=100,
                           average_frequency="year",
                           experiment_id=["historical"],
                           variable_id=["tas"],
                           member_id=["r1i1p1f1"],
                           table_id=["Amon"],
                           institution_id=["MIROC"],
                           ):
    """
    Parameters
    ==========
    node: str, where you would like to run the WPS
        options: DKRZ, ORNL, ANL
    
    variable: str
        options: must be a valid variable in the vocabulary
    
    start_date: str
        Start date in YYYY-MM-DD for the temporal subset
    
    end_date: str
        End date in YYYY-MM-DD for the temporal subset
        
    lat_min: int
        Minimum latitude for the spatial subset
    
    lat_max: int
        Maximum latitude for the spatial subset
        
    lon_min: int
        Minimum longitude for the spatial subset
    
    lon_max: int
        Maximum longitude for the spatial subset
    
    average_frequency: str
        options: "year", "month", "day"

    # [Edited from original run_wps.py] -- removed "odir" since it is set automatically

    """
    import os
    import uuid # [Edited from original run_wps.py]
    from rooki import operators as ops
    from rooki import rooki
    import intake_esgf
    from intake_esgf import ESGFCatalog

    # =======================================
    # Below [Edited from original run_wps.py]

    # Create a UUID for the compute request
    REQUEST_ID = str(uuid.uuid4())

    # Details of the Guest Collection from where results will be shared with users
    GUEST_COLLECTION_ID = "9e5cf346-9d81-4d96-9eaa-9f07f8370478"
    GUEST_COLLECTION_BASE_PATH = "/eagle/projects/PortalDevelopment/esgf_collection_dev"
    
    # Assign the output folder and create the folder within the Guest Collection
    odir = os.path.join(GUEST_COLLECTION_BASE_PATH, REQUEST_ID)
    os.mkdir(odir)

    # TODO: If we can have the user's identity, we could set permissions on the guest collection's "odir" folder

    # Above [Edited from original run_wps.py]
    # =======================================
    
    if node == "ORNL":
        intake_esgf.conf.set(indices={"anl-dev": False,
                                      "ornl-dev": True})
        
        def build_rooki_id(id_list):
            rooki_id = id_list[0]
            rooki_id = rooki_id.split("|")[0]
            rooki_id = f"css03_data.{rooki_id}"  # <-- just something you have to know for now :(
            return rooki_id
        

    elif node == "DKRZ":
        intake_esgf.conf.set(indices={"anl-dev": False,
                                      "ornl-dev": False,
                                      "esgf-node.llnl.gov": True})
        
        def build_rooki_id(id_list):
            rooki_id = id_list[0]
            rooki_id = rooki_id.split("|")[0]
            return rooki_id
        
    else:
        raise NameError("Node not in allowed list ['ORNL', 'DKRZ']")
        
    
    def run_workflow(variable_id, rooki_id):
        workflow = ops.AverageByTime(
            ops.Subset(
            ops.Input(variable_id, [rooki_id]),
            time=f"{start_date}/{end_date}",
            area=f"{lon_min},{lat_min},{lon_max},{lat_max}",
        ),
        freq=average_frequency,
        )
    
        response = workflow.orchestrate()
        if not response.ok:
            raise ValueError(response)
        
        # Set the output dir dynamically
        response.output_dir = odir
        return response.download()[0]
        
    
    # Search the catalog
    cat = ESGFCatalog().search(experiment_id=experiment_id,
                               variable_id=variable_id,
                               member_id=member_id,
                               table_id=table_id,
                               institution_id=institution_id
                              )
    
    # Apply the id building
    rooki_ids = cat.df.id.apply(build_rooki_id)
    
    # =======================================
    # Below [Edited from original run_wps.py] - Now returning the URL to recover results with Globus
    
    # Create URL link to point users to their results through the Globus webapp
    results_url = ""
    results_url += "https://app.globus.org/file-manager?"
    results_url += f"origin_id={GUEST_COLLECTION_ID}&"
    results_url += f"origin_id={GUEST_COLLECTION_ID}&"
    results_url += f"origin_path={REQUEST_ID}&"
    results_url += "two_pane=true"

    # Return the results URL link
    return results_url

# Creating Globus Compute client
gcc = globus_compute_sdk.Client()

# Register the function
COMPUTE_FUNCTION_ID = gcc.register_function(average_subset_by_time)

# Write function UUID in a file
uuid_file_name = "average_subset_by_time.txt"
with open(uuid_file_name, "w") as file:
    file.write(COMPUTE_FUNCTION_ID)
    file.write("\n")
file.close()

# End of script
print("Function registered with UUID -", COMPUTE_FUNCTION_ID)
print("The UUID is stored in " + uuid_file_name + ".")
print("")