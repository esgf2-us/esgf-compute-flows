"""
Runs the web processing services (WPS) for ESGF

Author:
    Max Grover - 4.2.2025

Note:
    Very much still in development
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
                           odir=None
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
        
    odir: str
        path to the desired output directory
    
    """
    import os
    
    from rooki import operators as ops
    from rooki import rooki
    import intake_esgf
    from intake_esgf import ESGFCatalog

    if odir is None:
        odir = os.getcwd()

    
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
    
    
    dset_dict = {}
    for rooki_id in rooki_ids:
        dset_dict[rooki_id] = run_workflow(variable_id[0], rooki_id)
    
    return list(dset_dict.values())

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