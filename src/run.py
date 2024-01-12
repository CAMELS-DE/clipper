import os
import json
import glob
from pathlib import Path

import papermill as pm
from json2args import get_parameter
from concurrent.futures import ProcessPoolExecutor

from db import DuckProcessor

# we can update the input.json, before reading it, with environment variables passed in from
# the cloud environment.
if os.environ.get('CATCHMENT_ID') is not None:
    with open('/in/inputs.json', 'r') as f:
        conf = json.load(f)
    with open('/in/inputs.json', 'w') as f:
        conf['hyras']['parameters']['catchment_id'] = os.environ['CATCHMENT_ID']
        json.dump(conf, f, indent=4)


# get the parameters passed from via a json file
kwargs = get_parameter()

# extracte the parameters needed
catchment_id = kwargs['catchment_id']
catchments_path = kwargs['catchments']

# instantiate a Processor
processor = DuckProcessor(
    target_bucket='camels_output_data',
    notebook_bucket='camels_notebooks',
    catchments_path=catchments_path,
    local_hyras_cache='/hyras_cache'
)

# handle tool name
tool_name = os.environ.get('RUN_TOOL', 'hyras')
if tool_name != 'hyras':
    raise ValueError(f'Unknown tool name: {tool_name}')

# helper to save errors to a file
def error(msg: str, path: str):
    with open(path, 'a') as f:
        f.write(msg)


# define the function to process a single catchment and variable
def process_step(variable: str) -> bool:
    try:
        pm.execute_notebook(
            'clip_catchment_from_hyras.ipynb',
            f'clip_{variable}_for_catchment_{catchment_id}_from_hyras.ipynb',
            parameters=dict(
                catchment_id=catchment_id,
                variable=variable,
                local_cache=None,   # this is handled within the notebook
                catchments_path=catchments_path,
                WITH_PAPERMILL=True
            )
        )
    except Exception as e:
        # save the exeption to a file in GCE
        msg = f"\n-------\nERRORED ON VARIABLE: {variable}\n-------\n{str(e)}"
        error(msg, f"{catchment_id}/errors.txt")
    
    return True


with ProcessPoolExecutor() as executor:
    variables = ['hurs', 'rsds', 'pr', 'tas', 'tasmax', 'tasmin']

    # call the variables one by one and get a future
    futures = [executor.submit(process_step, variable) for variable in variables]

    # wait for all futures to finish
    for future, variable in zip(futures, variables):
        try:
            # wait for the result
            future.result(timeout=60*30)

            # upload notebook
            notebook_name = f'clip_{variable}_for_catchment_{catchment_id}_from_hyras.ipynb'
            processor.upload_notebook(f"{processor.result_prefix}/{notebook_name}", notebook_name)
        except Exception as e:
            msg = f"\n-------\nPAPERMILL EXECUTION FAILED ON VARIABLE: {variable}\n-------\n{str(e)}"
            error(msg, f"{catchment_id}/errors.txt")

    # now all results should be there, so upload
    for filename in glob.glob(f"{catchment_id}/*"):
        blob_name = f"{processor.result_prefix}/{filename}"
        processor.upload(blob_name=blob_name, source=filename)

