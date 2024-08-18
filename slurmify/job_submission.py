import os
import subprocess
import logging
import sys
import time
import itertools
import functools
from typing import Dict, Any, Optional, List, Union, Tuple
import threading
import random
import math
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def submit_job(
    job_name: str,
    partition: str,
    script_path: str,
    output: str = "job_logs/R-%x.%A_%a.out",
    error: str = "job_logs/R-%x.%A_%a.err",
    time_limit: str = "01:00:00",
    account: Optional[str] = None,
    nodes: int = 1,
    ntasks_per_node: int = 1,
    cpus_per_task: int = 1,
    mem: str = "0",
    array: Optional[str] = None,
    dependency: Optional[str] = None,
    additional_params: Optional[Dict[str, Any]] = None
) -> str:
    """Submit a job to SLURM."""
    if not partition:
        raise ValueError("Partition must be specified for job submission")

    sbatch_options = [
        f"--job-name={job_name}",
        f"--partition={partition}",
        f"--output={output}",
        f"--error={error}",
        f"--time={time_limit}",
        f"--nodes={nodes}",
        f"--ntasks-per-node={ntasks_per_node}",
        f"--cpus-per-task={cpus_per_task}",
        f"--mem={mem}",
    ]

    if account:
        sbatch_options.append(f"--account={account}")
    if array:
        sbatch_options.append(f"--array={array}")
    if dependency:
        sbatch_options.append(f"--dependency={dependency}")
    if additional_params:
        for key, value in additional_params.items():
            sbatch_options.append(f"--{key}={value}")

    return _submit_job_to_slurm(script_path, sbatch_options)

def _submit_job_to_slurm(script_path: str, sbatch_options: List[str]) -> str:
    """Submit the job script to SLURM and return the job ID."""
    logger.info(f"Submitting job script: {script_path}")
    
    try:
        if not os.path.exists(script_path):
            raise FileNotFoundError(f"Script file not found: {script_path}")

        if not os.access(script_path, os.R_OK):
            raise PermissionError(f"Script file is not readable: {script_path}")
        while True:
            try:
                cmd = ["sbatch"] + sbatch_options + [script_path]
                result = subprocess.run(cmd, capture_output=True, text=True, check=True)
                logger.info(f"Job submission output: {result.stdout}")
                return result.stdout.strip().split()[-1]
            except subprocess.CalledProcessError as e:
                logger.error(f"Error during job submission: {e.stderr}. Will sleep 1m and try again.")
                time.sleep(60)
    except (FileNotFoundError, PermissionError) as e:
        logger.error(f"Error during job submission: {str(e)}")
        raise RuntimeError(f"Failed to submit job: {str(e)}")

def get_job_state(job_id: str) -> List[Tuple[str, str]]:
    """Get the current state of a job."""
    try:
        result = subprocess.run(['scontrol', 'show', 'job', job_id], capture_output=True, text=True, check=True)
        for line in result.stdout.split('\n'):
            line = line.strip()
            if line.startswith('JobState='):
                current_state = line.split('=')[1].split()[0]
                return current_state

        # If no states were found, return UNKNOWN
        return "UNKNOWN"

    except subprocess.CalledProcessError:
        return "UNKNOWN"

def parse_time_limit(time_limit: str) -> int:
    """Parse the time limit string and return the total seconds."""
    parts = time_limit.replace('-', ':').split(':')
    parts = [int(part) for part in parts]
    
    if len(parts) == 4:  # DD-HH:MM:SS
        return parts[0] * 86400 + parts[1] * 3600 + parts[2] * 60 + parts[3]
    elif len(parts) == 3:  # HH:MM:SS
        return parts[0] * 3600 + parts[1] * 60 + parts[2]
    elif len(parts) == 2:  # MM:SS
        return parts[0] * 60 + parts[1]
    elif len(parts) == 1:  # SS
        return parts[0]
    else:
        raise ValueError(f"Invalid time limit format: {time_limit}")

def monitor_and_resubmit_job(job_id: int, task_id: int, time_limit: str, max_resubmissions: int, submit_fn) -> None:
    """Monitor a job and resubmit it if it exits before the time limit."""
    resubmissions = 0
    time_limit_seconds = parse_time_limit(time_limit)
    
    while resubmissions <= max_resubmissions:
        start_time = time.time()
        while True:
            time.sleep(30) # Check every 30 seconds
            elapsed_time = time.time() - start_time
            job_state = get_job_state(f"{job_id}_{task_id}")
            if job_state == "PENDING":
                logger.info(f"Job {job_id}, Task id {task_id} is pending.")
                continue
            if job_state != "RUNNING":  
                if elapsed_time < time_limit_seconds and job_state != "COMPLETED":
                    logger.info(f"Job {job_id}, Task id {task_id} ended prematurely. Resubmitting (attempt {resubmissions}).")
                    job_id = submit_fn(array=f"{task_id}-{task_id}")
                    logger.info(f"Job submitted with ID: {job_id}")
                else:
                    logger.info(f"Job {job_id}, Task id {task_id} finished normally")
                    return

def get_array_job_info(job_id: int) -> Dict[str, str]:
    """Get the state of each task in an array job.
    Args:
        job_id (int): Job ID of the array job
    Returns:
        Dict[str, str]: Dictionary mapping task IDs to their state.
    """
    command = f"squeue -j {job_id} -o '%i %t' --noheader"
    result = subprocess.run(command, shell=True, capture_output=True, text=True)
    
    # Split the output into lines
    lines = result.stdout.strip().split('\n')
    
    states = {}
    # Process each line (skip the header if present)
    for line in lines:
        if "PD" in line:
            if "-" in line:
                # many are waiting together
                task_ids = line.split("_")[1].split()[0]
                start_id = int(task_ids.split("-")[0][1:])
                end_id = int(task_ids.split("-")[1][:-1])
                for i in range(start_id, end_id+1):
                    states[f"{job_id}_{i}"] = "PD"
            elif "[" in line:
                # one is waiting, but it is part of an array job
                task_id = line.split("_")[1].split()[0][1:]
                states[f"{job_id}_{task_id}"] = "PD"
            else:
                # one is waiting, but it is not part of an array job
                task_id = line.split("_")[1].split()[0]
                states[f"{job_id}_{task_id}"] = "PD"
        else:
            parts = line.split()
            if len(parts) == 2:
                full_job_id, state = parts
                job_parts = full_job_id.split('_')
                
                if len(job_parts) > 1:
                    formatted_job_id = f"{job_parts[0]}_{job_parts[1]}"
                else:
                    formatted_job_id = f"{job_parts[0]}_0"
                states[formatted_job_id] = state
    return states


def submit_array_with_dependencies(
    main_job_name: str,
    main_partition: str,
    main_script_path: str,
    array: str,
    dependent_job_name: str,
    dependent_partition: str,
    dependent_script_path: str,
    dependency_type: str = "afterany",
    **kwargs
) -> List[str]:
    """Submit a job array and dependent jobs for each array task."""
    main_job_id = submit_job(
        job_name=main_job_name,
        partition=main_partition,
        script_path=main_script_path,
        array=array,
        **kwargs
    )
    
    dependent_job_ids = []
    array_size = int(array.split('-')[-1]) + 1
    
    for i in range(array_size):
        dependency = f"{dependency_type}:{main_job_id}_{i}"
        dependent_job_id = submit_job(
            job_name=f"{dependent_job_name}_{i}",
            partition=dependent_partition,
            script_path=dependent_script_path,
            dependency=dependency,
            **kwargs
        )
        dependent_job_ids.append(dependent_job_id)
    
    return [main_job_id] + dependent_job_ids


def submit_parametric_array_job(
    job_name: str,
    partition: str,
    script_path: str,
    time_limit: str,
    parameter_grid: Dict[str, List[Any]],
    array: Optional[str] = None,
    **kwargs
):
    """
    Submit a parametric array job to SLURM.
    
    :param job_name: Name of the job
    :param partition: SLURM partition to use
    :param script_path: Path to the job script
    :param time_limit: Time limit for each job in the array
    :param parameter_grid: Dictionary of parameter names and their possible values
    :param array: Array specification (e.g., "0-15" or "0,1,2,5-8")
    :param kwargs: Additional keyword arguments for job submission
    :return: Job ID of the submitted array job and the submit function
    """
    # Generate all combinations of parameters
    param_names = list(parameter_grid.keys())
    param_values = list(itertools.product(*parameter_grid.values()))
    
    # Ensure the job_logs directory exists
    os.makedirs('job_logs', exist_ok=True)
    
    # Generate a random 5-digit number for the file names
    random_id = f"{random.randint(10000, 99999):05d}"
    
    # Create a temporary file to store parameter combinations
    param_file = f"job_logs/job_params_{job_name}_{random_id}.txt"
    with open(param_file, 'w') as f:
        for i, combo in enumerate(param_values):
            param_str = ' '.join(f'--{name} {value}' for name, value in zip(param_names, combo))
            f.write(f"{param_str}\n")
    
    # Modify the script to read parameters from the file
    modified_script = f"job_logs/job_script_{job_name}_{random_id}.sh"

    with open(modified_script, 'w') as modified:
        modified.write("#!/bin/bash\n")
        modified.write(f"PARAM_LINE=$(sed -n \"$((SLURM_ARRAY_TASK_ID + 1))\"p {param_file})\n")
        modified.write("eval set -- $PARAM_LINE\n")
        modified.write("while [ $# -gt 0 ]; do\n")
        modified.write("    case \"$1\" in\n")
        for param in param_names:
            modified.write(f"        --{param}) {param.upper()}=\"$2\"; shift 2 ;;\n")
        modified.write("        *) shift ;;\n")
        modified.write("    esac\n")
        modified.write("done\n\n")
        for param in param_names:
            modified.write(f'export {param.upper()}="${param.upper()}"\n')
        
        module_path = script_path.replace('/', '.').replace('.py', '')
        # load imports
        modified.write(f"eval \"$(python -c 'from {module_path} import setup; print(setup())')\" \n")
        # run
        python_cmd = f"eval \"$(python -c 'from {module_path} import run; run()')\""
        modified.write(python_cmd)
    
    # Submit the array job
    array_size = len(param_values) - 1 if array is None else array  # Use provided array if available

    submit_fn = functools.partial(submit_job, 
                      job_name=job_name, 
                      partition=partition, 
                      script_path=modified_script, 
                      time_limit=time_limit, 
                      **kwargs)
    job_id = submit_fn(array=array_size)
    return job_id, submit_fn


def get_qos_limits(partition: str) -> Dict[str, int]:
    """Get QOS limits for the given partition."""
    try:
        result = subprocess.run(['scontrol', 'show', 'partition', partition], capture_output=True, text=True, check=True)
        qos_name = None
        for line in result.stdout.split('\n'):
            if 'QoS=' in line:
                qos_name = line.split('QoS=')[1].strip()
                break
        
        if not qos_name:
            logger.warning(f"QoS not found for partition {partition}")
            return {}
        
        result = subprocess.run(['sacctmgr', 'show', 'qos', qos_name, 'format=MaxSubmitJobsPerUser,MaxJobsPerUser', '--noheader'], capture_output=True, text=True, check=True)
        limits = result.stdout.strip().split()
        return {
            'MaxSubmitJobsPerUser': int(limits[0]),
            'MaxJobsPerUser': int(limits[1])
        }
    except subprocess.CalledProcessError as e:
        logger.error(f"Error getting QoS limits: {e}")
        return {}

def split_array(array_spec: str, max_size: int) -> List[str]:
    """Split an array specification into subarrays of maximum size."""
    start, end = map(int, array_spec.split('-'))
    total_size = end - start + 1
    num_subarrays = math.ceil(total_size / max_size)
    
    subarrays = []
    for i in range(num_subarrays):
        subarray_start = start + i * max_size
        subarray_end = min(subarray_start + max_size - 1, end)
        subarrays.append(f"{subarray_start}-{subarray_end}")
    
    return subarrays


def submit_parametric_array_with_resubmission(
    job_name: str,
    partition: str,
    script_path: str,
    time_limit: str,
    parameter_grid: Dict[str, List[Any]],
    max_resubmissions: int = 3,
    **kwargs
) -> List[str]:
    """
    Submit a parametric array job with automatic resubmission and subarray splitting.
    
    :param job_name: Name of the job
    :param partition: SLURM partition to use
    :param script_path: Path to the job script
    :param time_limit: Time limit for each job in the array
    :param parameter_grid: Dictionary of parameter names and their possible values
    :param max_resubmissions: Maximum number of resubmissions allowed
    :param kwargs: Additional keyword arguments for job submission
    :return: List of Job IDs of the submitted array jobs
    """
    # Calculate total number of combinations
    total_combinations = 1
    for values in parameter_grid.values():
        total_combinations *= len(values)
    array_size = total_combinations - 1  # Subtract 1 as SLURM array indices are 0-based

    # Get QoS limits
    qos_limits = get_qos_limits(partition)
    max_array_size = min(qos_limits.get('MaxSubmitJobsPerUser', array_size), 
                         qos_limits.get('MaxJobsPerUser', array_size))

    if max_array_size <= 0:
        logger.warning(f"Could not determine QoS limits for partition {partition}. Submitting as a single array job.")
        max_array_size = array_size

    # Split the array if necessary
    subarrays = split_array(f"0-{array_size}", max_array_size)
    job_ids = []
    for i, subarray in enumerate(subarrays):
        sub_job_name = f"{job_name}_part{i+1}"        
        job_id, submit_fn = submit_parametric_array_job(
            job_name=sub_job_name,
            partition=partition,
            script_path=script_path,
            time_limit=time_limit,
            parameter_grid=parameter_grid,
            array=subarray,
            **kwargs
        )
        job_ids.append(job_id)
        
        job_infos = get_array_job_info(job_id)
        threads = []
        for job_info in job_infos:
            task_id = job_info.split("_")[1]
            thread = threading.Thread(target=functools.partial(monitor_and_resubmit_job,
                job_id, task_id, time_limit, max_resubmissions, submit_fn))
            thread.start()
            threads.append(thread)
    
    return job_ids
