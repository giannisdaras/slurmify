import os
import subprocess
import logging
import time
import sys
from typing import Dict, Any, Optional

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_value(key: str, args: Dict[str, Any], default: Optional[str] = None) -> str:
    """Get value from args or environment variables."""
    value = args.get(key) or os.environ.get(f"SLURM_{key.upper()}")
    if value is None and default is None:
        raise ValueError(f"Missing required argument: {key}")
    return value or default

def submit_job(
    job_name: str,
    partition: str,
    output: str = "job_logs/R-%x.%A_%a.out",
    error: str = "job_logs/R-%x.%A_%a.err",
    time: str = "01:00:00",
    account: Optional[str] = None,
    nodes: int = 1,
    ntasks_per_node: int = 1,
    cpus_per_task: int = 1,
    mem: str = "0",
    network: Optional[str] = None,
    array: Optional[str] = None,
    script_content: str = "",
    **kwargs
) -> str:
    """
    Generate an sbatch script based on the provided arguments.
    """
    sbatch_options = {
        "job-name": job_name,
        "partition": partition,
        "output": output,
        "error": error,
        "time": time,
        "account": account,
        "nodes": nodes,
        "ntasks-per-node": ntasks_per_node,
        "cpus-per-task": cpus_per_task,
        "mem": mem,
        "network": network,
        "array": array,
    }

    # Update with any additional kwargs
    sbatch_options.update(kwargs)

    # Generate the sbatch script
    script = "#!/bin/bash\n"
    for key, value in sbatch_options.items():
        if value is not None:
            script += f"#SBATCH --{key}={value}\n"

    # Add the actual script content
    script += "\n" + script_content

    return script

def ensure_directory_exists(directory):
    """Ensure that the specified directory exists."""
    os.makedirs(directory, exist_ok=True)

def get_job_id_from_submission(submission_output: str) -> str:
    """Extract job ID from sbatch submission output."""
    return submission_output.strip().split()[-1]

def write_sbatch_script(script: str, job_name: str) -> str:
    """
    Write the generated sbatch script to a file in the job_logs directory.

    Args:
        script (str): The generated sbatch script
        job_name (str): Name of the job

    Returns:
        str: The path to the saved script file
    """
    ensure_directory_exists("job_logs")
    filename = f"job_logs/{job_name}_script.sh"
    with open(filename, "w") as f:
        f.write(script)
    return os.path.abspath(filename)

def submit_job_to_slurm(script_path: str) -> str:
    """
    Submit the job script to SLURM and return the job ID.

    Args:
        script_path (str): Path to the sbatch script file

    Returns:
        str: The job ID returned by SLURM

    Raises:
        RuntimeError: If the job submission fails
    """
    logger.info(f"Submitting job script: {script_path}")
    
    try:
        result = subprocess.run(['sbatch', script_path], capture_output=True, text=True, check=True)
        logger.info(f"Job submission output: {result.stdout}")
        job_id = get_job_id_from_submission(result.stdout)
        return job_id
    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to submit job. Return code: {e.returncode}")
        logger.error(f"Error output: {e.stderr}")
        raise RuntimeError(f"Failed to submit job: {e.stderr}")
    except Exception as e:
        logger.error(f"Unexpected error during job submission: {str(e)}")
        raise RuntimeError(f"Unexpected error during job submission: {str(e)}")

def get_job_state(job_id: str) -> str:
    """
    Get the current state of a job.

    Args:
        job_id (str): The ID of the job to check

    Returns:
        str: The current state of the job
    """
    try:
        result = subprocess.run(['scontrol', 'show', 'job', job_id], capture_output=True, text=True, check=True)
        for line in result.stdout.split('\n'):
            if line.strip().startswith('JobState='):
                # Extract only the state, not the reason
                return line.strip().split('=')[1].split()[0]
        return "UNKNOWN"
    except subprocess.CalledProcessError:
        return "UNKNOWN"

def parse_time_limit(time_limit: str) -> int:
    """
    Parse the time limit string and return the total seconds.

    Args:
        time_limit (str): Time limit in the format 'DD-HH:MM:SS', 'HH:MM:SS', 'MM:SS', or 'SS'

    Returns:
        int: Total seconds
    """
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


def start_background_monitoring(script_path: str, time_limit: str, max_resubmissions: int, log_file: str, job_name: str):
    """
    Start the job monitoring process in the background.

    Args:
        script_path (str): Path to the sbatch script file
        time_limit (str): Time limit of the job
        max_resubmissions (int): Maximum number of resubmissions allowed
        log_file (str): Path to the log file for background process output
        job_name (str): Name of the job
    """
    current_script = os.path.abspath(sys.argv[0])
    cmd = [
        "nohup", sys.executable, current_script, "background-monitor",
        "--script-path", script_path,
        "--time-limit", time_limit,
        "--max-resubmissions", str(max_resubmissions),
        "--job-name", job_name,
        f">> {log_file} 2>&1 &"
    ]
    
    subprocess.Popen(" ".join(cmd), shell=True, env=os.environ.copy())
    logger.info(f"Started background monitoring process. Check {log_file} for output.")


def background_monitor(script_path: str, time_limit: str, max_resubmissions: int, job_name: str):
    """
    Function to be run in the background for monitoring and resubmitting jobs.

    Args:
        script_path (str): Path to the sbatch script file
        time_limit (str): Time limit of the job
        max_resubmissions (int): Maximum number of resubmissions allowed
        job_name (str): Name of the job
    """
    logger.info(f"Starting background job monitoring for job: {job_name}")
    monitor_and_resubmit_job(script_path, time_limit, max_resubmissions)
    logger.info(f"Background job monitoring completed for job: {job_name}")

def monitor_and_resubmit_job(script_path: str, time_limit: str, max_resubmissions: int) -> None:
    """
    Monitor a job and resubmit it if it exits before the time limit.

    Args:
        script_path (str): Path to the sbatch script file
        time_limit (str): Time limit of the job
        max_resubmissions (int): Maximum number of resubmissions allowed
    """
    resubmissions = 0
    time_limit_seconds = parse_time_limit(time_limit)
    
    while resubmissions <= max_resubmissions:
        start_time = time.time()
        job_id = submit_job_to_slurm(script_path)
        logger.info(f"Job submitted with ID: {job_id}")

        while True:
            time.sleep(4)  # Check every minute
            current_state = get_job_state(job_id)
            elapsed_time = time.time() - start_time
            print(f"Checking: {current_state}")
            if current_state in ['COMPLETED', 'FAILED', 'CANCELLED', 'TIMEOUT']:
                if elapsed_time < time_limit_seconds:
                    resubmissions += 1
                    logger.info(f"Job {job_id} ended prematurely. Resubmitting (attempt {resubmissions}).")
                    break
                else:
                    logger.info(f"Job {job_id} completed after running for the full time limit.")
                    return
            elif current_state == 'UNKNOWN':
                logger.error(f"Unable to determine state of job {job_id}. Stopping monitoring.")
                return

        if resubmissions > max_resubmissions:
            logger.info(f"Reached maximum number of resubmissions ({max_resubmissions}). Stopping.")
            return


def submit_job_with_resubmission(
    job_name: str,
    partition: str,
    time_limit: str,
    max_resubmissions: int = 3,
    **kwargs
) -> None:
    """
    Submit a job with automatic resubmission if it exits prematurely.
    """
    ensure_directory_exists("job_logs")
    script = submit_job(job_name=job_name, partition=partition, time=time_limit, **kwargs)
    script_path = write_sbatch_script(script, job_name)
    logger.info(f"Sbatch script written to: {script_path}")
    
    job_id = submit_job_to_slurm(script_path)
    
    # Rename the script file to include the job ID
    new_script_path = f"job_logs/{job_name}_{job_id}_script.sh"
    os.rename(script_path, new_script_path)
    logger.info(f"Renamed script file to: {new_script_path}")
    
    log_file = f"job_logs/{job_name}_{job_id}_resubmission.log"
    start_background_monitoring(new_script_path, time_limit, max_resubmissions, log_file, job_name)