import click
import logging
from slurm_utils.job_submission import (
    submit_job, write_sbatch_script, submit_job_to_slurm,
    submit_job_with_resubmission, background_monitor as bg_monitor
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@click.group()
def main():
    """SLURM Utils - A utility library for SLURM job management"""
    pass

@main.command()
@click.option('--job-name', required=True, help='Name of the job')
@click.option('--partition', required=True, help='Partition (queue) name')  # Now a required option
@click.option('--output', default='job_logs/R-%x.%A_%a.out', help='Output file path')
@click.option('--error', default='job_logs/R-%x.%A_%a.err', help='Error file path')
@click.option('--account', help='Account name')
@click.option('--nodes', default=1, type=int, help='Number of nodes')
@click.option('--ntasks-per-node', default=1, type=int, help='Number of tasks per node')
@click.option('--cpus-per-task', default=1, type=int, help='Number of CPUs per task')
@click.option('--mem', default='0', help='Memory allocation')
@click.option('--network', help='Network type')
@click.option('--time', default='01:00:00', help='Time limit for the job')
@click.option('--array', help='Job array specification')
@click.option('--script-file', type=click.Path(exists=True), help='Path to the script file to be executed')
@click.option('--submit/--no-submit', default=False, help='Submit the job to SLURM after creating the script')
def submit(job_name, partition, output, error, account, nodes, ntasks_per_node, cpus_per_task, mem, network, time, array, script_file, submit):
    """Submit a job to SLURM"""
    try:
        if script_file:
            with open(script_file, 'r') as f:
                script_content = f.read()
        else:
            script_content = ""

        script = submit_job(
            job_name=job_name,
            partition=partition,  # Now included in the function call
            output=output,
            error=error,
            account=account,
            nodes=nodes,
            ntasks_per_node=ntasks_per_node,
            cpus_per_task=cpus_per_task,
            mem=mem,
            network=network,
            time=time,
            array=array,
            script_content=script_content
        )

        script_path = write_sbatch_script(script)
        click.echo(f"Sbatch script written to: {script_path}")

        if submit:
            job_id = submit_job_to_slurm(script_path)
            click.echo(f"Job submitted successfully. Job ID: {job_id}")
    except Exception as e:
        logger.error(f"Error during job submission: {str(e)}")
        click.echo(f"Error: {str(e)}", err=True)

@main.command()
@click.option('--job-name', required=True, help='Name of the job')
@click.option('--partition', required=True, help='Partition (queue) name')
@click.option('--time', required=True, help='Time limit for the job (e.g., "1:00:00" for 1 hour)')
@click.option('--max-resubmissions', default=3, type=int, help='Maximum number of resubmissions allowed')
@click.option('--output', default='job_logs/R-%x.%A_%a.out', help='Output file path')
@click.option('--error', default='job_logs/R-%x.%A_%a.err', help='Error file path')
@click.option('--account', help='Account name')
@click.option('--nodes', default=1, type=int, help='Number of nodes')
@click.option('--ntasks-per-node', default=1, type=int, help='Number of tasks per node')
@click.option('--cpus-per-task', default=1, type=int, help='Number of CPUs per task')
@click.option('--mem', default='0', help='Memory allocation')
@click.option('--network', help='Network type')
@click.option('--array', help='Job array specification')
@click.option('--script-file', type=click.Path(exists=True), help='Path to the script file to be executed')
def submit_with_resubmission(job_name, partition, time, max_resubmissions, output, error, account, nodes, ntasks_per_node, cpus_per_task, mem, network, array, script_file):
    """Submit a job with automatic resubmission if it exits prematurely"""
    try:
        if script_file:
            with open(script_file, 'r') as f:
                script_content = f.read()
        else:
            script_content = ""

        submit_job_with_resubmission(
            job_name=job_name,
            partition=partition,
            time_limit=time,
            max_resubmissions=max_resubmissions,
            output=output,
            error=error,
            account=account,
            nodes=nodes,
            ntasks_per_node=ntasks_per_node,
            cpus_per_task=cpus_per_task,
            mem=mem,
            network=network,
            array=array,
            script_content=script_content
        )
    except Exception as e:
        logger.error(f"Error during job submission with resubmission: {str(e)}")
        click.echo(f"Error: {str(e)}", err=True)

@main.command()
@click.option('--script-path', required=True, help='Path to the sbatch script file')
@click.option('--time-limit', required=True, help='Time limit of the job')
@click.option('--max-resubmissions', required=True, type=int, help='Maximum number of resubmissions allowed')
@click.option('--job-name', required=True, help='Name of the job')
def background_monitor(script_path, time_limit, max_resubmissions, job_name):
    """Run the background monitoring process"""
    bg_monitor(script_path, time_limit, max_resubmissions, job_name)


if __name__ == "__main__":
    main()