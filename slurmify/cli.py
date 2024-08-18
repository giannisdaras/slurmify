import click
import ast
from slurmify import job_submission

@click.group()
def main():
    """SLURM Utils - A utility library for SLURM job management"""
    pass

@main.command()
@click.option('--job-name', required=True, help='Name of the job')
@click.option('--partition', required=True, help='Partition (queue) name')
@click.option('--script-path', required=True, type=click.Path(exists=True), help='Path to the job script')
@click.option('--time-limit', default='01:00:00', help='Time limit for the job')
@click.option('--nodes', default=1, type=int, help='Number of nodes')
@click.option('--ntasks-per-node', default=1, type=int, help='Number of tasks per node')
@click.option('--cpus-per-task', default=1, type=int, help='Number of CPUs per task')
@click.option('--mem', default='0', help='Memory allocation')
@click.option('--array', help='Job array specification (e.g., "0-15" or "0-15%4")')
@click.option('--dependency', help='Job dependency specification')
def submit(**kwargs):
    """Submit a job to SLURM"""
    try:
        job_id = job_submission.submit_job(**kwargs)
        click.echo(f"Submitted job with ID: {job_id}")
    except Exception as e:
        click.echo(f"Error: {str(e)}", err=True)


@main.command()
@click.option('--main-job-name', required=True, help='Name of the main job array')
@click.option('--main-partition', required=True, help='Partition for the main job array')
@click.option('--main-script-path', required=True, type=click.Path(exists=True), help='Path to the main job script')
@click.option('--array', required=True, help='Range for the job array (e.g., "0-15" or "0-15%4")')
@click.option('--dependent-job-name', required=True, help='Name of the dependent jobs')
@click.option('--dependent-partition', required=True, help='Partition for the dependent jobs')
@click.option('--dependent-script-path', required=True, type=click.Path(exists=True), help='Path to the dependent job script')
@click.option('--dependency-type', default='afterany', help='Type of dependency (e.g., "afterany", "afterok")')
@click.option('--time-limit', default='01:00:00', help='Time limit for the jobs')
@click.option('--nodes', default=1, type=int, help='Number of nodes')
@click.option('--ntasks-per-node', default=1, type=int, help='Number of tasks per node')
@click.option('--cpus-per-task', default=1, type=int, help='Number of CPUs per task')
@click.option('--mem', default='0', help='Memory allocation')
def submit_array_with_deps(**kwargs):
    """Submit a job array with dependent jobs for each array task"""
    try:
        job_ids = job_submission.submit_array_with_dependencies(**kwargs)
        click.echo(f"Submitted main job array with ID: {job_ids[0]}")
        click.echo(f"Submitted {len(job_ids) - 1} dependent jobs")
    except Exception as e:
        click.echo(f"Error: {str(e)}", err=True)


@main.command()
@click.option('--job-name', required=True, help='Name of the job')
@click.option('--partition', required=True, help='Partition (queue) name')
@click.option('--account', required=False, help='Account name', default=None)
@click.option('--script-path', required=True, type=click.Path(exists=True), help='Path to the job script')
@click.option('--time-limit', required=True, help='Time limit for the job')
@click.option('--parameter', required=True, multiple=True, help='Parameter in the format name:value1,value2,value3')
@click.option('--max-resubmissions', default=3, type=int, help='Maximum number of resubmissions allowed')
@click.option('--nodes', default=1, type=int, help='Number of nodes')
@click.option('--ntasks-per-node', default=1, type=int, help='Number of tasks per node')
@click.option('--cpus-per-task', default=1, type=int, help='Number of CPUs per task')
@click.option('--mem', default='0', help='Memory allocation')
@click.option('--dependency', help='Job dependency specification')
def submit_parametric_array(**kwargs):
    """Submit a parametric array job to SLURM with automatic resubmission"""
    parameter_grid = {}
    for param in kwargs.pop('parameter'):
        name, values = param.split(':', 1)
        try:
            parameter_grid[name] = ast.literal_eval(values)
        except:
            parameter_grid[name] = values.split(',')

    try:
        job_id = job_submission.submit_parametric_array_with_resubmission(
            parameter_grid=parameter_grid,
            **kwargs
        )
        click.echo(f"Submitted parametric array job with resubmission. Initial job ID: {job_id}")
    except Exception as e:
        click.echo(f"Error: {str(e)}", err=True)

if __name__ == "__main__":
    main()