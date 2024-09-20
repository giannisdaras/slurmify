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
@click.option('--account', required=False, help='Account name', default=None)
@click.option('--script-path', required=True, type=click.Path(exists=True), help='Path to the job script')
@click.option('--time-limit', required=True, help='Time limit for the job')
@click.option('--parameter', required=True, multiple=True, help='Parameter in the format name:value1,value2,value3 or name:value')
@click.option('--max-resubmissions', default=3, type=int, help='Maximum number of resubmissions allowed')
@click.option('--nodes', default=1, type=int, help='Number of nodes')
@click.option('--ntasks-per-node', default=1, type=int, help='Number of tasks per node')
@click.option('--cpus-per-task', default=1, type=int, help='Number of CPUs per task')
@click.option('--mem', default='0', help='Memory allocation')
@click.option('--dependency', help='Job dependency specification')
@click.option('--check_worthiness', default=False, type=bool, help='Check if the job is worth running')
def submit_parametric_array(**kwargs):
    """Submit a parametric array job to SLURM with automatic resubmission"""
    parameter_grid = {}
    for param in kwargs.pop('parameter'):
        name, values = param.split(':', 1)
        if ',' in values:
            try:
                parameter_grid[name] = ast.literal_eval(values)
            except:
                parameter_grid[name] = values.split(',')
        else:
            parameter_grid[name] = [values]

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