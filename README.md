# 🚀 slurmify: A Python Library to ease your SLURM Workflow! 🚀

Welcome to slurmify, a python library for managing SLURM jobs with style and efficiency! 🎉

## 🌟 Features

- 📊 Submit parametric array jobs with ease
- 🔄 Automatic job resubmission
- 📝 Simplified CLI for common SLURM tasks

## 🛠 Installation

```bash
pip install slurmify
```

## 🚀 Quick Start

Here's a taste of what SLURM Utils can do:

```bash
slurmify submit-parametric-array \
  --job-name awesome_experiment \
  --script-path examples/run_experiment.py \
  --time-limit 01:00:00 \
  --parameter "learning_rate:0.001,0.01,0.1" \
  --parameter "batch_size:32,64,128" \
  --partition "gpu" \
  --nodes=1
```

## 📚 How It Works

1. Create your Python script (`run_experiment.py`) with two essential functions:

   - `setup()`: Prepare your environment. This should be a function that returns a string with the setup commands.
   - `run()`: Define your experiment logic. This should be a function that returns a string with the command to run your experiment.

2. smurmify takes care of the rest! It creates a parametric array job, manages submissions, and handles resubmissions if needed.

## 🎭 Example Script

Here's a simple template for your `run_experiment.py`:

```python
import os

def setup():
    setup_cmd = """
    source ~/.bashrc
    conda activate myenv
    module load cuda/11.3
    """
    print(setup_cmd)
    return setup_cmd

def run():
    learning_rate = float(os.environ["LEARNING_RATE"])
    batch_size = int(os.environ["BATCH_SIZE"])
    
    cmd = f"python train.py --lr {learning_rate} --batch-size {batch_size}"
    print(cmd)
    return cmd

if __name__ == "__main__":
    run()
```

## 🎉 Happy SLURMing!

Now go forth and conquer those clusters! 🏆
