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