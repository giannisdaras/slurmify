from setuptools import setup, find_packages

setup(
    name="slurmify",
    version="0.1.1",
    packages=find_packages(exclude=["tests*"]),
    install_requires=[
        "click",  # for CLI interface
    ],
    entry_points={
        "console_scripts": [
            "slurmify=slurmify.cli:main",
        ],
    },
    author="Giannis Daras",
    author_email="giannisdaras@utexas.edu",
    description="A utility library for SLURM job management",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/giannisdaras/slurmify",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.6",
)
