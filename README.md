# SprkCalo: spark-based analysis toolkit for ATLAS calorimeter data
[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-4.0+-E25A1C?logo=apachespark&logoColor=fff)](#)
[![NumPy](https://img.shields.io/badge/NumPy-2.0.+-4DABCF?logo=numpy&logoColor=fff)](#)
[![Pandas](https://img.shields.io/badge/Pandas-2.3.+-150458?logo=pandas&logoColor=fff)](#)
[![GitHub](https://img.shields.io/badge/github-repo-blue?logo=github&color=blue)](https://github.com/MarcoValente/sprkcalo)
[![Contributions Welcome](https://img.shields.io/badge/contributions-welcome-lightblue.svg)]()
[![Status: Active](https://img.shields.io/badge/status-active-brightgreen.svg)]()

**Sprkcalo** is an Apache Spark based analysis toolkit, designed for efficient data analysis of ATLAS calorimeter data. It provides scalable data processing and analysis capabilities tailored for high-energy physics workflows.

## Command Line Interface

The main entry point is the CLI tool `sprkcalo`.  
Example usage:

```bash
sprkcalo -c config/analysis.yaml --inputs input_file.parquet -o output_dir show
```
For more options, see `sprkcalo --help`.
```bash
Usage: sprkcalo [OPTIONS] COMMAND [ARGS]...

Options:
  -c, --mainConfig PATH           YAML file containing main configuration
  -o, --output_dir PATH           Output directory
  -f, --force                     Force overwrite of output directory if it
                                  exists
  -N, --names TEXT                Names for input files
  -i, --inputs PATH               Input parquet files
  -l, --output_level [DEBUG|INFO|WARNING|ERROR|CRITICAL]
                                  Logging output level
  -n, --nevents INTEGER           Number of events to process
  --inputsToMatch PATH            Input parquet files to use for matching the
                                  main ones
  --help                          Show this message and exit.

Commands:
  hist-dump
  show 
```

## Installation

1. Clone the repository:
    ```bash
    git clone https://github.com/MarcoValente/sprkcalo.git
    ```
2. Setup python virtual environment:
    ```bash
    python -m venv venv
    ```
3. Activate virtual environment and install dependencies:
    ```bash
    source venv/bin/activate && pip install -r requirements.txt .
    ```
4. Run your first analysis:
    ```bash
    sprkcalo --help
    ```