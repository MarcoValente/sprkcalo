# FastCaloSim SparkAnalysis

![ATLAS Banner](https://atlas.cern/sites/atlas-public.web.cern.ch/files/atlas_banner.png)
![Spark Logo](https://upload.wikimedia.org/wikipedia/commons/f/ff/Spark-logo.png)

## Overview

**FastCaloSim SparkAnalysis** is a toolkit designed for efficient analysis of ATLAS FastCaloSim data using Apache Spark. It provides scalable data processing and analysis capabilities tailored for high-energy physics workflows.

## Features

- Distributed data analysis with Apache Spark
- FastCaloSim data ingestion and transformation
- Customizable selection and filtering
- Histogramming and summary statistics
- Output to common formats (ROOT, Parquet, CSV)

## Command Line Interface

The main entry point is the CLI tool `sprkana`.  
Example usage:

```bash
sprkana --input /path/to/data.root --output results.parquet --config analysis.yaml
```

### CLI Options

- `--input`: Path to input FastCaloSim file(s)
- `--output`: Output file path
- `--config`: YAML configuration for analysis parameters
- `--help`: Show all available options

## Getting Started

1. Clone the repository:
    ```bash
    git clone https://github.com/your-org/FastCaloSim-SparkAnalysis.git
    ```
2. Install dependencies:
    ```bash
    pip install -r requirements.txt
    ```
3. Run your first analysis:
    ```bash
    sprkana --input data.root --output results.parquet
    ```

## Documentation

See the [docs](docs/) folder for detailed usage and API reference.

## License

Distributed under the MIT License.

---

![CERN Logo](https://upload.wikimedia.org/wikipedia/commons/6/6a/CERN_logo.svg)