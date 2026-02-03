# Airflow Initialization Script

This repository contains a simple shell script to initialize an Apache Airflow environment.

## What it does
- Sets up required Airflow directories  
- Initializes the Airflow database  
- Prepares Airflow to run locally

## Usage
Make the script executable and run it:

```bash
chmod +x init_airflow.sh
./init_airflow.sh
```

## Requirements
    Bash
    Docker Compose

#### Note: Docker configuration files mounts the crediantials folder found at /home/ajay/.cred into airflow environment. This path needs to be set appropriately for your use case. Same goes for project_id and dataset names.
