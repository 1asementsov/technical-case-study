# SAP Data Modeling and PySpark Coding

## Description

In this exercise, we will practice **data modeling** and **coding in PySpark** to build data products that provide insights into our supply chain data.

### Exercise Overview

We will work with data from two SAP systems that have similar data sources. Our task is to implement and integrate this data to provide a unified view for supply chain insights.

The exercise involves:

- **Processing Local Material** data.
- **Processing Process Order** data.
- Ensuring both datasets have the **same schema** for **harmonization** across systems.
- Writing **modular, reusable code** with proper documentation.
- Following **best-in-class principles** for flexibility and maintainability.

**Technical-Case-Study.pdf** explains everything in detail about what we need to do the exercise.

**Technical Case Study Diagram** is an architecture diagram that shows how different SAP tables and datasets are connected in the solution.

![Technical Case Study](/technical-case-study.drawio.png)

---

## Project Structure

In the `data/` folder, you will find CSV files for two SAP systems (System 1 and System 2) that need to be used and transformed. Also, there is an `output/` folder which contains output results.

In the `src/` folder, you will find the solution to this project:

1. **`input_check.py`**: This file is used to do the initial check on the data and examine what’s there. You can run this optionally if there is a need.
2. **`local_material_s1.py`**: Used to process local material data for **System 1**.
3. **`local_material_s2.py`**: Used to process local material data for **System 2**.
4. **`process_order_s1.py`**: Used to process order data for **System 1**.
5. **`process_order_s2.py`**: Used to process order data for **System 2**.
6. **`main.py`**: This is the main script that runs both `local_material_s1.py`, `process_order_s1.py`, `local_material_s2.py`, and `process_order_s2.py`.
7. **`output_check.py`**: This script checks the final output files (it takes parquet files for each system as input and gives an output log with print messages).

Additionally, the **`technical-case-study.drawio.png`** file provides the data modeling visualization on how the initial files were transformed.

And **`tech_case_study.pbix`** file is an example of visualization on data completed in Power BI

### Output

As an output, there should be 4 folders (2 for each system) with parquet files inside. Some files were partitioned by default, though they can be merged if necessary.

(Optional) Also, you can run output_check.py to get `output_check.log` file to check both datasets have the **same schema** for **harmonization** across systems.

After we build parquet files, we can use any BI tool to visualize data and find insights in data.
Here are two examples how it might look like.

![Local Material Data](/Local_Material_Data.PNG)
---
![Process Order Data](/Process_Order_Data.PNG)

## Setup Instructions

This project processes data using Apache Spark. 
Docker and Docker Compose is used to package and run it on any machine.

### Running the Project Locally

#### Requirements:

- Docker
- Docker Compose

#### Steps to Run:

##### 1. Clone the repository:

```bash
git clone https://github.com/1asementsov/technical-case-study.git
cd technical-case-study
```

#### 2. Start the container and run Spark job
```bash
docker-compose up
```
##### (Optional): you can build container if needed
```bash
docker-compose build
```

#### 3. Stop and remove containers (if needed)
```bash
docker-compose down
```

Docker Compose simplifies running and managing containers. It allows to define multiple services, manage configurations, and execute commands like spark-submit in a unified manner.

### (**Optional**) Also, there is a way to run everything, but step by step.
#### 1. Pull image from Docker Hub
```bash
docker pull avsemddm/tech-case-study:latest
```

#### 2. You can build the docker image using this command
```bash
docker build -t tech-case-study .
```

#### 3. **On macOS/Linux**: 
Make the script executable and run it:

```bash
chmod +x run.sh
./run.sh
```

#### This script does the following:

It uses the pwd command to get the current working directory (i.e., the project directory where run.sh is located).

It dynamically sets the volume mappings to data and data/output within the project directory, which should work regardless of the operating system.


```bash
#!/bin/bash

# Get the absolute path of the current directory
PROJECT_DIR=$(pwd)

# Run Docker with the correct paths
docker run --rm \
  -v "$PROJECT_DIR/data:/src/raw_data" \
  -v "$PROJECT_DIR/data/output:/src/output" \
  tech-case-study \
  bash -c "spark-submit /src/main.py && spark-submit /src/output_check.py"
```

#### **Windows**: 

You can either use Git Bash (or a similar terminal) to run the run.sh script, or you can provide a Windows-specific batch file (run.bat). if you're using Git Bash, run the run.sh script

```bash
./run.sh
```

Or, use the run.bat script
```bash
run.bat
```

Here is a code
```bash
@echo off
set PROJECT_DIR=%cd%

docker run --rm ^
  -v "%PROJECT_DIR%/data:/src/raw_data" ^
  -v "%PROJECT_DIR%/data/output:/src/output" ^
  tech-case-study ^
  bash -c "spark-submit /src/main.py && spark-submit /src/output_check.py"
```
---
