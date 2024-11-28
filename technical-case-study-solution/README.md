# SAP Data Modeling and PySpark Coding

## Description

In this exercise, you will practice **data modeling** and **coding in PySpark** to build data products that provide insights into our supply chain.


### Exercise Overview
You will work with data from two SAP systems that have similar data sources. Your task is to implement and integrate this data to provide a unified view for supply chain insights.

The exercise involves:

- **Processing Local Material** data.
- **Processing Process Order** data.
- Ensuring both datasets have the **same schema** for **harmonization** across systems.
- Writing **modular, reusable code** with proper documentation.
- Following **best-in-class principles** for flexibility and maintainability.

**Technical-Case-Study.pdf** explains everything in detail about what you need to do.

---

## Project Structure

In the `data/` folder, you will find CSV files for two SAP systems (System 1 and System 2) that need to be used and transformed.

In the `src/` folder, you will find the solution to this project:

1. **`input_check.py`**: This file is used to do the initial check on the data and examine what’s there.
2. **`local_material_s1.py`**: Used to process local material data for **System 1**.
3. **`local_material_s2.py`**: Used to process local material data for **System 2**.
4. **`process_order_s1.py`**: Used to process order data for **System 1**.
5. **`process_order_s2.py`**: Used to process order data for **System 2**.
6. **`main.py`**: This is the main script that runs both `local_material.py` and `process_order.py`.
7. **`output_check.py`**: This script checks the final output files (Parquet files for each system).

Additionally, the **`technical-case-study.drawio`** file provides the data modeling visualization.

---

## Setup Instructions

### 1. Create the `README.md` File
```bash
New-Item README.md -ItemType File
```

### Go to src folder
```bash
cd src
```

### Let's create four .py files for each system (system 1 and system 2)
```bash
New-Item local_material_s1.py -ItemType File
New-Item local_material_s2.py -ItemType File
New-Item process_order_s1.py -ItemType File
New-Item process_order_s2.py -ItemType File
```

### (OPTIONAL) Adjust the execution policy to allow scripts to run (in case if you get an error)
```bash
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
```

### (OPTIONAL) Reset policy to default if needed
```bash
Set-ExecutionPolicy -ExecutionPolicy Restricted -Scope CurrentUser
```

### Activate virtual environment
```bash
.\venv\Scripts\activate
```

### install requirements.txt
```bash
pip install -r requirements.txt
```

### (OPTIONAL) upgrade pip install if needed; check your path
```bash
C:\Users\<username>\Downloads\technical-case-study\technical-case-study-solution\venv\Scripts\python.exe -m pip install --upgrade pip
```

### install winutils; be ready to install it locally depending on the system 
```bash
git clone https://github.com/steveloughran/winutils.git
```

### (OPTIONAL) run input_check.py to check initial data and get idead on what's there
```bash
python input_check.py
```

### run main.py to process data from local material and order data
```bash
python main.py 
```

### run output_check.py to check output dataset 
```bash
python output_check.py
```

### Other 
### We can use libraries like flake8 and black for code linting (checking code for errors, style violations, and formatiing issues) .flake8 file for customizations like max-line-length = 120
```bash
flake8 input_check.py
flake8 main.py
flake8 output_check.py
flake8 local_material_s1.py
flake8 local_material_s2.py
flake8 process_order_s1.py
flake8 process_order_s2.py
```

### use black to automatically fix issues
```bash
black input_check.py
black main.py
black output_check.py
black local_material_s1.py
black local_material_s2.py
black process_order_s1.py
black process_order_s2.py
```
### (OPTIONAL) automatically fix issues whenever commit code every time when commiting flake8 and black will automatically run and fix issues before they’re committed to a repository.

### Set up a .pre-commit-config.yaml file in our project then install and run pre-commit
```bash
pip install pre-commit
pre-commit install
pre-commit run --all-files
```