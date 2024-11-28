### Description

SAP Data Modeling and PySpark Coding
In this exercise, you will practice data modeling
and coding in PySpark to build data products that provide insights into our supply chain. 

Exercise Overview
You will work with data from two SAP systems that have similar data sources. Your task is to implement and integrate this data
to provide a unified view for supply chain insights. The exercise involves:
● Processing Local Material data.
● Processing Process Order data.
● Ensuring both datasets have the same schema for harmonization across systems.
● Writing modular, reusable code with proper documentation.
● Following best-in-class principles for flexibility and maintainability

Technical-Case-Study.pdf explains everything in detail what you need to do

in data/ folder there are csv files for two SAP systems (system 1 and system 2) that we need to use and transform
in src/ folder you can find all files as a solution to this project

1. input_check.py file is to do the initial check on data and see what's there
2. local_material_s1.py is used to process local material for system 1 
3. local_material_s2.py is used to process local material for system 2 
4. process_order_s1.py is used to process order data for system 1
5. process_order_s2.py is used to process order data for system 2
6. main.py is used to run local_material.py and process_order.py
7. output_check.py is used to check the final output files (parquet files for each system)

technical-case-study.drawio is used to show the data modeling

# Additional instructions on the solution and how to get started

# First, we need create new file README.md (this file)
New-Item README.md -ItemType File

# Go to src folder
cd src

# Let's create four .py files for each system (system 1 and system 2)
New-Item local_material_s1.py -ItemType File
New-Item local_material_s2.py -ItemType File
New-Item process_order_s1.py -ItemType File
New-Item process_order_s2.py -ItemType File

# (OPTIONAL) Adjust the execution policy to allow scripts to run (in case if you get an error)
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser

# (OPTIONAL) Reset policy to default if needed
Set-ExecutionPolicy -ExecutionPolicy Restricted -Scope CurrentUser

# Activate virtual environment
.\venv\Scripts\activate

# install requirements.txt
pip install -r requirements.txt

# (OPTIONAL) upgrade pip install if needed; check your path
C:\Users\<username>\Downloads\technical-case-study\technical-case-study-solution\venv\Scripts\python.exe -m pip install --upgrade pip

# install winutils; be ready to install it locally depending on the system 
git clone https://github.com/steveloughran/winutils.git

# (OPTIONAL) run input_check.py to check initial data and get idead on what's there
python input_check.py

# run main.py to process data from local material and order data
python main.py 

# run output_check.py to check output dataset 
python output_check.py

# Other 
# We can use flake8 for code linting (checking code for errors, style violations, and formatiing issues)
# .flake8 file for customizations like max-line-length = 120
flake8 input_check.py
flake8 main.py
flake8 output_check.py
flake8 local_material_s1.py
flake8 local_material_s2.py
flake8 process_order_s1.py
flake8 process_order_s2.py

# use black for fixing issues
black input_check.py
black main.py
black output_check.py
black local_material_s1.py
black local_material_s2.py
black process_order_s1.py
black process_order_s2.py

# (OPTIONAL) automatically fix issues whenever commit code
# every time when commiting flake8 and black will automatically run and fix issues before they’re committed to a repository.

# Set up a .pre-commit-config.yaml file in our project 
# then install and run pre-commit
pip install pre-commit
pre-commit install
pre-commit run --all-files