import logging
import os
import pyarrow.parquet as pq

# Ensure the folder exists
os.makedirs('logs', exist_ok=True)

# Set up logging to file in the 'logs' folder
logging.basicConfig(filename='output/output_check.log', level=logging.INFO, format='%(asctime)s - %(message)s')

# Paths for the Parquet files
parquet_file_path_local_material_s1 = "output/local_material_data_s1.parquet"
parquet_file_path_local_material_s2 = "output/local_material_data_s2.parquet"
parquet_file_path_process_order_s1 = "output/process_order_data_s1.parquet"
parquet_file_path_process_order_s2 = "output/process_order_data_s2.parquet"

# Read the Parquet files using PyArrow
table_s1 = pq.read_table(parquet_file_path_local_material_s1)
table_s2 = pq.read_table(parquet_file_path_local_material_s2)
table_process_order_s1 = pq.read_table(parquet_file_path_process_order_s1)
table_process_order_s2 = pq.read_table(parquet_file_path_process_order_s2)

# Convert Arrow Tables to Pandas DataFrames
pandas_df_s1 = table_s1.to_pandas()
columns_s1 = table_s1.schema.names
types_s1 = {col: table_s1.column(col).type for col in columns_s1}

pandas_df_s2 = table_s2.to_pandas()
columns_s2 = table_s2.schema.names
types_s2 = {col: table_s2.column(col).type for col in columns_s2}

pandas_df_process_order_s1 = table_process_order_s1.to_pandas()
columns_process_order_s1 = table_process_order_s1.schema.names
types_process_order_s1 = {
    col: table_process_order_s1.column(col).type for col in columns_process_order_s1
}

pandas_df_process_order_s2 = table_process_order_s2.to_pandas()
columns_process_order_s2 = table_process_order_s2.schema.names
types_process_order_s2 = {
    col: table_process_order_s2.column(col).type for col in columns_process_order_s2
}

# Function to compare columns and data types
def compare_parquet_columns_and_types(
    columns_s1, types_s1, columns_s2, types_s2, dataset_name
):
    logging.info(f"Comparing columns and data types for {dataset_name}...")

    # Find common columns
    common_columns = set(columns_s1).intersection(set(columns_s2))
    missing_in_s1 = set(columns_s2) - set(columns_s1)
    missing_in_s2 = set(columns_s1) - set(columns_s2)

    logging.info(f"Common columns: {common_columns}")
    logging.info(
        f"Columns in {dataset_name} system 2 but missing in system 1: {missing_in_s1}"
    )
    logging.info(
        f"Columns in {dataset_name} system 1 but missing in system 2: {missing_in_s2}"
    )

    # Check if all common columns have the same type
    type_mismatch_found = False
    for column in common_columns:
        type_s1 = types_s1.get(column)
        type_s2 = types_s2.get(column)

        if type_s1 != type_s2:
            logging.info(
                f"Column '{column}' has different types: System 1 ({type_s1}) vs System 2 ({type_s2})"
            )
            type_mismatch_found = True

    # If no type mismatch was found, log a general message
    if not type_mismatch_found:
        logging.info("All columns in this dataset have the same type.")

    logging.info("\n" + "-" * 50 + "\n")


# Function to explore data in each dataset
def explore_data(pandas_df, dataset_name):
    logging.info(f"Exploring data for {dataset_name}...")
    logging.info(f"First 5 rows of the dataset {dataset_name}:")
    logging.info(f"{pandas_df.head()}")
    logging.info(f"\nSummary statistics for {dataset_name}:")
    logging.info(f"{pandas_df.describe()}")
    logging.info("\n" + "-" * 50 + "\n")


# Function to check for primary key uniqueness
def check_primary_key_uniqueness(pandas_df, primary_key_columns, dataset_name):
    logging.info(f"Checking primary key uniqueness for {dataset_name}...")
    for primary_key in primary_key_columns:
        if pandas_df[primary_key].duplicated().any():
            logging.warning(
                f"Primary key '{primary_key}' in {dataset_name} has duplicates."
            )
        else:
            logging.info(f"Primary key '{primary_key}' in {dataset_name} is unique.")
    logging.info("\n" + "-" * 50 + "\n")


# Function to check for missing values in dataset
def check_missing_values(pandas_df, dataset_name):
    logging.info(f"Checking for missing values in {dataset_name}...")
    missing_values = pandas_df.isnull().sum()
    if missing_values.any():
        logging.info(f"Missing values in {dataset_name}:\n{missing_values}")
    else:
        logging.info(f"No missing values in {dataset_name}.")
    logging.info("\n" + "-" * 50 + "\n")


# Function to check for empty columns
def check_empty_columns(pandas_df, dataset_name):
    logging.info(f"Checking for empty columns in {dataset_name}...")

    # Check for empty columns (all NaN)
    empty_columns = pandas_df.columns[pandas_df.isnull().all()]
    if empty_columns.any():
        logging.info(f"Empty columns in {dataset_name}:\n{empty_columns}")
    else:
        logging.info(f"No empty columns in {dataset_name}.")

    logging.info("\n" + "-" * 50 + "\n")


# Explore the data for both systems
explore_data(pandas_df_s1, "local_material_data system 1")
explore_data(pandas_df_s2, "local_material_data system 2")
explore_data(pandas_df_process_order_s1, "process_order_data system 1")
explore_data(pandas_df_process_order_s2, "process_order_data system 2")

# Check primary key uniqueness (assuming 'primary_key_intra' and 'primary_key_inter' are the primary keys)
check_primary_key_uniqueness(
    pandas_df_s1, ["primary_key_intra"], "local_material_data system 1"
)
check_primary_key_uniqueness(
    pandas_df_s2, ["primary_key_intra"], "local_material_data system 2"
)
check_primary_key_uniqueness(
    pandas_df_process_order_s1, ["primary_key_inter"], "process_order_data system 1"
)
check_primary_key_uniqueness(
    pandas_df_process_order_s2, ["primary_key_inter"], "process_order_data system 2"
)

# Check for missing values
check_missing_values(pandas_df_s1, "local_material_data system 1")
check_missing_values(pandas_df_s2, "local_material_data system 2")
check_missing_values(pandas_df_process_order_s1, "process_order_data system 1")
check_missing_values(pandas_df_process_order_s2, "process_order_data system 2")

# Check for empty columns
check_empty_columns(pandas_df_s1, "local_material_data system 1")
check_empty_columns(pandas_df_s2, "local_material_data system 2")
check_empty_columns(pandas_df_process_order_s1, "process_order_data system 1")
check_empty_columns(pandas_df_process_order_s2, "process_order_data system 2")

# Compare the two datasets
compare_parquet_columns_and_types(
    columns_s1, types_s1, columns_s2, types_s2, "local_material_data"
)
compare_parquet_columns_and_types(
    columns_process_order_s1,
    types_process_order_s1,
    columns_process_order_s2,
    types_process_order_s2,
    "process_order_data",
)
