import pyspark
from pyspark.sql import SparkSession


def read_csv_file(file_directory: str):
    # Initialize Spark session
    spark = SparkSession.builder.appName("technical-case-study-solution").getOrCreate()

    df = spark.read.csv(f"{file_directory}", header=True, inferSchema=True)
    return df


def get_column_statistics(df, column_names):
    """Calculate statistics for specific columns"""
    print(f"Statistics for columns {column_names}:")
    column_stats = df.describe(
        column_names
    ).show()  # This will give count, mean, stddev, min, and max
    return column_stats


def print_column_sample(df, column_names, num_rows=5):
    """Print a sample of the specified columns for the given number of rows"""
    print(f"Sample data for columns {column_names} (first {num_rows} rows):")
    sample_data = df.select(column_names).show(num_rows)
    return sample_data


def compare_columns(
    system_1_columns, system_1_types, system_2_columns, system_2_types, file_name
):
    """Compare columns and data types between two systems for the same file"""
    print(f"Comparing columns and data types for {file_name}...")

    # Find common columns
    common_columns = set(system_1_columns).intersection(set(system_2_columns))
    print(f"Common columns: {common_columns}")

    # Find columns in system 1 but not in system 2
    only_in_system_1 = set(system_1_columns) - set(system_2_columns)
    print(f"Columns only in system 1: {only_in_system_1}")

    # Find columns in system 2 but not in system 1
    only_in_system_2 = set(system_2_columns) - set(system_1_columns)
    print(f"Columns only in system 2: {only_in_system_2}")

    # Check if all common columns have the same type
    type_mismatch_found = False
    for column in common_columns:
        type_system_1 = system_1_types.get(column)
        type_system_2 = system_2_types.get(column)

        if type_system_1 != type_system_2:
            print(
                f"Column '{column}' has different types: System 1 ({type_system_1}) vs System 2 ({type_system_2})"
            )
            type_mismatch_found = True

    # If no type mismatch was found, print a general message
    if not type_mismatch_found:
        print("All columns in this dataset have the same type.")

    print("\n" + "-" * 50 + "\n")


def process_local_material_data(columns_to_check=None, num_rows=5):
    """
    Process local material data for system 1 and system 2 and compare columns and data types.
    Optionally, calculate statistics for specific columns and print sample data.
    """
    # Paths for system 1
    system_1_files = {
        "MARA": "../data/system_1/PRE_MARA.csv",
        "MBEW": "../data/system_1/PRE_MBEW.csv",
        "MARC": "../data/system_1/PRE_MARC.csv",
        "T001K": "../data/system_1/PRE_T001K.csv",
        "T001W": "../data/system_1/PRE_T001W.csv",
        "T001": "../data/system_1/PRE_T001.csv",
    }

    # Paths for system 2
    system_2_files = {
        "MARA": "../data/system_2/PRD_MARA.csv",
        "MBEW": "../data/system_2/PRD_MBEW.csv",
        "MARC": "../data/system_2/PRD_MARC.csv",
        "T001K": "../data/system_2/PRD_T001K.csv",
        "T001W": "../data/system_2/PRD_T001W.csv",
        "T001": "../data/system_2/PRD_T001.csv",
    }

    # Compare columns for each file
    for file_key in system_1_files:
        # Read CSV for system 1 and system 2
        system_1_df = read_csv_file(system_1_files[file_key])
        system_2_df = read_csv_file(system_2_files[file_key])

        # Extract columns and types
        system_1_columns = system_1_df.columns
        system_2_columns = system_2_df.columns
        system_1_types = dict(
            system_1_df.dtypes
        )  # Convert list of tuples (column_name, type) to a dictionary
        system_2_types = dict(
            system_2_df.dtypes
        )  # Convert list of tuples (column_name, type) to a dictionary

        # Compare columns and data types
        compare_columns(
            system_1_columns, system_1_types, system_2_columns, system_2_types, file_key
        )

        # If specific columns are provided, get statistics and print sample data for those columns
        if columns_to_check:
            # Check and calculate statistics for the specified columns in System 1
            system_1_columns_to_check = [
                col for col in columns_to_check if col in system_1_columns
            ]
            if system_1_columns_to_check:
                print(
                    f"Calculating statistics and printing sample data for columns {system_1_columns_to_check} in System 1, file: {file_key}"
                )
                get_column_statistics(system_1_df, system_1_columns_to_check)
                print_column_sample(system_1_df, system_1_columns_to_check, num_rows)

            # Check and calculate statistics for the specified columns in System 2
            system_2_columns_to_check = [
                col for col in columns_to_check if col in system_2_columns
            ]
            if system_2_columns_to_check:
                print(
                    f"Calculating statistics and printing sample data for columns {system_2_columns_to_check} in System 2, file: {file_key}"
                )
                get_column_statistics(system_2_df, system_2_columns_to_check)
                print_column_sample(system_2_df, system_2_columns_to_check, num_rows)


def process_order_data(columns_to_check=None, num_rows=5):
    """
    Process order-related data for system 1 and system 2 and compare columns and data types.
    Optionally, calculate statistics for specific columns and print sample data.
    """
    # Paths for system 1 (Order-related files)
    system_1_files = {
        "AFKO": "../data/system_1/PRE_AFKO.csv",
        "AFPO": "../data/system_1/PRE_AFPO.csv",
        "AUFK": "../data/system_1/PRE_AUFK.csv",
    }

    # Paths for system 2 (Order-related files)
    system_2_files = {
        "AFKO": "../data/system_2/PRD_AFKO.csv",
        "AFPO": "../data/system_2/PRD_AFPO.csv",
        "AUFK": "../data/system_2/PRD_AUFK.csv",
    }

    # Compare columns for each file
    for file_key in system_1_files:
        # Read CSV for system 1 and system 2
        system_1_df = read_csv_file(system_1_files[file_key])
        system_2_df = read_csv_file(system_2_files[file_key])

        # Extract columns and types
        system_1_columns = system_1_df.columns
        system_2_columns = system_2_df.columns
        system_1_types = dict(
            system_1_df.dtypes
        )  # Convert list of tuples (column_name, type) to a dictionary
        system_2_types = dict(
            system_2_df.dtypes
        )  # Convert list of tuples (column_name, type) to a dictionary

        # Compare columns and data types
        compare_columns(
            system_1_columns, system_1_types, system_2_columns, system_2_types, file_key
        )

        # If specific columns are provided, get statistics and print sample data for those columns
        if columns_to_check:
            # Check and calculate statistics for the specified columns in System 1
            system_1_columns_to_check = [
                col for col in columns_to_check if col in system_1_columns
            ]
            if system_1_columns_to_check:
                print(
                    f"Calculating statistics and printing sample data for columns {system_1_columns_to_check} in System 1, file: {file_key}"
                )
                get_column_statistics(system_1_df, system_1_columns_to_check)
                print_column_sample(system_1_df, system_1_columns_to_check, num_rows)

            # Check and calculate statistics for the specified columns in System 2
            system_2_columns_to_check = [
                col for col in columns_to_check if col in system_2_columns
            ]
            if system_2_columns_to_check:
                print(
                    f"Calculating statistics and printing sample data for columns {system_2_columns_to_check} in System 2, file: {file_key}"
                )
                get_column_statistics(system_2_df, system_2_columns_to_check)
                print_column_sample(system_2_df, system_2_columns_to_check, num_rows)


def main():
    # Specify the columns you want to check (e.g., "GSTRI", "MARC")
    columns_to_check = None  # ["GSTRI", "GETRI"]  # Add more column names as needed
    num_rows_to_print = 10  # Change this to 5, 10, etc. for the number of rows to print

    # Call the functions separately as needed
    process_local_material_data(
        columns_to_check, num_rows_to_print
    )  # Call for material data
    process_order_data(columns_to_check, num_rows_to_print)  # Call for order data


if __name__ == "__main__":
    main()
