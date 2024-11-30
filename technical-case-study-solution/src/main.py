import pyspark
from pyspark.sql import SparkSession

from local_material_s1 import (
    prep_general_material_data,
    prep_material_valuation,
    prep_plant_data_for_material,
    prep_plant_and_branches,
    prep_valuation_area,
    prep_company_codes,
    integration_local_material,
    post_prep_local_material,
    write_output,
    write_outputcsv
)

from local_material_s2 import (
    prep_general_material_data as prep_general_material_data_s2,
    prep_material_valuation as prep_material_valuation_s2,
    prep_plant_data_for_material as prep_plant_data_for_material_s2,
    prep_plant_and_branches as prep_plant_and_branches_s2,
    prep_valuation_area as prep_valuation_area_s2,
    prep_company_codes as prep_company_codes_s2,
    integration_local_material as integration_local_material_s2,
    post_prep_local_material as post_prep_local_material_s2,
    write_output as write_output_s2
)

from process_order_s1 import (
    prep_sap_order_header_data,
    prep_sap_order_item,
    prep_sap_order_master_data,
    prep_sap_general_material_data,
    integration_process_order,
    post_prep_process_order,
    process_order
)

from process_order_s2 import (
    prep_sap_order_header_data as prep_sap_order_header_data_s2,
    prep_sap_order_item as prep_sap_order_item_s2,
    prep_sap_order_master_data as prep_sap_order_master_data_s2,
    prep_sap_general_material_data as prep_sap_general_material_data_s2,
    integration_process_order as integration_process_order_s2,
    post_prep_process_order as post_prep_process_order_s2,
    process_order as process_order_s2
)

"""
Exercise Overview:
You will work with data from two SAP systems that have similar data sources.
Your task is to process and integrate this data to provide a unified view for
supply chain insights.

The exercise involves:
* Processing Local Material data.Processing Process Order data.
* Ensuring both datasets have the same schema for harmonization across systems.
* Writing modular, reusable code with proper documentation.
* Following best-in-class principles for flexibility and maintainability.

Note: You will create two Python scripts (local_material.py and process_order.py)
for each system, i.e. in total of four scripts (2 systems per modeled entity/event).

General Instructions
Work on both SAP systems:
* Perform all the steps for both systems to ensure consistency
* Enable and accomplish data harmonization through a common data model

Focus on data fields and transformations:
* Pay attention to the required fields and the transformations applied to them.
* Document your code: Include comments explaining why certain modules and functions are used.
* Follow best practices: Write modular code, handle exceptions, and ensure reusability.

Detailed instructions see attached PDF

"""


def read_csv_file(file_directory: str):
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("technical-case-study-solution") \
        .getOrCreate()

    df = spark.read.csv(f"{file_directory}", header=True, inferSchema=True)
    return df


def process_local_material_data(system):
    """
    Process local material data for a specific system.
    Arguments: system (str) - The system (system 1 or system 2)
    """
    # Paths
    # System 1
    if system == "system_1":
        file_mara = 'data/system_1/PRE_MARA.csv'
        file_mbew = 'data/system_1/PRE_MBEW.csv'
        file_marc = 'data/system_1/PRE_MARC.csv'
        file_t001k = 'data/system_1/PRE_T001K.csv'
        file_t001w = 'data/system_1/PRE_T001W.csv'
        file_t001 = 'data/system_1/PRE_T001.csv'

    # System 2
    elif system == "system_2":
        file_mara = 'data/system_2/PRD_MARA.csv'
        file_mbew = 'data/system_2/PRD_MBEW.csv'
        file_marc = 'data/system_2/PRD_MARC.csv'
        file_t001k = 'data/system_2/PRD_T001K.csv'
        file_t001w = 'data/system_2/PRD_T001W.csv'
        file_t001 = 'data/system_2/PRD_T001.csv'

    # Read CSV
    sap_mara = read_csv_file(file_mara)
    sap_mbew = read_csv_file(file_mbew)
    sap_marc = read_csv_file(file_marc)
    sap_t001k = read_csv_file(file_t001k)
    sap_t001w = read_csv_file(file_t001w)
    sap_t001 = read_csv_file(file_t001)

    if system == "system_1":
        # Pre-process the data using functions from local_material_s1.py
        col_mara_global_material_number = 'ZZMDGM'  # This can vary by system
        sap_mara = prep_general_material_data(sap_mara, col_mara_global_material_number)
        sap_mbew = prep_material_valuation(sap_mbew)
        sap_marc = prep_plant_data_for_material(sap_marc)
        sap_t001w = prep_plant_and_branches(sap_t001w)
        sap_t001k = prep_valuation_area(sap_t001k)
        sap_t001 = prep_company_codes(sap_t001)

        # Integrate data
        integrated_data = integration_local_material(sap_mara, sap_mbew, sap_marc, sap_t001k, sap_t001w, sap_t001)
        # Post-process data
        processed_data = post_prep_local_material(integrated_data)
        # Specify the output path where the data will be saved
        output_path = "output/local_material_data_s1.parquet"
        output_path_csv = "output/local_material_data_s1.csv"
        write_output(processed_data, output_path)
        write_outputcsv(processed_data, output_path_csv)

    elif system == "system_2":
        # Pre-process the data using functions from local_material_s2.py
        col_mara_global_material_number = 'ZZMDGM'  # This can vary by system  no ZZA_TEXT4 in system 1 or system 2
        sap_mara = prep_general_material_data_s2(sap_mara, col_mara_global_material_number)
        sap_mbew = prep_material_valuation_s2(sap_mbew)
        sap_marc = prep_plant_data_for_material_s2(sap_marc)
        sap_t001w = prep_plant_and_branches_s2(sap_t001w)
        sap_t001k = prep_valuation_area_s2(sap_t001k)
        sap_t001 = prep_company_codes_s2(sap_t001)

        # Integrate data
        integrated_data = integration_local_material_s2(sap_mara, sap_mbew, sap_marc, sap_t001k, sap_t001w, sap_t001)
        # Post-process data
        processed_data = post_prep_local_material_s2(integrated_data)
        # Specify the output path where the data will be saved
        output_path = "output/local_material_data_s2.parquet"
        write_output_s2(processed_data, output_path)


def process_order_data(system):
    """
    Process order data for system 1 or system 2
    Arguments: system (str) - The system (system 1 or system 2)
    """
    # Paths
    # System 1
    if system == "system_1":
        file_afko = 'data/system_1/PRE_AFKO.csv'
        file_afpo = 'data/system_1/PRE_AFPO.csv'
        file_aufk = 'data/system_1/PRE_AUFK.csv'
        file_mara = 'data/system_1/PRE_MARA.csv'
    elif system == "system_2":
        file_afko = 'data/system_2/PRD_AFKO.csv'
        file_afpo = 'data/system_2/PRD_AFPO.csv'
        file_aufk = 'data/system_2/PRD_AUFK.csv'
        file_mara = 'data/system_2/PRD_MARA.csv'

    # Read CSV
    sap_afko = read_csv_file(file_afko)
    sap_afpo = read_csv_file(file_afpo)
    sap_aufk = read_csv_file(file_aufk)
    sap_mara = read_csv_file(file_mara)

    # Process System 1
    if system == "system_1":
        sap_afko = prep_sap_order_header_data(sap_afko)
        sap_afpo = prep_sap_order_item(sap_afpo)
        sap_aufk = prep_sap_order_master_data(sap_aufk)
        col_mara_global_material_number = 'ZZMDGM'
        sap_mara = prep_sap_general_material_data(sap_mara, col_mara_global_material_number)
        # Integrate data
        integrated_data = integration_process_order(sap_afko, sap_afpo, sap_aufk, sap_mara)
        # Post-process data
        processed_data = post_prep_process_order(integrated_data)
        output_path = "output/process_order_data_s1.parquet"
        process_order(processed_data, output_path)

    # System 2
    elif system == "system_2":
        sap_afko = prep_sap_order_header_data_s2(sap_afko)
        sap_afpo = prep_sap_order_item_s2(sap_afpo)
        sap_aufk = prep_sap_order_master_data_s2(sap_aufk)
        col_mara_global_material_number = 'ZZMDGM'
        sap_mara = prep_sap_general_material_data_s2(sap_mara, col_mara_global_material_number)
        # Integrate data
        integrated_data = integration_process_order_s2(sap_afko, sap_afpo, sap_aufk, sap_mara)
        # Post-process data
        processed_data = post_prep_process_order_s2(integrated_data)
        # Specify the output path where the data will be saved
        output_path = "output/process_order_data_s2.parquet"
        process_order_s2(processed_data, output_path)


def main():
    """
    Run the process_local_material_data function to start the data processing
    """
    # process_local_material_data("system_1")
    # process_local_material_data("system_2")
    process_order_data("system_1")
    process_order_data("system_2")


if __name__ == "__main__":
    main()
