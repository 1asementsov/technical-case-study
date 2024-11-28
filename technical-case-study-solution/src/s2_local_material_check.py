from pyspark.sql import functions as F
from pyspark.sql.window import Window
import pyspark
from pyspark.sql import SparkSession


def read_csv_file(file_directory: str):
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("technical-case-study-solution") \
        .getOrCreate()
        #.master("local[*]") \
        #.config("spark.hadoop.security.authentication", "simple") \
        #.config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem") \
        #.config("spark.ui.showConsoleProgress", "true") \

    df = spark.read.csv(f"{file_directory}", header=True, inferSchema=True)   
    return df

def prep_general_material_data(sap_mara, col_mara_global_material_number):
    """
    1. Prepare General Material Data
    
    Input: SAP MARA table (General Material Data)

    Fields:
    ○ MANDT: Client
    ○ MATNR: Material Number
    ○ MEINS: Base Unit of Measure
    ○ Global Material Number: Column name may vary between systems (e.g., ZZMDGM, ZZA_TEXT4)

    Parameters:
    ○ col_mara_global_material_number: Specify the global material number column name for each system.
    ○ check_old_material_number_is_valid (bool): Set to True to filter out invalid old material numbers.
    ○ check_material_is_not_deleted (bool): Set to True to exclude materials flagged for deletion.

    Filters:
    ▪ Old Material Number (BISMT) is not in ["ARCHIVE", "DUPLICATE", "RENUMBERED"] or is null.
    ▪ Deletion flag (LVORM) is null or empty.

    Arguments:
    - sap_mara (df): df containing material data from SAP MARA.
    - col_mara_global_material_number (str): The column name for the global material number in the SAP system.

    Returns:
    - df: Processed general material data with the selected columns.
    """
    
    data = sap_mara.filter(
        (
            # Old Material Number (BISMT) is not in ["ARCHIVE", "DUPLICATE", "RENUMBERED"] or is null.
            (~F.col("BISMT").isin(["ARCHIVE", "DUPLICATE", "RENUMBERED"])) | 
            F.col("BISMT").isNull()
        ) &    
            # Deletion flag (LVORM) is null or empty
            (F.col("LVORM").isNull() | (F.trim(F.col("LVORM")) == ""))
    ).select(
        "MANDT",  # Client
        "MATNR",  # Material Number
        "MEINS",  # Base Unit of Measure
        # Rename the global material number column to a consistent name
        F.col(col_mara_global_material_number).alias("global_material_number")
    )
    print(sap_mara.columns)
    return data

system = "system_2"
file_mara = '../data/system_1/PRE_MARA.csv'
sap_mara = read_csv_file(file_mara)
col_mara_global_material_number = 'ZZMDGM'  # This can vary by system
data = prep_general_material_data(sap_mara, col_mara_global_material_number)