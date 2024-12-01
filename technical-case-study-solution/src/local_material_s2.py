from pyspark.sql import functions as F
from pyspark.sql.window import Window


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

    # Convert some other initial string date columns to date type for system 2
    # date_columns = ["ZZLAUNCHD", "MSTDE", "MSTDV", "LAEDA", "ERSDA"]
    # for column in date_columns:
    #    sap_mara = sap_mara.withColumn(
    #        column,
    #        F.when(F.col(column).isNotNull(), F.to_date(F.col(column), "yyyy-MM-dd")).otherwise(None)
    #    )

    data = sap_mara.filter(
        (
            # Old Material Number (BISMT) is not in ["ARCHIVE", "DUPLICATE", "RENUMBERED"] or is null.
            (~F.col("BISMT").isin(["ARCHIVE", "DUPLICATE", "RENUMBERED"]))
            | F.col("BISMT").isNull()
        )
        &
        # Deletion flag (LVORM) is null or empty
        (F.col("LVORM").isNull() | (F.trim(F.col("LVORM")) == ""))
    ).select(
        "MANDT",
        "MATNR",
        "MEINS",
        # Rename the global material number column to a consistent name
        F.col(col_mara_global_material_number).alias("global_material_number"),
    )

    return data


def prep_material_valuation(sap_mbew):
    """
    2. Prepare Material Valuation Data

    Input: SAP MBEW table (Material Valuation)

    Fields:
    ○ MANDT: Client
    ○ MATNR: Material Number
    ○ BWKEY: Valuation Area
    ○ VPRSV: Price Control Indicator
    ○ VERPR: Moving Average Price
    ○ STPRS: Standard Price
    ○ PEINH: Price Unit
    ○ BKLAS: Valuation Class

    Arguments:
    - sap_mbew (df) - df with material valuation from sap mbew

    Returns:
    - df with material valuation data
    """
    # Convert some other initial string date columns to date type for system 2
    # date_columns = ["ZPLD3", "ZPLD1"]
    # for column in date_columns:
    #    sap_mbew = sap_mbew.withColumn(
    #        column,
    #        F.when(F.col(column).isNotNull(), F.to_date(F.col(column), "yyyy-MM-dd")).otherwise(None)
    #    )

    # Filter out materials that are flagged for deletion (LVORM is null).
    data = sap_mbew.filter(F.col("LVORM").isNull())
    # Filter for entries with BWTAR (Valuation Type) as null to exclude split valuation materials.
    data = data.filter(F.col("BWTAR").isNull())

    # Deduplicate records:
    # ▪ Rule take the record having highest evaluated price LAEPR (Last Evaluated Price) at MATNR and BWKEY level
    window_spec = Window.partitionBy("MATNR", "BWKEY").orderBy(F.col("LAEPR").desc())

    data = sap_mbew.withColumn("row_number", F.row_number().over(window_spec))
    # Keep the first record per group
    data = (
        data.filter(F.col("row_number") == 1)
        .drop("row_number")
        .select("MANDT", "MATNR", "BWKEY", "VPRSV", "VERPR", "STPRS", "PEINH", "BKLAS")
    )
    return data


def prep_plant_data_for_material(sap_marc, drop_duplicate_records=True):
    """
    3. Prepare Plant Data for Material

    Input: SAP MARC table (Plant Data for Material)

    Fields:
    ○ SOURCE_SYSTEM_ERP: Source ERP system identifier
    ○ MATNR: Material Number
    ○ WERKS: Plant
    ○ Additional fields as required (e.g., PLIFZ, DZEIT, DISLS, etc.)

    Parameters:
    ○ check_deletion_flag_is_null (Boolean): Set to True to exclude records where LVORM is not null.
    ○ drop_duplicate_records (Boolean): Set to False unless deduplication is necessary

    Arguments:
    - sap_marc (df): df containing material data from SAP
    - drop_duplicate_records

    Returns:
    - df with plant data for material
    """
    # Filter records where the deletion flag (LVORM) is null.
    data = sap_marc.filter(F.col("LVORM").isNull())

    # Drop Duplicates if drop_duplicate_records is True.
    if drop_duplicate_records:
        duplicate_check_count = (
            data.groupBy("MATNR", "WERKS").count().filter(F.col("count") > 1)
        )

        # If there are duplicates, drop them and log a message (or handle as needed)
        if duplicate_check_count.count() > 0:
            print("Duplicates found and removed based on MATNR and WERKS.")
            data = data.dropDuplicates(["MATNR", "WERKS"])
        else:
            print("No duplicates found. Data will stay as is.")

    # ○ Select the required columns.
    return data.select("SOURCE_SYSTEM_ERP", "MATNR", "WERKS")


def prep_plant_and_branches(sap_t001w):
    """
    4. Prepare Plant and Branches Data

    Input: SAP T001W table (Plant/Branch)
    Fields:
    ○ MANDT: Client
    ○ WERKS: Plant
    ○ BWKEY: Valuation Area
    ○ NAME1: Name of Plant/Branch

    Arguments:
    - sap_t001w (df): df containing material data from SAP

    Returns:
    - df with plant and branches data
    """
    # ○ Select the required columns.
    data = sap_t001w.select(
        "MANDT",
        "WERKS",
        "BWKEY",
        "NAME1",
    )
    return data


def prep_valuation_area(sap_t001k, drop_duplicate_records=True):
    """
    5. Prepare Valuation Area Data

    Input: SAP T001K table (Valuation Area)

    Fields:
    ○ MANDT: Client
    ○ BWKEY: Valuation Area
    ○ BUKRS: Company Code

    Arguments:
    - sap_t001k (df): df containing data from SAP

    Returns:
    - df with valuation data
    """
    # Check if there are no null values in critical columns before processing
    data = sap_t001k.filter(
        F.col("MANDT").isNotNull()
        & F.col("BWKEY").isNotNull()
        & F.col("BUKRS").isNotNull()
    )

    # Drop Duplicates if drop_duplicate_records is True.
    if drop_duplicate_records:
        duplicate_check_count = (
            data.groupBy("BWKEY", "BUKRS", "MANDT").count().filter(F.col("count") > 1)
        )

        # If we get duplicates, let's drop them and print a message or just print a message
        if duplicate_check_count.count() > 0:
            print(
                "Duplicates found and removed based on BWKEY, BUKRS, MANDT in sap_t001k."
            )
            data = data.dropDuplicates(["BWKEY", "BUKRS", "MANDT"])
        else:
            print("No duplicates found in sap_t001k. Data will stay as is.")

    return data.select("MANDT", "BWKEY", "BUKRS")


def prep_company_codes(sap_t001):
    """
    6. Prepare Company Codes Data

    Input: SAP T001 table (Company Codes)

    Fields:
    ○ MANDT: Client
    ○ BUKRS: Company Code
    ○ WAERS: Currency Key

    Arguments:
    - sap_t001

    Returns:
    - df with company codes data
    """
    # ○ Select the required columns.
    data = sap_t001.select(["MANDT", "BUKRS", "WAERS"])
    return data


def integration_local_material(
    sap_mara, sap_mbew, sap_marc, sap_t001k, sap_t001w, sap_t001
):
    """
    7. Integrate Data

    Arguments:
    DataFrames to Integrate:
    ○ General Material Data (sap_mara)
    ○ Material Valuation Data (sap_mbew)
    ○ Plant Data for Material (sap_marc)
    ○ Valuation Area Data (sap_t001k)
    ○ Plant and Branches Data (sap_t001w)
    ○ Company Codes Data (sap_t001)

    Returns:
    df with integrated data

    """
    # ○ Join operations:
    df_join = (
        sap_marc.join(sap_mara, "MATNR", "left")
        .join(sap_t001w, ["MANDT", "WERKS"], "left")
        .join(sap_mbew, ["MANDT", "MATNR", "BWKEY"], "left")
        .join(sap_t001k, ["MANDT", "BWKEY"], "left")
        .join(sap_t001, ["MANDT", "BUKRS"], "left")
    )

    return df_join


def derive_intra_and_inter_primary_key(df):
    """
    Derives both intra and inter primary keys for the dataframe.

    Args:
    - df (DataFrame): The input dataframe containing the necessary columns.

    Returns:
    - df (DataFrame): The dataframe with the primary keys added.
    """
    # Deriving intra primary key by concatenating MATNR and WERKS
    df = df.withColumn("primary_key_intra", F.concat_ws("-", "MATNR", "WERKS"))

    # Deriving inter primary key by concatenating SOURCE_SYSTEM_ERP, MATNR, and WERKS
    df = df.withColumn(
        "primary_key_inter", F.concat_ws("-", "SOURCE_SYSTEM_ERP", "MATNR", "WERKS")
    )

    return df


def post_prep_local_material(df):
    """
    8. Post-Process Local Material Data

    Input: Resulting DataFrame from the integration step.

    Returns:
    df with post_processed local material data
    """

    # ○ Create mtl_plant_emd: Concatenate WERKS and NAME1 with a hyphen
    df = df.withColumn("mtl_plant_emd", F.concat_ws("-", "WERKS", "NAME1"))

    # ○  Assign global_mtl_id from MATNR or global material number, as appropriate
    df = df.withColumn("global_mtl_id", F.coalesce("MATNR", "global_material_number"))

    #  Use the derive_intra_and_inter_primary_key utility function to generate primary keys
    df = derive_intra_and_inter_primary_key(df)

    # ▪ Add a temporary column 'no_of_duplicates' to track duplicate counts
    window_spec = Window.partitionBy("SOURCE_SYSTEM_ERP", "MATNR", "WERKS")
    df = df.withColumn("no_of_duplicates", F.count("MATNR").over(window_spec))
    df = df.withColumn(
        "row_number",
        F.row_number().over(window_spec.orderBy(F.col("no_of_duplicates").desc())),
    )

    # ▪ Drop duplicates based on SOURCE_SYSTEM_ERP, MATNR, and WERKS; keep the first record
    df = df.filter(F.col("row_number") == 1).drop("row_number")

    return df


def write_output(df_join, output_path):
    """
    9. Write the Output DataFrame

    Arguments:
    df_join - df with processed data
    output_path - path to save the output

    Output:
    A df with the processed data
    """
    # ● Write the final local_material DataFrame to the designated output path.
    df_join.write.mode("overwrite").parquet(output_path)
    print(f"Data were written to {output_path}")