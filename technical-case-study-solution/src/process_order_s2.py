from pyspark.sql import functions as F
from pyspark.sql.window import Window

# 1. Prepare Order Header Data
def prep_sap_order_header_data(sap_afko):
    """ 
    1. Prepare Order Header Data
    
    Input: SAP AFKO table (Order Header Data)

    Fields:
    ○ SOURCE_SYSTEM_ERP: Source ERP system identifier
    ○ MANDT: Client
    ○ AUFNR: Order Number
    ○ Various date fields (e.g., GLTRP, GSTRP, FTRMS, GLTRS, GSTRS, GSTRI, GETRI, GLTRI, FTRMI, FTRMP)
    ○ Additional fields (e.g., DISPO, FEVOR, PLGRP, FHORI, AUFPL)

    Arguments:
    - sap_afko (df): df with order header data

    Returns:
    - df: processed data with selected columns
    """
    
    # Select the required columns.
    sap_afko_data = sap_afko.select(
        "SOURCE_SYSTEM_ERP",
        "MANDT",
        "AUFNR",
        "GLTRP", 
        "GSTRP", 
        "FTRMS", 
        "GLTRS", 
        "GSTRS", 
        "GSTRI", 
        "GETRI", 
        "GLTRI", 
        "FTRMI", 
        "FTRMP",
        "DISPO",
        "FEVOR", 
        "PLGRP",
        "FHORI",
        "AUFPL"
    )

    # Convert string date columns to date type for system 2
    date_columns = ["GLTRP", "GSTRP", "FTRMS", "GLTRS", "GSTRS", "GSTRI", "GETRI", "GLTRI", "FTRMI"]
    # other columns with different data type 
    # "TRMDT", "PDATV", "AUFLD", "SDATV", "GLTRI", "GETRI", "PLAUF"
    for column in date_columns:
        sap_afko_data = sap_afko_data.withColumn(column, F.to_date(F.col(column), "yyyy-MM-dd").cast("date"))

    # Create start_date and finish_date
    # ▪ Format GSTRP as 'yyyy-MM'.
    sap_afko_data = sap_afko_data.withColumn(
        "start_date",
        F.when(
            F.col("GSTRP").isNull(),
            F.date_format(F.current_date(), "yyyy-MM")
        ).otherwise(
            F.date_format(F.col("GSTRP"), "yyyy-MM")
        )
    )

    # ▪ Concatenate '-01' to form full dates.
    sap_afko_data = sap_afko_data.withColumn(
        "start_date",
        F.concat_ws("-", F.col("start_date"), F.lit("01"))
    ).withColumn(
        "start_date",
        F.to_date(F.col("start_date"), "yyyy-MM-dd")
    )

    # Create finish_date
    sap_afko_data = sap_afko_data.withColumn(
        "finish_date",
        F.when(
            F.col("GSTRS").isNull(),
            F.date_format(F.current_date(), "yyyy-MM")
        ).otherwise(
            F.date_format(F.col("GSTRS"), "yyyy-MM")
        )
    )

    # ▪ Concatenate '-01' to form full dates.
    sap_afko_data = sap_afko_data.withColumn(
        "finish_date",
        F.concat_ws("-", F.col("finish_date"), F.lit("01"))
    ).withColumn(
        "finish_date",
        F.to_date(F.col("finish_date"), "yyyy-MM-dd")
    )

    return sap_afko_data


def prep_sap_order_item(sap_afpo):
    """
    2. Prepare Order Item Data

    Input: prep_sap_order_item

    Fields:
    ○ AUFNR: Order Number
    ○ POSNR: Order Item Number
    ○ DWERK: Plant
    ○ MATNR: Material Number
    ○ Additional fields (e.g., MEINS, KDAUF, KDPOS, etc.)
    Arguments:
    - sap_afpo (df): df with order item data

    Returns:
    - df: processed data with selected columns
    """
    # Convert some other initial string date columns to date type for system 2
    #date_columns = ["KSBIS", "LTRMP", "DGLTS", "GSBTR", "ETRMP", "DGLTP","STRMP","KSVON"]
    #for column in date_columns:
    #    sap_afpo = sap_afpo.withColumn(
    #        column, 
    #        F.when(F.col(column).isNotNull(), F.to_date(F.col(column), "yyyy-MM-dd")).otherwise(None)
    #    )

    sap_afpo_data = sap_afpo.select(
        "AUFNR",
        "POSNR",
        "DWERK",
        "MATNR",
        "MEINS",
        "KDAUF",
        "KDPOS",
        "LTRMI"
    )
    sap_afpo_data = sap_afpo_data.dropDuplicates(["AUFNR","POSNR"])

    return sap_afpo_data


def prep_sap_order_master_data(sap_aufk):
    """
    # 3. Prepare Order Master Data

    Input: SAP AUFK table (Order Master Data)

    Fields:
    ○ AUFNR: Order Number
    ○ OBJNR: Object Number
    ○ ERDAT: Creation Date
    ○ ERNAM: Created By
    ○ AUART: Order Type
    ○ ZZGLTRP_ORIG: Original Basic Finish Date
    ○ ZZPRO_TEXT: Project Text

    Arguments: sap_aufk

    Returns: df

    """
    # Convert some other initial string date columns to date type for system 2
    #date_columns = ["ZZGLTRP_ORIG", "IDAT1", "IDAT2", "AEDAT"]
    #for column in date_columns:
    #    sap_aufk = sap_aufk.withColumn(
    #        column, 
    #        F.when(F.col(column).isNotNull(), F.to_date(F.col(column), "yyyy-MM-dd")).otherwise(None)
    #    )


    sap_aufk_data = sap_aufk.select(
        "AUFNR",
        "OBJNR",
        "ERDAT",
        "ERNAM",
        "AUART",
        "ZZGLTRP_ORIG",
        "ZZPRO_TEXT"
    )

    # Convert ZZGLTRP_ORIG from string to date
    sap_aufk_data = sap_aufk_data.withColumn(
        "ZZGLTRP_ORIG", F.to_date(F.col("ZZGLTRP_ORIG"), "yyyy-MM-dd").cast("date")
    )

    return sap_aufk_data

def prep_sap_general_material_data(sap_mara, col_mara_global_material_number):
    """
    # Prepare General Material Data

    Input: SAP MARA table (General Material Data)
    
    Fields:
    ○ MATNR: Material Number
    ○ Global Material Number: Column may vary between systems
    ○ NTGEW: Net Weight
    ○ MTART: Material Type

    Arguments: sap_mara

    Returns: df

    """
    # Convert some other initial string date columns to date type for system 2
    #date_columns = ["ZZLAUNCHD", "MSTDE", "MSTDV", "LAEDA", "ERSDA"]
    #for column in date_columns:
    #    sap_mara = sap_mara.withColumn(
    #        column, 
    #        F.when(F.col(column).isNotNull(), F.to_date(F.col(column), "yyyy-MM-dd")).otherwise(None)
    #    )

    sap_mara_data = sap_mara.filter(
        (
            (~F.col("BISMT").isin(["ARCHIVE", "DUPLICATE", "RENUMBERED"])) | 
            F.col("BISMT").isNull()
        ) &    
            F.col("LVORM").isNull()        
    ).select(
        "MATNR",
        F.col(col_mara_global_material_number).alias("global_material_number"),
        "NTGEW",
        "MTART" 
    )

    return sap_mara_data

# 6. Integrate Data
def integration_process_order(sap_afko, sap_afpo, sap_aufk, sap_mara, sap_cdpos=None):
    """
    6. Integrate Data

    Arguments:
    DataFrames to Integrate:
    ○ Order Header Data (sap_afko)
    ○ Order Item Data (sap_afpo)
    ○ Order Master Data (sap_aufk)
    ○ General Material Data (sap_mara)

    Returns:
    df with integrated data
    """
    df_join = sap_afko.join(sap_afpo, "AUFNR", "left")\
                    .join(sap_aufk, "AUFNR", "left")\
                    .join(sap_mara, "MATNR", "left")
                    
    if sap_cdpos:
        df_join = df_join.join(sap_cdpos, "OBJNR", "left")

    df_join = df_join.withColumn(
        "finish_date",
        F.coalesce(F.col("ZZGLTRP_ORIG"), F.col("GLTRP"))
    )

    return df_join


def post_prep_process_order(df):
    """
    7. Post-Process Process Order Data
    Input: Resulting DataFrame from the integration step.
    """

    # Convert all relevant string date columns to date type
    date_columns = ["ZZGLTRP_ORIG", "GLTRP", "GSTRP", "FTRMS", "GLTRS", "GSTRS", "GSTRI", "GETRI", "GLTRI", "FTRMI"]
    for column in date_columns:
        df = df.withColumn(column, F.to_date(F.col(column), "yyyy-MM-dd").cast("date"))

    # Intra Primary Key (primary_key_intra): Concatenate AUFNR, POSNR, and DWERK.
    df = df.withColumn("primary_key_intra", F.concat_ws("-","AUFNR","POSNR","DWERK"))
    
    # Inter Primary Key (primary_key_inter): Concatenate SOURCE_SYSTEM_ERP, AUFNR, POSNR, and DWERK.
    df = df.withColumn("primary_key_inter", F.concat_ws("-","SOURCE_SYSTEM_ERP","AUFNR","POSNR", "DWERK"))

    # Calculate On-Time Flag and other columns
    df = df.withColumn(
        "on_time_flag",
        F.when(
            F.col("ZZGLTRP_ORIG") >= F.col("LTRMI"), 1 # should be LTRMI
        ).when(
            F.col("ZZGLTRP_ORIG") < F.col("LTRMI"),0 # should be LTRMI
        ).otherwise(None)
    )

    # ▪ Compute actual_on_time_deviation as ZZGLTRP_ORIG - LTRMI.
    df = df.withColumn("actual_on_time_deviation", F.datediff(F.col("ZZGLTRP_ORIG"), F.col("LTRMI")))

    # ▪ Categorize late_delivery_bucket based on deviation days.
    df = df.withColumn(
        "late_delivery_bucket",
        F.when(F.col("actual_on_time_deviation") < 0, "EARLY DELIVERY")
        .when(F.col("actual_on_time_deviation") == 0, "ON-TIME DELIVERY")
        .when(F.col("actual_on_time_deviation") > 0, "LATE DELIVERY")
        .otherwise("UNKNOWN")
    )

    # ○ Ensure ZZGLTRP_ORIG is Present:
    # ▪ Add ZZGLTRP_ORIG with null values if it's not in the DataFrame.
    df = df.withColumn(
        "ZZGLTRP_ORIG",
        F.coalesce(F.col("ZZGLTRP_ORIG"), F.lit(None))
    )

    # Derive MTO vs. MTS Flag:
    # ▪ Set mto_vs_mts_flag to:
    # ● "MTO" if KDAUF (Sales Order Number) is not null.
    # ● "MTS" otherwise.
    df = df.withColumn(
        "mto_vs_mts_flag",
        F.when(F.col("KDAUF").isNotNull(), "MTO").otherwise("MTS")
    )

    # ○ Convert Dates to Timestamps:
    # ▪ Create order_finish_timestamp from LTRMI.

    df = df.withColumn(
        "order_finish_timestamp",
        F.to_timestamp(F.col("LTRMI")) # should be LTRMI
    )

    # ▪ Create order_start_timestamp from GSTRI.
    df = df.withColumn(
        "order_start_timestamp",
        F.to_timestamp(F.col("GSTRI"))
    )

    return df


def process_order(df, output_path):
    """ 
    8. Write the Output DataFrame
    """
    df.write.mode("overwrite").parquet(output_path)
    print(f"Data were written to {output_path}")
