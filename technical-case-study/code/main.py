from pyspark.sql import SparkSession

"""
Exercise Overview:
You will work with data from two SAP systems that have similar data sources. 
Your task is to process and integrate this data to provide a unified view for supply chain insights. 

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
        .appName("technical-case-study") \
        .getOrCreate()

    df = spark.read.csv(f"{file_directory}", header=True, inferSchema=True)
    
    
    return df

# Example usage
file = 'data/system_1/PRE_AFKO.csv'
pre_afko = read_csv_file(file)

# Structure your code project
# Work with pyspark

# Show the DataFrame
pre_afko.show()