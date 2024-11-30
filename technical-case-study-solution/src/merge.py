import pyarrow as pa
import pyarrow.parquet as pq
import os

# Path to the folder containing your Parquet files
input_folder = r"C:\Users\sementso\Downloads\technical-case-study\technical-case-study-solution\src\output\process_order_data_s1.parquet"
output_file = r"C:\Users\sementso\Downloads\merged_file.parquet"

# List all Parquet files in the folder
parquet_files = [os.path.join(input_folder, f) for f in os.listdir(input_folder) if f.endswith(".parquet")]

# Read and combine all Parquet files
tables = [pq.read_table(file) for file in parquet_files]
combined_table = pa.concat_tables(tables)

# Save as a single Parquet file
pq.write_table(combined_table, output_file)

print(f"Merged file saved to: {output_file}")