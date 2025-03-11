import pandas as pd
import os

def merge_parquet_files(input_folder, output_file):
    # Get all parquet files in the folder
    parquet_files = [os.path.join(input_folder, f) for f in os.listdir(input_folder) if f.endswith('.parquet')]
    print(parquet_files)
    # Read and combine all Parquet files
    data_frames = [pd.read_parquet(file) for file in parquet_files]
    combined_df = pd.concat(data_frames, ignore_index=True)
    
    # Write to a single output file
    combined_df.to_parquet(output_file, index=False)
    print(f"Data merged into {output_file}")

# Example usage
input_folder = "/media/zaman/Data Storage/anonymization/data_anonymization_new/csv_to_parquet_40/parquet_tables"
output_file = "/media/zaman/Data Storage/anonymization/data_anonymization_new/csv_to_parquet_40/parquet_tables/trans_hist.parquet"
merge_parquet_files(input_folder, output_file)
