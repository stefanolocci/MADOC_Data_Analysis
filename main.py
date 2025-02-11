import pandas as pd
from datasets import load_dataset
from tqdm import tqdm
import os

# Load the dataset from the local parquet file with streaming enabled
dataset = load_dataset('parquet', data_files='dataset/reddit_fatpeoplehate_madoc.parquet', streaming=False)

# Access the dataset (assuming no split)
reddit_data = dataset['train']

# Initialize dictionaries to collect data for each interaction type
interaction_data = {}

# Target user_id
target_user_id = '1bdbc0a9-0e79-5841-985d-dd602228787c'

mock_filename = f"{target_user_id}_COMMENT.csv"

if not os.path.exists(mock_filename):
    # Stream through the dataset and collect data by interaction type
    for row in tqdm(reddit_data):
        if row['user_id'] == target_user_id:
            interaction_type = row['interaction_type']
            if interaction_type not in interaction_data:
                interaction_data[interaction_type] = []
            interaction_data[interaction_type].append(row)

    # Create DataFrames for each interaction type and store them in a dictionary
    dfs = {interaction_type: pd.DataFrame(rows) for interaction_type, rows in interaction_data.items()}

    for k, df in dfs.items():
        df.to_csv(f"{target_user_id}_{k}.csv")

dfs = dict()
ds = [d for d in os.listdir() if target_user_id in d]
for d in ds:
    df = pd.read_csv(d)
    dfs[d] = df
    print(df.to_string())

# Display a summary of the DataFrames created
dfs_summary = {interaction_type: df.shape for interaction_type, df in dfs.items()}
print(dfs_summary)
