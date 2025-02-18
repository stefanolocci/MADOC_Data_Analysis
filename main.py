# import pandas as pd
from datasets import load_dataset
from tqdm import tqdm
import os, sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from MADOC_Data_Analysis.utils import download_madoc_dataset
from MADOC_Data_Analysis.paths import DATADIR

filename = "reddit_gaming_madoc.parquet"
download_madoc_dataset(filename=filename)
folder = filename.replace('.parquet', '')
datadir = os.path.join(DATADIR, folder)
destination = os.path.join(datadir, filename)

# Load the dataset from the local parquet file with streaming enabled
dataset = load_dataset('parquet', data_files=destination, streaming=False)

# Access the dataset (assuming no split)
reddit_data = dataset['train']

# Initialize dictionaries to collect data for each interaction type
interaction_data = {}
limit_rows = -1 # 100_000

# Target user_id
# target_user_id = '1bdbc0a9-0e79-5841-985d-dd602228787c'

if len(os.listdir(datadir)) == 1:
    # Stream through the dataset and collect data by interaction type
    i = 0
    post_ids_comments = []
    post_ids_posts = []
    for row in tqdm(reddit_data):
        i += 1
        # print(row)
        # if row['user_id'] == target_user_id:
        interaction_type = row['interaction_type']
        if interaction_type not in interaction_data:
            interaction_data[interaction_type] = []
        interaction_data[interaction_type].append(row)

        if interaction_type == 'COMMENT':
            post_ids_comments.append(row['post_id'])
        elif interaction_type == 'POST':
            post_ids_posts.append(row['post_id'])

        if i == limit_rows:
            break

    post_ids_posts = set(post_ids_posts)
    post_ids_comments = set(post_ids_comments)
    print(f"Number of unique post_ids in POST interaction type: {len(post_ids_posts)}")
    print(f"Number of unique post_ids in COMMENT interaction type: {len(post_ids_comments)}")
    print(f"Number of post_ids in both interaction types: {len(post_ids_posts.intersection(post_ids_comments))}")

    # Create DataFrames for each interaction type and store them in a dictionary
    dfs = {interaction_type: rows for interaction_type, rows in interaction_data.items()}

    for k, df in dfs.items():
        filename = os.path.join(datadir, f"{k}.json")
        # save as json, without assuming pandas
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(str(df))



dfs = dict()
# ds = [d for d in os.listdir(datadir) if target_user_id in d]
ds = [d for d in os.listdir(datadir)]
for d in ds:

    filename = os.path.join(datadir, d)
    with open(filename, 'r', encoding='utf-8') as f:
        data = f.read()
        dfs[d] = data

    # print first 10 rows
    print(data[:1000])

# Display a summary of the DataFrames created
dfs_summary = {interaction_type: df.shape for interaction_type, df in dfs.items()}
print(dfs_summary)
