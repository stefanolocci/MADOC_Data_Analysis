from datasets import load_dataset
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

# select 1000 samples
# reddit_data = reddit_data.select(np.arange(1000))

print(reddit_data.column_names)

# select subset of dataset with interaction_type == 'POST'
posts = reddit_data.filter(lambda x: x['interaction_type'] == 'POST')
comments = reddit_data.filter(lambda x: x['interaction_type'] == 'COMMENT')

print(posts.shape)
print(comments.shape)

print('almost there')
post_id_posts = set(posts['post_id'])
post_id_comments = set(comments['post_id'])
print('almost there there')
print(f"Number of unique post_ids in POST interaction type: {len(post_id_posts)}")
print(f"Number of unique post_ids in COMMENT interaction type: {len(post_id_comments)}")
print(f"Number of post_ids in both interaction types: {len(set(post_id_posts).intersection(set(post_id_comments)))}")


print('there')
