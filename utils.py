import os, sys
import requests
from tqdm import tqdm

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from MADOC_Data_Analysis.paths import DATADIR

filenames = [
    "bluesky_madoc.parquet",  # 449.3 MB
    "koo_madoc.parquet",  # 774.3 MB
    "reddit_fatpeoplehate_madoc.parquet",
    "reddit_greatawakening_madoc.parquet",
    "reddit_qanon_madoc.parquet",
    "reddit_the_donald_madoc.parquet",
    "reddit_toxic_madoc.parquet",
    "reddit_funny_madoc.parquet",
    "reddit_gifs_madoc.parquet",
    "reddit_gaming_madoc.parquet",
]


def download_madoc_dataset(filename=None):
    if filename is None:
        filename = "reddit_gifs_madoc.parquet"
    folder = filename.replace('.parquet', '')
    assert filename in filenames

    url = f'https://zenodo.org/records/14637314/files/{filename}?download=1'

    # Local filename to save as
    destination = os.path.join(DATADIR, folder, filename)
    if not os.path.exists(destination):
        print('destination:', destination)
        os.makedirs(os.path.join(DATADIR, folder), exist_ok=True)

        # Download the file
        response = requests.get(url, stream=True)
        if response.status_code == 200:
            with open(destination, "wb") as file:
                for chunk in tqdm(response.iter_content(chunk_size=1024)):
                    file.write(chunk)
            print(f"Download complete: {destination}")
        else:
            print(f"Failed to download. Status code: {response.status_code}")


if __name__ == '__main__':
    download_madoc_dataset()
