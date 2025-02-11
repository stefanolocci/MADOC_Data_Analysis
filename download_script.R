install.packages('devtools')

# Load necessary libraries
library(rMADOC)   # for downloading and loading the MADOC data
library(dplyr)    # for data manipulation

# ----------------------------
# 1. Download and load the data
# ----------------------------

# For example, download the Reddit "gaming" community file to disk.
# (Note: some Reddit files are very large, so it’s best to download to disk first.)
filename <- download_file("reddit", "fatpeoplehate", output_dir = "data")