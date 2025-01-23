# this is a script to run and data pulled directly from bedbase
# it assumes you've pulled the data into a local csv for analysis
import os.path

import matplotlib.colors as mcolors
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

DATA_FILE_BED_TYPES = "./data/bedbase_manual_pull/23jan2025/bedbase_file_types.csv"
# /home/drc/GITHUB/bedboss/bedboss/scripts/bedclassifier_tuning/data/bedbase_manual_pull/23jan2025/bedbase_file_types.csv
DATA_FILE_REFERENCE_STATS = "./data/bedbase_manual_pull/23jan2025/ref_stats.csv"


bed_types_path = os.path.abspath(DATA_FILE_BED_TYPES)
df_bed_types = pd.read_csv(bed_types_path)

# "bed_type" "bed_format"
# bed format can be either bed, narrowpeak, broadpeak
# we have name but not original file name (which would include an extension)

# Count the occurrences of each bed_format
bed_format_counts = df_bed_types["bed_format"].value_counts()
total_rows = df_bed_types.shape[0]

print(bed_format_counts)
print(total_rows)
narrowpeakcount = bed_format_counts.iloc[0]
bedcount = bed_format_counts.iloc[1]
broadpeakcount = bed_format_counts.iloc[2]
print(narrowpeakcount, bedcount, broadpeakcount)

# Count the occurrences of each bed_type
bed_type_counts = df_bed_types["bed_type"].value_counts()
print(bed_type_counts)

df_bed_types["bed_type"].value_counts(normalize=True).plot(kind="barh")
# pt.show()
plt.savefig("./results/bed_type_distribution.png")
