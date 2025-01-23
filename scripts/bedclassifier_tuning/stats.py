# this is a script to run and data pulled directly from bedbase
# it assumes you've pulled the data into a local csv for analysis

# Three Required Fields, 9 additional for BED files
# broadPeak = 6+3
# narrowpeak = 6+4

# We are classifying everything as bed if it is not broadPeak, narrowPeak
# need file name, just so we can get extension, and the input_type

# Sources ENCODE, GEO, only geo is flagging as success or fail

# geo 1042 processed, 39 failed -> 3.7% for GEO

# we can map genome to genome provided


import os.path

import matplotlib.colors as mcolors
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

DATA_FILE_BED_TYPES = "./data/bedbase_manual_pull/23jan2025/bedbase_file_types.csv"
# /home/drc/GITHUB/bedboss/bedboss/scripts/bedclassifier_tuning/data/bedbase_manual_pull/23jan2025/bedbase_file_types.csv
DATA_FILE_REFERENCE_STATS = "./data/bedbase_manual_pull/23jan2025/ref_stats.csv"

DATA_FILE_GEO_FAIL = "./data/bedbase_manual_pull/23jan2025/fail_geo.csv"


bed_types_path = os.path.abspath(DATA_FILE_BED_TYPES)
df_bed_types = pd.read_csv(bed_types_path)

# "bed_type" "bed_format"
# bed format can be either bed, narrowpeak, broadpeak
# we have name but not original file name (which would include an extension)

# Count the occurrences of each bed_format
bed_format_counts = df_bed_types["bed_format"].value_counts()
df_bed_types["bed_format"].value_counts(normalize=True).plot(kind="pie")


def autopct_format(values):
    def my_format(pct):
        total = sum(values)
        val = int(round(pct*total/100.0))
        return '{:.1f}%\n({v:d})'.format(pct, v=val)
    return my_format

plt.pie(bed_format_counts, autopct=autopct_format(bed_format_counts))
#plt.show()
plt.savefig("./results/bed_format_pie_chart.png")
plt.close()

total_rows = df_bed_types.shape[0]

print(bed_format_counts)
print(total_rows)

# BE CAREFUL, THE POSITIONS MAY SHIFT
narrowpeakcount = bed_format_counts.iloc[0]
bedcount = bed_format_counts.iloc[1]
broadpeakcount = bed_format_counts.iloc[2]
print(narrowpeakcount, bedcount, broadpeakcount)

# Count the occurrences of each bed_type
bed_type_counts = df_bed_types["bed_type"].value_counts()
print(bed_type_counts)


df_bed_types["bed_type"].value_counts(normalize=True).plot(kind="barh" )

# pt.show()
plt.savefig("./results/bed_type_distribution.png")
plt.close()

# Get 6+4 beds aka narrowPeaks
bed_6plus4_rows = df_bed_types[df_bed_types['bed_type'] == 'bed6+4']
bed_6plus4_rows_count = len(bed_6plus4_rows)
print(f"6+4 vs # narrowpeaks: {bed_6plus4_rows_count},{narrowpeakcount}  {(narrowpeakcount/bed_6plus4_rows_count)*100}")

# Get 6+3 beds
bed_6plus3_rows = df_bed_types[df_bed_types['bed_type'] == 'bed6+3']
bed_6plus3_rows_count = len(bed_6plus3_rows)
print(f"6+3 vs # broadpeaks: {bed_6plus3_rows_count},{broadpeakcount}  {(broadpeakcount/bed_6plus3_rows_count)*100}")

# Get all 3+# BEDS
all_bed3=["bed3+0", "bed3+3","bed3+8", "bed3+2", "bed3+4", "bed3+1", "bed3+5","bed3+9"]
bed3_rows = df_bed_types[df_bed_types['bed_type'].isin(all_bed3)]
bed3_rows_count = len(bed3_rows)

print(f"All other bed3+#: {bed3_rows_count}")

print(f"Remaining: {total_rows-bed3_rows_count-bed_6plus3_rows_count-bed_6plus4_rows_count} , {(total_rows-bed3_rows_count-bed_6plus3_rows_count-bed_6plus4_rows_count)*100/total_rows}  ")

geo_fail_path = os.path.abspath(DATA_FILE_GEO_FAIL)
df_geo_fail = pd.read_csv(geo_fail_path)
geo_fail_counts = df_geo_fail["error"].value_counts()

print(geo_fail_counts)