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

# Should we be getting a line count?


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
        val = int(round(pct * total / 100.0))
        return "{:.1f}%\n({v:d})".format(pct, v=val)

    return my_format


plt.pie(bed_format_counts, autopct=autopct_format(bed_format_counts))
# plt.show()
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


df_bed_types["bed_type"].value_counts(normalize=True).plot(kind="barh")

# pt.show()
plt.savefig("./results/bed_type_distribution.png")
plt.close()

# Get 6+4 beds aka narrowPeaks
bed_6plus4_rows = df_bed_types[df_bed_types["bed_type"] == "bed6+4"]
bed_6plus4_rows_count = len(bed_6plus4_rows)
print(
    f"6+4 vs # narrowpeaks: {bed_6plus4_rows_count},{narrowpeakcount}  {(narrowpeakcount/bed_6plus4_rows_count)*100}"
)

# Get 6+3 beds
bed_6plus3_rows = df_bed_types[df_bed_types["bed_type"] == "bed6+3"]
bed_6plus3_rows_count = len(bed_6plus3_rows)
print(
    f"6+3 vs # broadpeaks: {bed_6plus3_rows_count},{broadpeakcount}  {(broadpeakcount/bed_6plus3_rows_count)*100}"
)

# Get all 3+# BEDS
all_bed3 = [
    "bed3+0",
    "bed3+3",
    "bed3+8",
    "bed3+2",
    "bed3+4",
    "bed3+1",
    "bed3+5",
    "bed3+9",
]
bed3_rows = df_bed_types[df_bed_types["bed_type"].isin(all_bed3)]
bed3_rows_count = len(bed3_rows)

print(f"All other bed3+#: {bed3_rows_count}")

print(
    f"Remaining: {total_rows-bed3_rows_count-bed_6plus3_rows_count-bed_6plus4_rows_count} , {(total_rows-bed3_rows_count-bed_6plus3_rows_count-bed_6plus4_rows_count)*100/total_rows}  "
)

geo_fail_path = os.path.abspath(DATA_FILE_GEO_FAIL)
df_geo_fail = pd.read_csv(geo_fail_path)
geo_fail_counts = df_geo_fail["error"].value_counts()

print(geo_fail_counts)

# What about breakdown of format into provided genome aliases?

beds_genomes = df_bed_types[df_bed_types["bed_format"] == "bed"][
    "genome_alias"
].value_counts()
# beds_genomes= df_bed_types[df_bed_types['bed_format'] == 'bed']['genome_alias'].value_counts().plot(kind="barh" )
# plt.show()
# plt.savefig("./results/bed_genomes.png")
# plt.close()
print(beds_genomes)
bed_genomes_df = beds_genomes.reset_index()
bed_genomes_df.columns = ["genome_alias", "count"]
bed_genomes_df.to_csv("/home/drc/Downloads/bed_genomes_df.csv")

narrowpeaks_genomes = df_bed_types[df_bed_types["bed_format"] == "narrowpeak"][
    "genome_alias"
].value_counts()
print(narrowpeaks_genomes)
narrowpeaks_genomes_df = narrowpeaks_genomes.reset_index()
narrowpeaks_genomes_df.columns = ["genome_alias", "count"]
narrowpeaks_genomes_df.to_csv("/home/drc/Downloads/narrow_peaks_df.csv")


broadpeaks_genomes = df_bed_types[df_bed_types["bed_format"] == "broadpeak"][
    "genome_alias"
].value_counts()
print(broadpeaks_genomes)
broadpeaks_genomes_df = broadpeaks_genomes.reset_index()
broadpeaks_genomes_df.columns = ["genome_alias", "count"]
broadpeaks_genomes_df.to_csv("/home/drc/Downloads/broadpeaks_df.csv")


###### REFERENCE GENOME COMPATIBILITY
print("REFGENOME COMPAT")
reference_genome_compat_path = os.path.abspath(DATA_FILE_REFERENCE_STATS)
df_ref_genome_compat = pd.read_csv(reference_genome_compat_path)

print(df_ref_genome_compat.head(n=10))

df_ref_genome_compat["rank"] = df_ref_genome_compat.groupby("bed_id")[
    "assigned_points"
].rank(method="dense", ascending=True)

print(df_ref_genome_compat.head(n=10))

# best_ranked_genomes = df_ref_genome_compat[df_ref_genome_compat['rank'] == 1].groupby('bed_id')['compared_genome'].first()

# just give us the best ranked genome, so, in many cases this will be something with less points (better compatibility) but in some cases a poorly ranked genomes will still be the best case
best_ranked_genomes_df = (
    df_ref_genome_compat[df_ref_genome_compat["rank"] == 1].groupby("bed_id").first()
)
print("BEST RANKED")
print(best_ranked_genomes_df)
best_ranked_genomes_df.to_csv("/home/drc/Downloads/best_ranked_genomes.csv")

best_ranked_genomes_df["is_substring"] = best_ranked_genomes_df.apply(
    lambda row: 1 if str(row["provided_genome"]) in row["compared_genome"] else 0,
    axis=1,
)
best_ranked_genomes_df.to_csv("/home/drc/Downloads/best_ranked_genomes_2.csv")
# best_ranked_genomes_df = best_ranked_genomes.reset_index()
print(
    best_ranked_genomes_df["is_substring"].value_counts()
)  # How well did we do 1 vs 0, i.e. did the ranked 1st choice coincide with provided genome?

best_ranked_genomes_df["positive_prediction_quality"] = best_ranked_genomes_df.apply(
    lambda row: 1 if row["is_substring"] == 1 and row["assigned_points"] == 0 else 0,
    axis=1,
)
best_ranked_genomes_df.to_csv("/home/drc/Downloads/best_ranked_genomes_3.csv")
print(best_ranked_genomes_df["positive_prediction_quality"].value_counts())
# best_ranked_genomes_df.columns=['id','bed_id', 'compared_genome', 'xs', 'oobr','sequence_fit', 'assigned_points','tier_ranking','rank']
# print(best_ranked_genomes_df.columns())
# df['is_substring'] = df.apply(lambda row: row['provided_genome'] in row['compared_genome'], axis=1)
