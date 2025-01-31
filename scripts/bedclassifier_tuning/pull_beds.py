# Just pull selected digests from bedbase

import os.path
import requests
import os

import matplotlib.colors as mcolors
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from bedboss.bedclassifier.bedclassifier import get_bed_type
import pipestat

DATA_FILE_BED_TYPES = "./data/bedbase_manual_pull/23jan2025/bedbase_file_types.csv"
bed_types_path = os.path.abspath(DATA_FILE_BED_TYPES)
df_bed_types = pd.read_csv(bed_types_path)

# Get files that were not labeled at broadPeaks but do have the type as bed6+3
not_broadpeak_digests = df_bed_types[(df_bed_types['bed_format'] == 'bed') & (df_bed_types['bed_type'] == 'bed6+3')]['id']
# Get files that were labeled as broadPeak
broadpeak_digests = df_bed_types[(df_bed_types['bed_format'] == 'broadpeak') & (df_bed_types['bed_type'] == 'bed6+3')]['id']

# Get files that were not labeled at narrowPeaks but do have the type as bed6+4
not_narrowpeak_digests = df_bed_types[(df_bed_types['bed_format'] == 'bed') & (df_bed_types['bed_type'] == 'bed6+4')]['id']

# Get files that were  labeled as narrowPeaks
narrowpeak_digests = df_bed_types[(df_bed_types['bed_format'] == 'narrowpeak') & (df_bed_types['bed_type'] == 'bed6+4')]['id']

dest_folder ="/home/drc/GITHUB/bedboss/bedboss/scripts/bedclassifier_tuning/data/notbrdpks"
dest_folder_2 ="/home/drc/GITHUB/bedboss/bedboss/scripts/bedclassifier_tuning/data/brdpks"
dest_folder_3 ="/home/drc/GITHUB/bedboss/bedboss/scripts/bedclassifier_tuning/data/notnarpks"
dest_folder_4 ="/home/drc/GITHUB/bedboss/bedboss/scripts/bedclassifier_tuning/data/narpks"

#print(not_broadpeak_digests.shape)
count_nt_brdpeak = 0
count_brdpeak = 0

## SET TYPES AND LOCALE FOR LOCAL STORAGE
DIGESTS = narrowpeak_digests
DESTINATION_FOLDER = dest_folder_4

psm = pipestat.PipestatManager(results_file_path="/home/drc/GITHUB/bedboss/bedboss/scripts/bedclassifier_tuning/results/narrowpeak_results.yaml")

for digest in DIGESTS[:300]:
    #https://data2.bedbase.org/files/2/3/233479aab145cffe46221475d5af5fae.bed.gz
    #print(digest)
    # print(digest[0])
    url = f"https://data2.bedbase.org/files/{digest[0]}/{digest[1]}/{digest}.bed.gz"
    filename = url.split('/')[-1]  # Extract filename from URL
    file_path = os.path.join(DESTINATION_FOLDER, filename)
    #print(file_path)
    if not os.path.exists(file_path):
        try:
            response = requests.get(url, stream=True)
            response.raise_for_status()  # Raise an exception for bad status codes

            with open(file_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:  # filter out keep-alive new chunks
                        f.write(chunk)

            print(f"File downloaded successfully: {file_path}")


        except requests.exceptions.RequestException as e:
            print(f"Error downloading {url}: {e}")
    else:
        print(f"skipping file exists: {file_path}")
        pass

    result = get_bed_type(file_path)
    print(result)
    psm.report(record_identifier=digest, values={"bed_type":result})

    if result[1] != 'narrowpeak':
        print(f"This one is not classified as narrowpeak: {file_path}")
        count_nt_brdpeak += 1
    else:
        print("FOUND narrowpeak")
        count_brdpeak +=1

print(f"narrowpeak: {count_brdpeak} \nnot narrowpeak:{count_nt_brdpeak}")
# print(get_bed_type("/home/drc/Downloads/ENCFF534JCV.bed.gz"))
# print(get_bed_type("/home/drc/Downloads/ENCFF352KYI.bed.gz"))

#print(not_narrowpeak_digests.head())