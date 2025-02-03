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

broadpeak_digests_4plus5 = df_bed_types[(df_bed_types['bed_format'] == 'broadpeak') & (df_bed_types['bed_type'] == 'bed4+5')]['id']

# Get files that were not labeled at narrowPeaks but do have the type as bed6+4
not_narrowpeak_digests = df_bed_types[(df_bed_types['bed_format'] == 'bed') & (df_bed_types['bed_type'] == 'bed6+4')]['id']

# Get files that were  labeled as narrowPeaks
narrowpeak_digests = df_bed_types[(df_bed_types['bed_format'] == 'narrowpeak') & (df_bed_types['bed_type'] == 'bed6+4')]['id']

narrowpeak_digests_4plus6 = df_bed_types[(df_bed_types['bed_format'] == 'narrowpeak') & (df_bed_types['bed_type'] == 'bed4+6')]['id']

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

NARROWPEAK_RESULTS_FILE ="/home/drc/GITHUB/bedboss/bedboss/scripts/bedclassifier_tuning/results/narrowpeak_results.yaml"
BROADPEAK_RESULTS_FILE ="/home/drc/GITHUB/bedboss/bedboss/scripts/bedclassifier_tuning/results/broadpeak_results.yaml"

#psm = pipestat.PipestatManager(results_file_path=BROADPEAK_RESULTS_FILE)

for digest in DIGESTS[0:1400]:
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
        #print(f"skipping file exists: {file_path}")
        pass

    result = get_bed_type(file_path)
    #print(result)
    #psm.report(record_identifier=digest, values={"bed_type_60rws":result[0],"bed_format_60rws":result[1]})

    # if result[1] != 'broadpeak':
    #     #print(f"This one is not classified as broadpeak: {file_path}")
    #     count_nt_brdpeak += 1
    # else:
    #     #print("FOUND broadpeak")
    #     count_brdpeak +=1

    if result[1] != 'narrowpeak':
        # print(f"This one is not classified as broadpeak: {file_path}")
        count_nt_brdpeak += 1
    else:
        # print("FOUND broadpeak")
        count_brdpeak += 1

print(f"narrowpeak: {count_brdpeak} \nnot narrowpeak:{count_nt_brdpeak}")
# print(get_bed_type("/home/drc/Downloads/ENCFF534JCV.bed.gz"))
# print(get_bed_type("/home/drc/Downloads/ENCFF352KYI.bed.gz"))
#print(get_bed_type("/home/drc/GITHUB/bedboss/bedboss/scripts/bedclassifier_tuning/data/narpks/e7073e8cc9c597824d73e974d4c174b5.bed.gz"))
#print(get_bed_type("/home/drc/test/test_narrowpeak/e7073e8cc9c597824d73e974d4c174b5.bed.gz"))

#print(get_bed_type("/home/drc/GITHUB/bedboss/bedboss/scripts/bedclassifier_tuning/data/narpks/569304341e282330677bee56fd45db0a.bed.gz"))
#print(get_bed_type("/home/drc/GITHUB/bedboss/bedboss/scripts/bedclassifier_tuning/data/narpks/d091b4b1e97ad3c284235d4d43082078.bed.gz"))
#print(get_bed_type("/home/drc/GITHUB/bedboss/bedboss/scripts/bedclassifier_tuning/data/narpks/66788888eaea21c069763798ff719c33.bed.gz"))
#print(get_bed_type("/home/drc/GITHUB/bedboss/bedboss/scripts/bedclassifier_tuning/data/narpks/f759ec1fd104ab1db5a1d01200807937.bed.gz"))

#print(get_bed_type("/home/drc/GITHUB/bedboss/bedboss/scripts/bedclassifier_tuning/data/brdpks/540d9b3fed3341dd491123242e8ad408.bed.gz"))
#print(get_bed_type("/home/drc/GITHUB/bedboss/bedboss/scripts/bedclassifier_tuning/data/brdpks/6d0e8a30a7b9925823dc070dbe50f04f.bed.gz"))
#print(get_bed_type("/home/drc/test/test_broadpeak/6d0e8a30a7b9925823dc070dbe50f04f.bed.gz"))
#print(get_bed_type("/home/drc/GITHUB/bedboss/bedboss/scripts/bedclassifier_tuning/data/brdpks/c0dfe24eaff8af169942d2d8cf098eb6.bed.gz"))
#print(get_bed_type("/home/drc/GITHUB/bedboss/bedboss/scripts/bedclassifier_tuning/data/brdpks/2a8a91074fe6d85d04b0d87cf088f4aa.bed.gz"))

# print(not_narrowpeak_digests.head())
#
# print(broadpeak_digests_4plus5.head())
# print(narrowpeak_digests_4plus6.head())