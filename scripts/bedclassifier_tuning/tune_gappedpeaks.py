# Run classify on a selection of gappedPeaks
import os
from bedboss.bedclassifier.bedclassifier import get_bed_type

DATA_PATH = "/home/drc/test/gappedPeaks/"

all_files = []
for root, _, files in os.walk(DATA_PATH):
    for file in files:
        full_path = os.path.join(root, file)
        all_files.append(full_path)


for file in all_files:
    result = get_bed_type(file)
    print(result)