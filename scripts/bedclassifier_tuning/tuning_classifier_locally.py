# Run classify on a selection of local files
import os
from bedboss.bedclassifier.bedclassifier import get_bed_classification

# DATA_PATH = "/home/drc/test/gappedPeaks/"
# DATA_PATH = "/home/drc/test/test_tagalign/"
# DATA_PATH = "/home/drc/test/test_peptidemapping/"

# DATA_PATH = "/home/drc/GITHUB/bedboss/bedboss/scripts/bedclassifier_tuning/data/brdpks/"
# (BED_12_PLUS_0, ("bed12+0", "ucsc_bed")),
# (BED_12_PLUS_3, ("bed12+3", "ucsc_bed")),
# (BED_NARROWPEAK, ("bed6+4", "encode_narrowpeak")),
# (BED_NONSTRICT_NARROWPEAK, ("bed6+4", "ns_narrowpeak")),
# (BED_RNA_ELEMENTS, ("bed6+3", "encode_rna_elements")),
# (BED_BROADPEAK, ("bed6+3", "encode_broadpeak")),"

data_paths = []
data_paths.append(
    "/home/drc/GITHUB/bedboss/bedboss/scripts/bedclassifier_tuning/data/brdpks/"
)
data_paths.append(
    "/home/drc/GITHUB/bedboss/bedboss/scripts/bedclassifier_tuning/data/notbrdpks/"
)
data_paths.append(
    "/home/drc/GITHUB/bedboss/bedboss/scripts/bedclassifier_tuning/data/narpks/"
)
data_paths.append(
    "/home/drc/GITHUB/bedboss/bedboss/scripts/bedclassifier_tuning/data/notnarpks/"
)
data_paths.append(
    "/home/drc/GITHUB/bedboss/bedboss/scripts/bedclassifier_tuning/data/bed6plus3/"
)
data_paths.append(
    "/home/drc/GITHUB/bedboss/bedboss/scripts/bedclassifier_tuning/data/bed4plus6/"
)


for data_path in data_paths:
    all_files = []
    for root, _, files in os.walk(data_path):
        for file in files:
            full_path = os.path.join(root, file)
            all_files.append(full_path)
    total_files = 0
    count_bed = 0
    count_nar = 0
    count_ns_nar = 0
    count_rna = 0
    count_broad = 0
    count_unknown = 0
    for file in all_files:
        total_files += 1
        try:
            result = get_bed_classification(file)
            if result[1] == "ucsc_bed":
                count_bed += 1
            elif result[1] == "encode_narrowpeak":
                count_nar += 1
            elif result[1] == "ns_narrowpeak":
                count_ns_nar += 1
            elif result[1] == "encode_broadpeak":
                count_broad += 1
            elif result[1] == "encode_rna_elements":
                count_rna += 1
            else:
                count_unknown += 1
        except Exception:
            pass
    print(data_path)
    print(
        f"Total: {total_files}\nBED: {count_bed}\nNarrowPeak: {count_nar}\nNon-strict NarrowPeak: {count_ns_nar}\nBroadPeak: {count_broad}\nRNA: {count_rna} "
    )


# One-off testing

# result = get_bed_classification("/home/drc/test/test_gappedPeaks_geofetched/data/GSE192575/GSM5751922_ATAC_resis_1_peaks.gappedPeak.gz")
# result = get_bed_classification("/home/drc/test/test_gappedPeaks_geofetched/data/GSE192575/GSM5751923_ATAC_resis_2_peaks.gappedPeak.gz")

# print(result)
