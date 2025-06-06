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

data_paths.append(
    "/home/drc/GITHUB/bedboss/bedboss/scripts/bedclassifier_tuning/data/bed4plus/"
)

data_paths.append(
    "/home/drc/GITHUB/bedboss/bedboss/scripts/bedclassifier_tuning/data/misc/"
)

data_paths.append(
    "/home/drc/GITHUB/bedboss/bedboss/scripts/bedclassifier_tuning/data/bed3plus/"
)

data_paths.append("/home/drc/test/gappedPeaks/")


for data_path in data_paths:
    all_files = []
    for root, _, files in os.walk(data_path):
        for file in files:
            full_path = os.path.join(root, file)
            all_files.append(full_path)
    total_files = 0
    count_bed = 0
    count_bedlike = 0
    count_bedlike_rs = 0
    count_nar = 0
    count_ns_nar = 0
    count_gapped = 0
    count_gapped_rs = 0
    count_rna = 0
    count_rna_ns = 0
    count_broad = 0
    count_broad_ns = 0
    count_unknown = 0
    for file in all_files:
        total_files += 1
        # print(f"Opening file: {file}")
        try:
            result = get_bed_classification(file)
            # print(result)
            if result.data_format == "ucsc_bed":
                count_bed += 1
            elif result.data_format == "bed_like":
                count_bedlike += 1
            elif result.data_format == "bed_like_rs":
                count_bedlike_rs += 1
            elif result.data_format == "encode_narrowpeak":
                count_nar += 1
            elif result.data_format == "encode_narrowpeak_rs":
                count_ns_nar += 1
            elif result.data_format == "encode_broadpeak":
                count_broad += 1
            elif result.data_format == "encode_broadpeak_rs":
                count_broad_ns += 1
            elif result.data_format == "encode_rna_elements":
                count_rna += 1
            elif result.data_format == "encode_rna_elements_rs":
                count_rna_ns += 1
            elif result.data_format == "encode_gappedpeak":
                count_gapped += 1
            elif result.data_format == "encode_gappedpeak_rs":
                count_gapped_rs += 1
            else:
                count_unknown += 1
        except Exception:
            pass
    print(data_path)
    print(
        f"Total: {total_files}\nBED: {count_bed}\n bed-like: {count_bedlike}\nbed-like-rs: {count_bedlike_rs}\nNarrowPeak: {count_nar}\nNarrowPeakRelaxed: {count_ns_nar}\nBroadPeak: {count_broad}\nBroadPeakRelaxed: {count_broad_ns}\nRNA: {count_rna}\nRNARelaxed: {count_rna_ns}\nGappedPeak: {count_gapped}\nnGappedPeakRelaxed: {count_gapped_rs}"
    )


# One-off testing

# result = get_bed_classification(
#     "/home/drc/Downloads/example_Gapped_peaks/example.gappedPeak.gz"
# )
# result2 = get_bed_classification(
#     "/home/drc/Downloads/example_Gapped_peaks/example_2.gappedPeak.gz"
# )
# result = get_bed_classification("/home/drc/test/test_gappedPeaks_geofetched/data/GSE192575/GSM5751922_ATAC_resis_1_peaks.gappedPeak.gz")
# result2 = get_bed_classification("/home/drc/test/test_gappedPeaks_geofetched/data/GSE192575/GSM5751923_ATAC_resis_2_peaks.gappedPeak.gz")

# print(result)
# print(result2)
