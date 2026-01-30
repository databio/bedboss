from typing import Union

cell_lines = {
    "1205lu": "1205Lu",
    "12z": "12Z",
    "22rv1": "22Rv1",
    "293t": "293T",
    "2ftgh": "2fTGH",
    "3t3-l1": "3T3-L1",
    "46c mescs": "46C mESCs",
    "4t1": "4T1",
    "697": "697",
    "786-o": "786-O",
    "a375": "A375",
    "a498": "A498",
    "a549": "A549",
    "a673": "A673",
    "ag04450": "AG04450",
    "ag06561": "AG06561",
    "ags": "AGS",
    "an3ca": "AN3CA",
    "aspc-1": "AsPC-1",
    "asc52telo": "ASC52telo",
    "be2c": "BE2C",
    "bin67": "BIN67",
    "bj": "BJ",
    "bt474": "BT474",
    "bt549": "BT549",
    "c2c12": "C2C12",
    "c3h/10t1/2": "C3H/10T1/2",
    "caco-2": "Caco-2",
    "calu-3": "Calu-3",
    "colo320dm": "COLO320DM",
    "cov434": "COV434",
    "dnd-41": "DND-41",
    "dohh2": "DOHH2",
    "du145": "DU145",
    "e14": "E14",
    "e14tg2a": "E14tg2a",
    "f123 hybrid esc": "F123 hybrid ESC",
    "g401": "G401",
    "gbm002": "GBM002",
    "gm06990": "GM06990",
    "gm12878": "GM12878",
    "gm12891": "GM12891",
    "gm12892": "GM12892",
    "gm23338": "GM23338",
    "gm23248": "GM23248",
    "h1": "H1",
    "h1-hesc": "H1-hESC",
    "h7": "H7",
    "h9": "H9",
    "h1975": "H1975",
    "hap1": "HAP1",
    "hcc1954": "HCC1954",
    "hcc3153": "HCC3153",
    "hek293": "HEK293",
    "hek293t": "HEK293T",
    "hek293wt": "HEK293WT",
    "hela": "HeLa",
    "hela-s3": "HeLa-S3",
    "hl-60": "HL-60",
    "hoxb8": "HoxB8",
    "ht1080": "HT1080",
    "ht1376": "HT1376",
    "ht29": "HT29",
    "hudep2": "HUDEP2",
    "huh7": "Huh7",
    "huvec": "HUVEC",
    "imr-90": "IMR-90",
    "ishikawa": "Ishikawa",
    "j1": "J1",
    "jurkat": "Jurkat",
    "k562": "K562",
    "karpas-422": "Karpas-422",
    "kc167": "Kc167",
    "kh2": "KH2",
    "kms-11": "KMS-11",
    "kp4": "KP4",
    "koptk1": "KOPTK1",
    "lncap": "LNCaP",
    "loucy": "Loucy",
    "mcf-7": "MCF-7",
    "mcf10a": "MCF10A",
    "mda-mb-231": "MDA-MB-231",
    "mda-mb-453": "MDA-MB-453",
    "mef": "MEF",
    "mkl-1": "MKL-1",
    "molm-13": "MOLM-13",
    "mm.1s": "MM.1S",
    "mv4-11": "MV4-11",
    "nb4": "NB4",
    "nci-h226": "NCI-H226",
    "nci-h929": "NCI-H929",
    "nalm6": "Nalm6",
    "nih-3t3": "NIH-3T3",
    "nt2/d1": "NT2/D1",
    "oci-ly1": "OCI-LY1",
    "oci-ly3": "OCI-LY3",
    "oci-ly7": "OCI-LY7",
    "panc1": "PANC1",
    "pc-3": "PC-3",
    "pc-9": "PC-9",
    "raw264.7": "RAW264.7",
    "rd": "RD",
    "riva": "RIVA",
    "rko": "RKO",
    "rh30": "RH30",
    "rh4": "RH4",
    "rues2": "RUES2",
    "rues02": "RUES02",
    "sh-sy5y": "SH-SY5Y",
    "sk-n-mc": "SK-N-MC",
    "sk-n-sh": "SK-N-SH",
    "skbr3": "SKBR3",
    "sum149": "SUM149",
    "sum159": "SUM159",
    "sw480": "SW480",
    "su-dhl-6": "SU-DHL-6",
    "t47d": "T47D",
    "thp-1": "THP-1",
    "u2os": "U2OS",
    "u937": "U937",
    "uacc-257": "UACC-257",
    "vcap": "VCaP",
    "vero e6": "VERO E6",
    "wi38": "WI38",
    "zr-75-1": "ZR-75-1",
}


assay_map = {
    # ---- TF & Histone ChIP (most specific first) ----
    "tf chip-seq": "TF ChIP-seq",
    "histone chip-seq": "Histone ChIP-seq",
    "h3k27ac": "H3K27ac",
    "h3k27me3": "H3K27me3",
    "h3k9me2": "H3K9me2",
    "h3k9me3": "H3K9me3",
    "h3k4me3": "H3K4me3",
    "h3k4me2": "H3K4me2",
    "h3k4me1": "H3K4me1",
    "h3k36me3": "H3K36me3",
    "h3k79me2": "H3K79me2",
    "h3k79me3": "H3K79me3",
    "h3k27me1": "H3K27me1",
    "h3k9ac": "H3K9ac",
    "h3k14ac": "H3K14ac",
    "h3k18ac": "H3K18ac",
    "h3k23ac": "H3K23ac",
    "h4k20me1": "H4K20me1",
    "h4k20me3": "H4K20me3",
    "h2ak119ub": "H2AK119ub",
    "h2bk120ub": "H2BK120ub",
    "biotin chip-seq": "Biotin ChIP-seq",
    # ---- Generic ChIP ----
    "chip-seq": "ChIP-seq",
    "chip seq": "ChIP-seq",
    # ---- CUT-based assays ----
    "cut&tag": "CUT&Tag",
    "cut&run": "CUT&RUN",
    # ---- ATAC variants ----
    "snail atac-seq": "SNAIL ATAC-seq",
    "csnail atac-seq": "cSNAIL ATAC-seq",
    "scatac-seq": "scATAC-seq",
    "roboatac": "roboATAC",
    "atac-seq": "ATAC-seq",
    "atac": "ATAC-seq",
    # ---- DNase ----
    "gm dnase-seq": "GM DNase-seq",
    "dnase-hypersensitivity": "DNase-Hypersensitivity",
    "dnase-seq": "DNase-seq",
    # ---- RNA & derivatives ----
    "rampage": "RAMPAGE",
    "cage": "CAGE",
    "rna-seq": "RNA-Seq",
    "ncrna-seq": "ncRNA-Seq",
    "mirna-seq": "miRNA-Seq",
    "rip-seq": "RIP-Seq",
    "eclip": "eCLIP",
    "icl ip-seq": "iCLIP-seq",
    # ---- Chromatin conformation ----
    "hi-c": "Hi-C",
    "chia-pet": "ChIA-PET",
    # ---- Methylation & accessibility ----
    "medip-seq": "MeDIP-Seq",
    "mbd-seq": "MBD-Seq",
    "bisulfite-seq": "Bisulfite-Seq",
    "faire-seq": "FAIRE-seq",
    "mnase-seq": "MNase-Seq",
    # ---- Specialized ----
    "end-seq": "END-seq",
    "tn-seq": "Tn-Seq",
    "bruuvseq": "bruUVseq",
    "bruseq": "BruSeq",
    "selex": "SELEX",
    "microrna counts": "microRNA counts",
    "mitoperturb-seq": "MitoPerturb-Seq",
    # ---- Fallback ----
    "other": "OTHER",
}


def standardize_cell_line(cell_line: str) -> str:
    """
    Standardize cell line names to a consistent format.

    :param cell_line: The input cell line name.
    :return: The standardized cell line name.
    """

    key = cell_line.lower()
    if key not in cell_lines:
        return cell_line
    return cell_lines.get(key, cell_line)


def standardize_assay(assay: str) -> str:
    """
    Standardize assay names to a consistent format.

    :param assay: The input assay name.
    :return: The standardized assay name.
    """

    key = assay.lower()
    if key not in assay_map:
        return assay
    return assay_map.get(key, assay)


def find_assay(description: str) -> str:
    """
    Identify the assay type from a given description string.
    e.g. "encff285mhb_idr_thresholded_peaks_grch38_bed TF ChIP-seq from K562 (ENCSR861UWB)"
    will return "TF ChIP-seq"

    :param description: The input description string.
    :return: The identified assay type or None if not found.
    """
    description_lower = description.lower()
    for key in assay_map.keys():
        if key in description_lower:
            return assay_map[key]
    return ""


def find_cell_line(description: str) -> str:
    """
    Identify the cell line from a given description string.

    e.g. encff285mhb_idr_thresholded_k562_peaks_grch38_bed
    will return "K562"

    :param description: The input description string.
    :return: The identified cell line or None if not found.
    """
    description_lower = description.lower()
    for key in cell_lines.keys():
        if key in description_lower:
            return cell_lines[key]
    return ""
