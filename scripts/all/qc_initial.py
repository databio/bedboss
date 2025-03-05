from bedboss.utils import run_initial_qc

if __name__ == "__main__":

    url = "ftp://ftp.ncbi.nlm.nih.gov/geo/samples/GSM6760nnn/GSM6760881/suppl/GSM6760881_Sperm_14.5hmC.bed.gz"
    # url = "ftp://ftp.ncbi.nlm.nih.gov/geo/samples/GSM8669nnn/GSM8669735/suppl/GSM8669735_ATAC-M1-1.narrowPeak.gz"
    # url = "ftp://ftp.ncbi.nlm.nih.gov/geo/samples/GSM8669nnn/GSM8669759/suppl/GSM8669759_H3K27ac-M2-1.narrowPeak.gz"
    url = "ftp://ftp.ncbi.nlm.nih.gov/geo/samples/GSM7163nnn/GSM7163568/suppl/GSM7163568_IRF3-ChIP-seq_HL_SVI_3h_exp1_2023_Total_peaks.bed.gz"
    run_initial_qc(url)