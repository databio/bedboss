# This script exists because some chrom.sizes files have been hard to track down.
# However, one can extract their own from the sequence files. Requires having downloaded a reference genome locally

from Bio import SeqIO


def main():
    # file_path = "/home/drc/Downloads/ncbi_ref_genome/ncbi_dataset/GCF_000001405.40_GRCh38.p14_genomic.fa"
    file_path = "/home/drc/Downloads/backup ref genome/GCA_000001405.29.fasta"
    FastaFile = open(file_path, "r")

    with open(
        "/home/drc/GITHUB/bedboss/bedboss/scripts/ref_genome_validating/chrom_sizes/ensembl_hg38.chrom.sizes",
        "w",
    ) as file:
        for rec in SeqIO.parse(FastaFile, "fasta"):
            name = rec.id
            seq = rec.seq
            seqLen = len(rec)
            print(name)
            print(seqLen)
            file.write(f"{name}\t{seqLen}\n")

    FastaFile.close()


if __name__ == "__main__":
    main()
