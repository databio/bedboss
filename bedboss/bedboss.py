import bedstat
from bedmaker import BedMaker
import os


def run_bedboss(
    sample_name: str,
    input_file: str,
    input_type: str,
    output_folder: str,
    genome: str,
    bedbase_config: str,
    open_signal_matrix: str = None,
    ensdb: str = None,
    rfg_config: str = None,
    narrowpeak: bool = False,
    check_qc: bool = True,
    sample_yaml: str = None,
    standard_chrom: bool = False,
    chrom_sizes: str = None,
    just_db_commit: bool = False,
    no_db_commit: bool = False,
):
    """
    Running bedmaker, bedqc and bedstat in one package
    :param sample_name: Sample name [required]
    :param input_file: Input file [required]
    :param input_type: Input type [required] options: (bigwig|bedgraph|bed|bigbed|wig)
    :param output_folder: Folder, where output should be saved  [required]
    :param genome: genome_assembly of the sample. [required] options: (hg19, hg38) #TODO: add more
    :param bedbase_config: a path to the bedbase configuration file.[required] #TODO: add example
    :param open_signal_matrix:
    :param ensdb:
    :param rfg_config:
    :param narrowpeak:
    :param check_qc:
    :param sample_yaml:
    :param standard_chrom:
    :param chrom_sizes:
    :param just_db_commit:
    :param no_db_commit:
    :return: NoReturn??
    """
    cwd = os.getcwd()
    if not rfg_config:
        rfg_config = os.path.join(cwd, "genome_config.yaml")

    if not open_signal_matrix:
        if genome == "hg19":
            open_signal_matrix = "./openSignalMatrix/openSignalMatrix_hg38_percentile99_01_quantNormalized_round4d.txt.gz"
        if genome == "hg38":
            open_signal_matrix = "./openSignalMatrix/openSignalMatrix_hg38_percentile99_01_quantNormalized_round4d.txt.gz"

    if not sample_yaml:
        sample_yaml = f"{sample_name}.yaml"


    output_bed = os.path.join(output_folder, "files_bed", f"{sample_name}.bed.gz")
    output_bigbed = os.path.join(output_folder, "files_bigbed")


    BedMaker(input_file=input_file,
             input_type=input_type,
             output_bed=output_bed,
             output_bigbed=output_bigbed,
             sample_name=sample_name,
             genome=genome,
             rfg_config=rfg_config,
             narrowpeak=narrowpeak,
             check_qc=check_qc,
             standard_chrom=standard_chrom,
             chrom_sizes=chrom_sizes,
             ).make()

    bedstat.Bedstat.run_bedstat(bedfile=output_bed,
                        bigbed=output_bigbed,
                        genome_assembly=genome,
                        ensdb=ensdb,
                        open_signal_matrix=open_signal_matrix,
                        bedbase_config=bedbase_config,
                        sample_yaml=sample_yaml,
                        just_db_commit=just_db_commit,
                        no_db_commit=no_db_commit,
                        )


#--input-file /home/bnt4me/Virginia/bed_base_all/bedbase/bedbase_tutorial/files/hg38/AML_db1.bed.gz --output-bed /home/bnt4me/Virginia/bed_base_all/bedbase/bedbase_tutorial/bed_files/AML_db1.bed.gz --output-bigbed /home/bnt4me/Virginia/bed_base_all/bedbase/bedbase_tutorial/bigbed_files --narrowpeak True --input-type bed --genome hg38 --rfg-config genome_config.yaml --sample-name AML_db1

# bedstat --bedfile /home/bnt4me/Virginia/bed_base_all/bedbase/bedbase_tutorial/bed_files/AML_db1.bed.gz --genome hg38 --sample-yaml ./AML_db1_sample.yaml  --bedbase-config /home/bnt4me/Virginia/repos/bedstat/tests/config_db_local.yaml   --open-signal-matrix /home/bnt4me/Virginia/bed_base_all/bedbase/bedbase_tutorial/openSignalMatrix_hg38_percentile99_01_quantNormalized_round4d.txt.gz   --bigbed /home/bnt4me/Virginia/bed_base_all/bedbase/bedbase_tutorial/bigbed_files

run_bedboss(sample_name="new",
            input_file="/home/bnt4me/Virginia/bed_base_all/bedbase/bedbase_tutorial/files/hg38/AML_db1.bed.gz",
            input_type="bed",
            output_folder="../test_f",
            genome="hg38",
            rfg_config="../test_f/cfg.yaml",
            bedbase_config="/home/bnt4me/Virginia/repos/bedboss/bedboss/config_db_local.yaml",
            )