# This script will be used to do standalone trials and tuning of the ref genome validator
import json
import os
import refgenconf
from pipestat import pipestat

from bedboss.refgenome_validator.main import (
    ReferenceValidator,
    GenomeModel,
)

# helper utils
from process_exclude_ranges import unzip_bedfile, get_samples, MAX_SAMPLES

try:
    IGD_DB_PATH = os.environ["IGD_DB_PATH"]
except:
    # Local IGD file
    IGD_DB_PATH = "/home/drc/Downloads/igd_database.igd"

try:
    BEDFILE_DIRECTORY = os.environ["BEDFILE_DIRECTORY"]
except:
    # where bedfiles for testing live (unzipped!)
    # BEDFILE_DIRECTORY = (
    #     "/home/drc/GITHUB/bedboss/bedboss/scripts/ref_genome_validating/results"
    # )
    BEDFILE_DIRECTORY = "/home/drc/GITHUB/bedboss/bedboss/scripts/ref_genome_validating/data/bed_small_subset"
    # BEDFILE_DIRECTORY = "/home/drc/GITHUB/bedboss/bedboss/scripts/ref_genome_validating/data/test_singles"

try:
    PEP_URL = os.environ["PEP_URL"]
except:
    # if you wish to report results to pephub
    PEP_URL = "donaldcampbelljr/refgenome_compat_testing:default"
    # PEP_URL = "donaldcampbelljr/ref_genome_compat_testing_small:default"
    # PEP_URL ="donaldcampbelljr/ref_genome_dros_only:default"
    # PEP_URL = "donaldcampbelljr/ref_genome_compat_testing_refactor:default"

# Where to get Bedfiles?
LOCAL = True
GEOFETCH = False
SPECIES = "homosapiens"
# SPECIES = "fly"


def main():
    # Simple script to testing that the Validator objects is working correctly.
    # Set up Ref Genie
    # try:
    #     ref_genie_config_path = os.environ["REFGENIE"]
    # except:
    #     print("Ref genie environment variable not found")
    #     # hard code for testing for now
    #     ref_genie_config_path = "/home/drc/GITHUB/bedboss/bedboss/scripts/ref_genome_validating/genome_folder/genome_config.yaml"
    #
    # # Ensure refgenie assets are built based on the provided config module
    # rgc = refgenconf.RefGenConf(filepath=ref_genie_config_path)
    # rgc.pull(
    #     genome="hg38", asset="fasta", tag="default", force=False
    # )  # GCA_000001405.15 GRCh38_no_alt_analysis_set from NCBI
    # # rgc.pull(genome="hg38_primary", asset="fasta", tag="default", force=False) # UCSC primary chromosomes only
    # rgc.pull(
    #     genome="hg19", asset="fasta", tag="default", force=False
    # )  # GRCh37 reference sequence from UCSC
    # rgc.pull(genome="mm10", asset="fasta", tag="default", force=False)
    # # rgc.pull(genome="dm6", asset="fasta", tag="default", force=False) #the ncbi chromosomes
    #
    # genome_list = rgc.list()
    #
    # print(genome_list)

    # build genome models
    # for each reference genome in the user's config file, build a genome model

    # from geniml.io import RegionSet
    #
    # ff =RegionSet("/home/drc/GITHUB/bedboss/bedboss/scripts/ref_genome_validating/data/test_singles/GSM8196564_435_RUNX3_KO_H3K27AC_v_435_ctrl_IgG_seacr.relaxed.bed")
    # ff

    all_genome_models = []

    # for reference_genome in rgc.list():
    #     new_genome_model = GenomeModel(genome_alias=reference_genome, refgenomeconf=rgc)
    #     all_genome_models.append(new_genome_model)

    # Manually create more genome models not found in ref genie
    ucsc_hg38 = GenomeModel(
        genome_alias="ucsc_hg38",
        chrom_sizes_file="/home/drc/GITHUB/bedboss/bedboss/bedboss/refgenome_validator/chrom_sizes/ucsc_hg38.chrom.sizes",
    )

    ncbi_hg38 = GenomeModel(
        genome_alias="ncbi_hg38",
        chrom_sizes_file="/home/drc/GITHUB/bedboss/bedboss/bedboss/refgenome_validator/chrom_sizes/ncbi_hg38.chrom.sizes",
    )

    ensembl_hg38 = GenomeModel(
        genome_alias="ensembl_hg38",
        chrom_sizes_file="/home/drc/GITHUB/bedboss/bedboss/bedboss/refgenome_validator/chrom_sizes/ensembl_hg38.chrom.sizes",
    )

    ucsc_hg19 = GenomeModel(
        genome_alias="ucsc_hg19",
        chrom_sizes_file="/home/drc/GITHUB/bedboss/bedboss/bedboss/refgenome_validator/chrom_sizes/ucsc_hg19.chrom.sizes",
    )

    ucsc_dm6 = GenomeModel(
        genome_alias="ucsc_dm6",
        chrom_sizes_file="/home/drc/GITHUB/bedboss/bedboss/bedboss/refgenome_validator/chrom_sizes/ucsc_dm6.chrom.sizes",
    )

    ucsc_mm10 = GenomeModel(
        genome_alias="ucsc_mm10",
        chrom_sizes_file="/home/drc/GITHUB/bedboss/bedboss/bedboss/refgenome_validator/chrom_sizes/ucsc_mm10.chrom.sizes",
    )

    ucsc_mm39 = GenomeModel(
        genome_alias="ucsc_mm39",
        chrom_sizes_file="/home/drc/GITHUB/bedboss/bedboss/bedboss/refgenome_validator/chrom_sizes/ucsc_mm39.chrom.sizes",
    )

    ucsc_pantro6 = GenomeModel(
        genome_alias="ucsc_pantro6",
        chrom_sizes_file="/home/drc/GITHUB/bedboss/bedboss/bedboss/refgenome_validator/chrom_sizes/ucsc_panTro6.chrom.sizes",
    )

    all_genome_models.append(ucsc_hg38)
    all_genome_models.append(ncbi_hg38)
    all_genome_models.append(ucsc_mm10)
    all_genome_models.append(ucsc_mm39)
    all_genome_models.append(ucsc_hg19)
    all_genome_models.append(ensembl_hg38)
    all_genome_models.append(ucsc_pantro6)
    all_genome_models.append(ucsc_dm6)
    # Create Validator Object
    validator = ReferenceValidator(genome_models=all_genome_models)

    # Get BED files
    # LOCAL
    if LOCAL:
        print("Obtaining Bed files locally")
        all_bed_files = []
        for root, dirs, files in os.walk(BEDFILE_DIRECTORY):
            for file in files:
                if file.endswith(".bed"):
                    # print(os.path.join(root, file))
                    all_bed_files.append(os.path.join(root, file))

        for bedfile in all_bed_files[:10]:
            compat_vector = validator.determine_compatibility(bedfile)
            rid = os.path.basename(bedfile)
            tier = {
                "tier_rating": {}
            }  # add this to a column to make comparisons easier for human eyes on pephub
            all_vals = {}
            if compat_vector:
                for i in compat_vector.keys():
                    if i is not None:
                        all_vals.update({i: compat_vector[i].model_dump()})
                        dict_to_check = compat_vector[i].model_dump()
                        if "compatibility" in dict_to_check:
                            tier["tier_rating"].update(
                                {i: dict_to_check["compatibility"]}
                            )

            all_vals.update(tier)

            # use pipestat to report to pephub and file backend
            psm = pipestat.PipestatManager(
                pephub_path=PEP_URL,
            )
            psm2 = pipestat.PipestatManager(
                results_file_path="stats_results/results.yaml"
            )

            # Report to file backend
            psm2.report(record_identifier=rid, values=all_vals)

            # Convert to json string before reporting to pephub
            for key, value in all_vals.items():
                all_vals[key] = str(json.dumps(value))
            psm.report(record_identifier=rid, values=all_vals)

    # # USE GEOFETCH TO OBTAIN BED FILES
    if GEOFETCH:
        print("Obtaining Bed files using Geofetch")
        data_output_path = os.path.abspath("data")
        species_output_path = os.path.join(data_output_path, SPECIES)
        bedfileslist = os.path.join(species_output_path, "bedfileslist.txt")
        results_path = os.path.abspath("results")

        with open(bedfileslist, "r") as file:
            lines = file.readlines()
            lines = [
                line.strip() for line in lines
            ]  # Remove leading/trailing whitespace

            for line in lines:
                samples = get_samples(
                    data_output_path=species_output_path, gse_number=line
                )

                if samples:
                    for sample in samples[:MAX_SAMPLES]:
                        all_vals = {}
                        if isinstance(sample.output_file_path, list):
                            bedfile = sample.output_file_path[0]
                        else:
                            bedfile = sample.output_file_path
                        reported_ref_genome = getattr(sample, "ref_genome", None)
                        all_vals.update({"reported_ref_genome": reported_ref_genome})

                        file = unzip_bedfile(bedfile, results_path)

                        if file:
                            compat_vector = validator.determine_compatibility(bedfile)
                            rid = os.path.basename(bedfile)
                            tier = {
                                "tier_rating": {}
                            }  # add this to a column to make comparisons easier for human eyes on pephub
                            if compat_vector:
                                for i in compat_vector.keys():
                                    if i is not None:
                                        all_vals.update(
                                            {i: compat_vector[i].model_dump()}
                                        )
                                        dict_to_check = compat_vector[i].model_dump()
                                        if "compatibility" in dict_to_check:
                                            tier["tier_rating"].update(
                                                {i: dict_to_check["compatibility"]}
                                            )

                            all_vals.update(tier)

                            # use pipestat to report to pephub and file backend
                            psm = pipestat.PipestatManager(
                                pephub_path=PEP_URL,
                            )
                            psm2 = pipestat.PipestatManager(
                                results_file_path="stats_results/results.yaml"
                            )

                            # Report to file backend
                            psm2.report(record_identifier=rid, values=all_vals)

                            # Convert to json string before reporting to pephub
                            for key, value in all_vals.items():
                                all_vals[key] = str(json.dumps(value))
                            psm.report(record_identifier=rid, values=all_vals)

    return


if __name__ == "__main__":
    main()
