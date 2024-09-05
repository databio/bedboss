# This script will be used to do standalone trials and tuning of the ref genome validator

import os
import refgenconf

from bedboss.refgenome_validator import *


def main():
    # Simple script to testing that the Validator objects is working correctly.
    # Set up Ref Genie
    try:
        ref_genie_config_path = os.environ["REFGENIE"]
    except:
        print("Ref genie environment variable not found")
        # hard code for testing for now
        ref_genie_config_path = "/home/drc/GITHUB/bedboss/bedboss/scripts/ref_genome_validating/genome_folder/genome_config.yaml"

    # Ensure refgenie assets are built based on the provided config module
    rgc = refgenconf.RefGenConf(filepath=ref_genie_config_path)
    rgc.pull(genome="hg38", asset="fasta", tag="default", force=False)
    rgc.pull(genome="hg19", asset="fasta", tag="default", force=False)

    genome_list = rgc.list()

    print(genome_list)

    # build genome models
    # for each reference genome in the user's config file, build a genome model

    all_genome_models = []

    for reference_genome in rgc.list():
        new_genome_model = GenomeModel(genome_alias=reference_genome, refgenomeconf=rgc)
        all_genome_models.append(new_genome_model)

    # Get BED files
    # for now, hard code a couple

    # all_bed_files = [
    #     "/home/drc/GITHUB/bedboss/bedboss/test/data/bed/hg19/correct/hg19_example1.bed"
    # ]
    all_bed_files = []
    for root, dirs, files in os.walk(
        "/home/drc/GITHUB/bedboss/bedboss/scripts/ref_genome_validating/results"
    ):
        for file in files:
            if file.endswith(".bed"):
                # print(os.path.join(root, file))
                all_bed_files.append(os.path.join(root, file))

    # validate each Bed file
    validator = Validator(genome_models=all_genome_models)

    for bedfile in all_bed_files[:20]:
        compat_vector = validator.determine_compatibility(bedfile)

        # Debug printing
        import pprint

        pprint.pprint(compat_vector, depth=5)

    return


if __name__ == "__main__":
    main()
