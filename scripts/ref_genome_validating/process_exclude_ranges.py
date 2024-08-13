# Ensure excluded ranges are downloaded and cached
# after building excluded ranges cache and an IGD database
# look at overlaps amongst the bed files
import argparse
import os
from geofetch import Geofetcher

IGD_DB_PATH = "/home/drc/GITHUB/bedboss/bedboss/scripts/ref_genome_validating/data/excludedranges/igd/EXCLUDED_RANGES_IGD_DATABASE.igd"
IGD_TSV = "/home/drc/GITHUB/bedboss/bedboss/scripts/ref_genome_validating/data/excludedranges/igd/EXCLUDED_RANGES_IGD_DATABASE_index.tsv"


def main(species):
    if not filter or not species:
        print("Must supply species,e.g. mouse, homosapiens, rat, cow!")

    else:
        print("Hello World")
        # Make sure to have the IDE ignore these folders!!!!

        data_output_path = os.path.abspath("data")
        results_path = os.path.abspath("results")
        logs_dir = os.path.join(results_path, "logs")

        species_output_path = os.path.join(data_output_path, species)

        # Note this assumes you've downloaed and cached species relevant bedfiles already and they are located in "bedfileslist.txt" under each species folder
        samples = get_samples(data_output_path=species_output_path)

        for sample in samples:
            if isinstance(sample.output_file_path, list):
                bedfile = sample.output_file_path[0]
            else:
                bedfile = sample.output_file_path

            geo_accession = sample.sample_geo_accession
            sample_name = sample.sample_name
            bed_type_from_geo = sample.type.lower()
            reported_ref_genome = sample.ref_genome
            reported_genome_build = sample.genome_build
            reported_organism = sample.sample_organism_ch1

            print(sample)

    pass


def get_samples(data_output_path):
    geofetcher_obj = Geofetcher(
        filter_size="25MB",
        data_source="samples",
        geo_folder=data_output_path,
        metadata_folder=data_output_path,
        processed=True,
        max_soft_size="20MB",
        discard_soft=True,
    )

    geofetched = geofetcher_obj.get_projects(
        input=os.path.join(data_output_path, "bedfileslist.txt"), just_metadata=True
    )

    samples = geofetched["bedfileslist_samples"].samples

    return samples


def get_overlaps():
    # Open bed file

    # compare regions

    # report output to results file (PEPHub?)

    pass


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Calculate overlaps")
    parser.add_argument(
        "-s",
        "--species",
        type=str,
        required=True,
        help="species: homosapiens, mouse, rat, cow",
    )
    args = parser.parse_args()
    main(args.species)
