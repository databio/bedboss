# Ensure excluded ranges are downloaded and cached
# after building excluded ranges cache and an IGD database
# look at overlaps amongst the bed files
import argparse
import gzip
import os
import shutil
import subprocess
import pipestat

from geofetch import Geofetcher

# Note many values are hardcoded for now
# assumes user has built the igd database and has igd(c) installed locally
IGD_DB_PATH = "/home/drc/Downloads/igd_database.igd"
IGD_TSV = "/home/drc/Downloads/igd_database_index.tsv"

MAX_SAMPLES = 500

# PEP_URL = "donaldcampbelljr/excluded_ranges_species:default"
PEP_URL = "donaldcampbelljr/excluded_ranges_species_exp2:default"


def main(species):
    if not filter or not species:
        print("Must supply species,e.g. mouse, homosapiens, rat, cow!")

    else:
        # print("Hello World")
        # Make sure to have the IDE ignore these folders!!!!

        data_output_path = os.path.abspath("data")
        results_path = os.path.abspath("results")
        logs_dir = os.path.join(results_path, "logs")

        species_output_path = os.path.join(data_output_path, species)

        # bedfilepath = '/home/drc/IGD_TEST_2/query_bed_file/igd_query_test.bed'
        # command = f"/home/drc/GITHUB/igd/IGD/bin/igd search {IGD_DB_PATH} -q {bedfilepath}"
        # returned_stdout = run_igd(command)
        # print(returned_stdout)
        # #
        # data = parse_output(returned_stdout)
        #
        # print(data)
        # Note this assumes you've downloaed and cached species relevant bedfiles already and they are located in "bedfileslist.txt" under each species folder

        psm = pipestat.PipestatManager(
            pephub_path=PEP_URL,
        )

        bedfileslist = os.path.join(species_output_path, "bedfileslist.txt")

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
                        all_values = {}
                        if isinstance(sample.output_file_path, list):
                            bedfile = sample.output_file_path[0]
                        else:
                            bedfile = sample.output_file_path

                        print(f"Processing bedfile {bedfile}")

                        all_values.update({"file_name": os.path.basename(bedfile)})

                        gse = getattr(sample, "gse", None)
                        all_values.update({"gse": gse})

                        geo_accession = getattr(sample, "sample_geo_accession", None)
                        all_values.update({"geo_accession": geo_accession})

                        sample_name = getattr(sample, "sample_name", None)
                        all_values.update({"sample_name": sample_name})

                        bed_type_from_geo = getattr(sample, "type", None)
                        if bed_type_from_geo:
                            bed_type_from_geo = bed_type_from_geo.lower()
                        all_values.update({"bed_type_from_geo": bed_type_from_geo})

                        reported_ref_genome = getattr(sample, "ref_genome", None)
                        all_values.update({"reported_ref_genome": reported_ref_genome})

                        reported_genome_build = getattr(sample, "genome_build", None)
                        all_values.update(
                            {"reported_genome_build": reported_genome_build}
                        )

                        reported_organism = getattr(
                            sample, "sample_organism_ch1", None
                        )  # sample_organism_ch1
                        all_values.update({"reported_organism": reported_organism})

                        reported_assembly = getattr(
                            sample, "assembly", None
                        )  # sample_organism_ch1
                        all_values.update({"assembly": reported_organism})

                        # process bedfile
                        file = unzip_bedfile(bedfile, results_path)

                        if file:
                            # Count regions
                            with open(file, "r") as f:
                                region_count = len(f.readlines())

                            all_values.update({"bed_region_count": region_count})

                            command = f"/home/drc/GITHUB/igd/IGD/bin/igd search {IGD_DB_PATH} -q {file}"
                            returned_stdout = run_igd(command)
                            # print(returned_stdout)
                            #
                            if returned_stdout:
                                data = parse_output(returned_stdout)
                                print("IGD Results: \n")
                                print(returned_stdout)
                                if data:
                                    for datum in data:
                                        if (
                                            "file_name" in datum
                                            and "number_of_hits" in datum
                                        ):
                                            all_values.update(
                                                {
                                                    datum["file_name"]: datum[
                                                        "number_of_hits"
                                                    ]
                                                }
                                            )
                                else:
                                    continue
                            else:
                                continue
                        else:
                            # just skip reporting if not the right file type
                            continue
                        # print("Reporting values")
                        psm.report(record_identifier=sample_name, values=all_values)

    pass


def unzip_bedfile(input_file, output_dir):
    abs_bed_path = os.path.abspath(input_file)
    file_name = os.path.splitext(os.path.basename(abs_bed_path))[0]
    file_extension = os.path.splitext(abs_bed_path)[-1]

    # we need this only if unzipping a file
    output_dir = output_dir or os.path.join(
        os.path.dirname(abs_bed_path), "temp_processing"
    )
    if file_extension == ".gz":
        unzipped_input_file = os.path.join(output_dir, file_name)

        with gzip.open(input_file, "rb") as f_in:
            with open(unzipped_input_file, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)
        input_file = unzipped_input_file
        return input_file
    elif file_extension == ".bed":
        return input_file
    else:
        return None


def parse_output(output_str):
    """
    Parses IGD output into a list of dicts
    Args:
      output_str: The output string from IGD

    Returns:
      A list of dictionaries, where each dictionary represents a record.
    """

    try:
        lines = output_str.splitlines()
        data = []
        for line in lines:
            if line.startswith("index"):
                continue  # Skip the header line
            elif line.startswith("Total"):
                break  # Stop parsing after the "Total" line
            else:
                fields = line.split()
                record = {
                    "index": int(fields[0]),
                    "number_of_regions": int(fields[1]),
                    "number_of_hits": int(fields[2]),
                    "file_name": fields[3],
                }
                data.append(record)
        return data
    except Exception:
        return None


def run_igd(command):
    """Run IGD, this is a temp workaround until Rust python bindings are finished."""
    result = subprocess.run(command, shell=True, capture_output=True, text=True)
    if result.returncode == 0:
        return result.stdout
    else:
        print(f"Error running command: {result.stderr}")
        return None


def get_samples(data_output_path, gse_number):
    geofetcher_obj = Geofetcher(
        filter="\.(bed)\.",
        filter_size="25MB",
        data_source="samples",
        geo_folder=data_output_path,
        metadata_folder=data_output_path,
        processed=True,
        max_soft_size="20MB",
        discard_soft=True,
    )

    try:
        geofetched = geofetcher_obj.get_projects(input=gse_number, just_metadata=False)
    except Exception:
        return None

    if geofetched:
        if geofetched != {}:
            key = gse_number + "_samples"
            samples = geofetched[key].samples
            return samples

    return None


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
