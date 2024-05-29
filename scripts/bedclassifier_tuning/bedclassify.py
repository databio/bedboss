import gzip
import logging
import os
import shutil
import pypiper
from typing import Optional

from bedboss.bedclassifier import get_bed_type

_LOGGER = logging.getLogger("bedboss")

from geofetch import Finder, Geofetcher


class BedClassifier:
    """
    This will take the input of either a .bed or a .bed.gz and classify the type of BED file.

    Types:
    BED, BED2 - BED12, narrowPeak, broadPeak
    UnknownType

    """

    def __init__(
        self,
        input_file: str,
        output_dir: Optional[str] = None,
        bed_digest: Optional[str] = None,
        input_type: Optional[str] = None,
        pm: pypiper.PipelineManager = None,
        report_to_database: Optional[bool] = False,
    ):
        # Raise Exception if input_type is given and it is NOT a BED file
        # Raise Exception if the input file cannot be resolved
        self.input_file = input_file
        self.bed_digest = bed_digest
        self.input_type = input_type

        self.abs_bed_path = os.path.abspath(self.input_file)
        self.file_name = os.path.splitext(os.path.basename(self.abs_bed_path))[0]
        self.file_extension = os.path.splitext(self.abs_bed_path)[-1]

        # we need this only if unzipping a file
        self.output_dir = output_dir or os.path.join(
            os.path.dirname(self.abs_bed_path), "temp_processing"
        )
        # Use existing Pipeline Manager or Construct New one
        # Want to use Pipeline Manager to log work AND cleanup unzipped gz files.
        if pm is not None:
            self.pm = pm
            self.pm_created = False
        else:
            self.logs_dir = os.path.join(self.output_dir, "logs")
            self.pm = pypiper.PipelineManager(
                name="bedclassifier",
                outfolder=self.logs_dir,
                recover=True,
                pipestat_sample_name=bed_digest,
            )
            self.pm.start_pipeline()
            self.pm_created = True

        if self.file_extension == ".gz":
            unzipped_input_file = os.path.join(self.output_dir, self.file_name)

            with gzip.open(self.input_file, "rb") as f_in:
                _LOGGER.info(
                    f"Unzipping file:{self.input_file} and Creating Unzipped file: {unzipped_input_file}"
                )
                with open(unzipped_input_file, "wb") as f_out:
                    shutil.copyfileobj(f_in, f_out)
            self.input_file = unzipped_input_file
            self.pm.clean_add(unzipped_input_file)

        self.bed_type = get_bed_type(self.input_file)

        if self.input_type is not None:
            if self.bed_type != self.input_type:
                _LOGGER.warning(
                    f"BED file classified as different type than given input: {self.bed_type} vs {self.input_type}"
                )

        self.pm.report_result(key="bedtype", value=self.bed_type)

        if self.pm_created is True:
            self.pm.stop_pipeline()


def main():
    data_output_path = os.path.abspath("data")

    from geofetch import Finder

    gse_obj = Finder()

    # # Optionally: provide filter string and max number of retrieve elements
    # gse_obj = Finder(filters="narrowpeak", retmax=100)
    #
    # gse_list = gse_obj.get_gse_all()
    # gse_obj.generate_file("data/output.txt", gse_list=gse_list)

    # for geo in gse_list:
    geofetcher_obj = Geofetcher(
        filter="\.(bed|narrowPeak|broadPeak)\.",
        filter_size="25MB",
        data_source="samples",
        geo_folder=os.path.abspath("data"),
        metadata_folder=os.path.abspath("data"),
        processed=True,
        max_soft_size="20MB",
        discard_soft=True,
    )

    # geofetcher_obj.fetch_all(input="data/output.txt", name="donald_test")
    geofetched = geofetcher_obj.get_projects(
        input="data/output.txt", just_metadata=False
    )
    print(geofetched)

    samples = geofetched["output_samples"].samples

    print(samples)

    for sample in samples:
        bedfile = sample.output_file_path[0]
        geo_accession = sample.sample_geo_accession

    # bed = BedClassifier(
    #     input_file="/home/drc/GITHUB/bedboss/bedboss/test/data/bed/simpleexamples/bed1.bed",
    #     bed_digest="bed1.bed",
    #     output_dir=os.path.abspath("results")
    #
    #
    # )

    # Get list of Bed Files and Download them

    # Open Bed Files, Classify them, Report them.


if __name__ == "__main__":
    main()
