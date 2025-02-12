import gzip
import logging
import os
import shutil
from typing import Optional

import pipestat
import pypiper

from bedboss.bedclassifier.bedclassifier import get_bed_classification
from bedboss.exceptions import BedTypeException

_LOGGER = logging.getLogger("bedboss")

from geofetch import Finder, Geofetcher


class BedClassifier:
    """
    This will take the input of either a .bed or a .bed.gz and classify the type of BED file.

    """

    def __init__(
        self,
        input_file: str,
        output_dir: Optional[str] = None,
        bed_digest: Optional[str] = None,
        input_type: Optional[str] = None,
        pm: pypiper.PipelineManager = None,
        report_to_database: Optional[bool] = False,
        psm: pipestat.PipestatManager = None,
        gsm: str = None,
    ):
        # Raise Exception if input_type is given and it is NOT a BED file
        # Raise Exception if the input file cannot be resolved

        self.gsm = gsm
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
        # Use existing Pipeline Manager if it exists
        self.pm = pm

        if psm is None:
            pephuburl = "donaldcampbelljr/bedclassifier_tuning_geo:default"
            self.psm = pipestat.PipestatManager(
                pephub_path=pephuburl, schema_path="bedclassifier_output_schema.yaml"
            )
        else:
            self.psm = psm

        if self.file_extension == ".gz":
            unzipped_input_file = os.path.join(self.output_dir, self.file_name)

            with gzip.open(self.input_file, "rb") as f_in:
                _LOGGER.info(
                    f"Unzipping file:{self.input_file} and Creating Unzipped file: {unzipped_input_file}"
                )
                with open(unzipped_input_file, "wb") as f_out:
                    shutil.copyfileobj(f_in, f_out)
            self.input_file = unzipped_input_file
            if self.pm:
                self.pm.clean_add(unzipped_input_file)

        try:
            self.bed_type, self.bed_type_named = get_bed_classification(self.input_file)
        except BedTypeException as e:
            _LOGGER.warning(msg=f"FAILED {bed_digest}  Exception {e}")
            self.bed_type = "unknown_bedtype"
            self.bed_type_named = "unknown_bedtype"

        if self.input_type is not None:
            if self.bed_type_named != self.input_type:
                _LOGGER.warning(
                    f"BED file classified as different type than given input: {self.bed_type} vs {self.input_type}"
                )
                do_types_match = False
            else:
                do_types_match = True
        else:
            do_types_match = False

        # Create Value Dict to report via pipestat

        all_values = {}

        if self.input_type:
            all_values.update({"given_bedfile_type": self.input_type})
        if self.bed_type:
            all_values.update({"bed_type": self.bed_type})
        if self.bed_type_named:
            all_values.update({"bed_format": self.bed_type_named})
        if self.gsm:
            all_values.update({"gsm": self.gsm})

        all_values.update({"types_match": do_types_match})

        try:
            psm.report(record_identifier=bed_digest, values=all_values)
        except Exception as e:
            _LOGGER.warning(msg=f"FAILED {bed_digest}  Exception {e}")

        if self.pm:
            self.pm.stop_pipeline()


def main():
    # PEP for reporting all classification results
    #pephuburl = "donaldcampbelljr/bedclassifier_tuning_geo:default"

    # Place these external to pycharm folder!!!
    # data_output_path = os.path.abspath("data")
    # results_path = os.path.abspath("results")
    # logs_dir = os.path.join(results_path, "logs")
    # data_output_path = os.path.abspath("/home/drc/test/test_gappedPeaks_geofetched/data/")
    # results_path = os.path.abspath("/home/drc/test/test_gappedPeaks_geofetched/results/")
    # logs_dir = os.path.abspath("/home/drc/test/test_gappedPeaks_geofetched/results/logs")

    data_output_path = os.path.abspath("/home/drc/test/test_other_types/data/")
    results_path = os.path.abspath("/home/drc/test/test_other_types/results/")
    logs_dir = os.path.abspath("/home/drc/test/test_other_types/results/logs")

    gse_obj = Finder()

    # # Optionally: provide filter string and max number of retrieve elements
    gse_obj = Finder(filters="peptideMapping", retmax=1000)
    #
    gse_list = gse_obj.get_gse_all()
    gse_obj.generate_file(os.path.join(data_output_path, "output.txt"), gse_list=gse_list)

    pm = pypiper.PipelineManager(
        name="bedclassifier",
        outfolder=logs_dir,
        recover=True,
    )

    pm.start_pipeline()

    # for geo in gse_list:
    geofetcher_obj = Geofetcher(
        filter="\.(peptideMapping)\.",
        filter_size="100MB",
        data_source="samples",
        geo_folder=data_output_path,
        metadata_folder=data_output_path,
        processed=True,
        max_soft_size="20MB",
        discard_soft=True,
    )

    # geofetcher_obj.fetch_all(input="data/output.txt", name="donald_test")
    geofetched = geofetcher_obj.get_projects(
        input=os.path.join(data_output_path, "output.txt"), just_metadata=False
    )

    samples = geofetched["output_samples"].samples

    psm = pipestat.PipestatManager(
        results_file_path="/home/drc/test/test_other_types/results/other_types_results.yaml",
    )

    for sample in samples:
        if isinstance(sample.output_file_path, list):
            bedfile = sample.output_file_path[0]
        else:
            bedfile = sample.output_file_path
        geo_accession = sample.sample_geo_accession
        sample_name = sample.sample_name
        bed_type_from_geo = sample.type.lower()

        try:
            bed = BedClassifier(
                input_file=bedfile,
                bed_digest=sample_name,  # TODO FIX THIS IT SHOULD BE AN ACTUAL DIGEST
                output_dir=results_path,
                input_type=bed_type_from_geo,
                psm=psm,
                pm=pm,
                gsm=geo_accession,
            )
        except:
            pass


    pm.stop_pipeline()


if __name__ == "__main__":
    main()
