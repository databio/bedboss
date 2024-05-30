import gzip
import logging
import os
import shutil

import pipestat
import pypiper
from typing import Optional

from bedboss.bedclassifier import get_bed_type
from bedboss.exceptions import BedTypeException

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
        # Use existing Pipeline Manager or Construct New one
        # Want to use Pipeline Manager to log work AND cleanup unzipped gz files.
        self.pm = pm
        # if pm is not None:
        #     self.pm = pm
        #     self.pm_created = False
        # else:
        #     self.logs_dir = os.path.join(self.output_dir, "logs")
        #     self.pm = pypiper.PipelineManager(
        #         name="bedclassifier",
        #         outfolder=self.logs_dir,
        #         recover=True,
        #         pipestat_sample_name=bed_digest,
        #     )
        #     self.pm.start_pipeline()
        #     self.pm_created = True

        if psm is None:
            pephuburl = "donaldcampbelljr/bedclassifier_tuning_geo:default"
            self.psm = pipestat.PipestatManager(
                pephub_path=pephuburl, schema_path="bedclassifier_output_schema.yaml"
            )
            # create piepstat manager
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
            self.bed_type, self.bed_type_named = get_bed_type(self.input_file)
        except BedTypeException as e:
            _LOGGER.warning(msg=f"FAILED {bed_digest}  Exception {e}")
            self.bed_type = "unknown_bedtype"
            self.bed_type_named = "unknown_bedtype"

        # return f"bed{bedtype}+{n}", bed_type_named

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
            all_values.update({"bedfile_type": self.bed_type})
        if self.bed_type_named:
            all_values.update({"bedfile_named": self.bed_type_named})
        if self.gsm:
            all_values.update({"gsm": self.gsm})

        all_values.update({"types_match": do_types_match})

        try:
            psm.report(record_identifier=bed_digest, values=all_values)
            # psm.set_status(record_identifier=bed_digest, status_identifier="completed")
        except Exception as e:
            _LOGGER.warning(msg=f"FAILED {bed_digest}  Exception {e}")
            # psm.set_status(record_identifier=bed_digest, status_identifier="failed")

        # self.pm.report_result(key="bedtype", value=self.bed_type)

        if self.pm:
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

    logs_dir = os.path.join(os.path.abspath("results"), "logs")
    pm = pypiper.PipelineManager(
        name="bedclassifier",
        outfolder=logs_dir,
        recover=True,
    )
    pm.start_pipeline()

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

    pephuburl = "donaldcampbelljr/bedclassifier_tuning_geo:default"
    psm = pipestat.PipestatManager(
        pephub_path=pephuburl, schema_path="bedclassifier_output_schema.yaml"
    )

    for sample in samples:
        if isinstance(sample.output_file_path, list):
            bedfile = sample.output_file_path[0]
        else:
            bedfile = sample.output_file_path
        geo_accession = sample.sample_geo_accession
        sample_name = sample.sample_name
        bed_type_from_geo = sample.type.lower()

        bed = BedClassifier(
            input_file=bedfile,
            bed_digest=sample_name,  # TODO FIX THIS IT HOULD BE AN ACTUAL DIGEST
            output_dir=os.path.abspath("results"),
            input_type=bed_type_from_geo,
            psm=psm,
            pm=pm,
            gsm=geo_accession,
        )

    pm.stop_pipeline()


if __name__ == "__main__":
    main()
