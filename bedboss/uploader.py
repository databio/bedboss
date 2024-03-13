# This file contains uploading scripts for s3, postgres, qdrant, and pephub
from typing import Union
import logging
import os

import geniml.bbclient
from bbconf import BedBaseConf
from geniml.io.io import RegionSet
import boto3
import botocore
from rich.progress import track
from geniml.bbclient.const import DEFALUT_BUCKET_NAME

from pephubclient import PEPHubClient
from ubiquerg import parse_registry_path
from pephubclient.helpers import is_registry_path

from bedboss.models import BedMetadata

_LOGGER = logging.getLogger("bedboss")


class BedBossUploader:
    def __init__(self, bedbase_config: Union[str, BedBaseConf]):
        if isinstance(bedbase_config, str):
            self.bedbase_config = BedBaseConf(bedbase_config)
        else:
            self.bedbase_config = bedbase_config

        self.bbclient = geniml.bbclient.BBClient()

    def upload_bedbase(
        self,
        sample_name: str,
        results: dict,
        force: bool = False,
    ) -> None:
        """
        Upload bedbase data to the bedbase database

        :param sample_name: name of the sample (digest)
        :param results: dictionary with the results
        :param force: force the upload

        :return: None
        """

        self.bedbase_config.bed.report(
            record_identifier=sample_name,
            values=results,
            force_overwrite=force,
        )

    @staticmethod
    def upload_pephub(
        pep_registry_path: str, bed_digest: str, genome: str, metadata: dict
    ) -> bool:
        """
        Load bedfile and metadata to PEPHUB

        :param str pep_registry_path: registry path to pep on pephub
        :param str bed_digest: unique bedfile identifier
        :param str genome: genome associated with bedfile
        :param dict metadata: Any other metadata that has been collected

        :return: true if successful, false if not
        """

        if is_registry_path(pep_registry_path):
            parsed_pep_dict = parse_registry_path(pep_registry_path)

            # Combine data into a dict for sending to pephub
            sample_data = {}
            sample_data.update({"sample_name": bed_digest, "genome": genome})

            metadata = BedMetadata(**metadata).model_dump()

            for key, value in metadata.items():
                # TODO: Confirm this key is in the schema
                # Then update sample_data
                sample_data.update({key: value})

            try:
                PEPHubClient().sample.create(
                    namespace=parsed_pep_dict["namespace"],
                    name=parsed_pep_dict["item"],
                    tag=parsed_pep_dict["tag"],
                    sample_name=bed_digest,
                    overwrite=True,
                    sample_dict=sample_data,
                )

            except Exception as e:  # Need more specific exception
                _LOGGER.error(f"Failed to upload BEDFILE to PEPhub: See {e}")
                return False
        else:
            _LOGGER.error(f"{pep_registry_path} is not a valid registry path")
            return False
        return True

    def upload_qdrant(self, identifier: str) -> bool:
        """
        Index bed file to qdrant

        :param identifier: bed file identifier or RegionSet object
        :return: true if successful, false if not
        """

        self.bedbase_config.add_bed_to_qdrant(
            bed_id=identifier,
            bed_file=self.bbclient.seek(identifier),
            payload={"digest": identifier},
        )
        return True

    def upload_s3(
        self,
        identifier: str,
        results: dict,
        local_path: str = None,
        bucket: str = DEFALUT_BUCKET_NAME,
        endpoint_url: str = None,
        aws_access_key_id: str = None,
        aws_secret_access_key: str = None,
    ) -> dict:
        """
        Upload file to s3

        :param identifier: the unique identifier of the BED file
        :param results: the dictionary with the results
        :param local_path: the local path to the output files
        :param bucket: the name of the bucket
        :param endpoint_url: the URL of the S3 endpoint [Default: set up by the environment vars]
        :param aws_access_key_id: the access key of the AWS account [Default: set up by the environment vars]
        :param aws_secret_access_key: the secret access key of the AWS account [Default: set up by the environment vars]

        :return: the dictionary with the results
        """
        _LOGGER.info(f"Uploading '{identifier}' data to S3 ...")

        s3_client = boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
        )

        return_dict = {}

        bed_path_s3 = self.bbclient.add_bed_to_s3(
            identifier=identifier,
            endpoint_url=endpoint_url,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
        )

        return_dict["bed_file"] = bed_path_s3

        upload_dict = self._parse_results_to_s3(results, local_path=local_path)

        # TODO: Add rich logging
        for key, value in upload_dict.items():
            s3_client.upload_file(value["local_path"], bucket, value["s3_path"])
            return_dict[key] = value["s3_path"]

        return return_dict

    @staticmethod
    def _parse_results_to_s3(results: dict, local_path: str = None) -> dict:
        """
        Parse the results to the format suitable for s3

        :param results: the dictionary with the results (the output of the bedstat pipeline)
        :param local_path: the local path to the files

        :return: the dictionary with the results
        """
        # TODO: make this list automatic:
        STAT_FILES_KEYS = [
            "open_chromatin",
            "neighbor_distances",
            "widths_histogram",
            "cumulative_partitions",
            "expected_partitions",
            "partitions",
            "gccontent",
            "chrombins",
            "tssdist",
            "bigbedfile",
        ]

        return_dict = {}

        for key, value in results.items():
            if key in STAT_FILES_KEYS:
                if not isinstance(value, dict):
                    _LOGGER.warning(
                        f"Error while parsing return value. Value for key '{key}' is not a dictionary. Skipping..."
                    )
                    continue

                for item, path in value.items():
                    if item == "path":
                        return_dict[f"{key}_pdf"] = {
                            "local_path": os.path.join(local_path, path),
                            "s3_path": path,
                        }
                    if item == "thumbnail_path":
                        return_dict[f"{key}_png"] = {
                            "local_path": os.path.join(local_path, path),
                            "s3_path": path,
                        }
        return return_dict
