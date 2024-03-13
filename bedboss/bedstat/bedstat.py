from typing import Union
import json
import os
import requests
import pypiper
import logging
from geniml.io import RegionSet


from bedboss.const import (
    OUTPUT_FOLDER_NAME,
    BEDSTAT_OUTPUT,
    OS_HG19,
    OS_HG38,
    OS_MM10,
    HOME_PATH,
    OPEN_SIGNAL_FOLDER_NAME,
    OPEN_SIGNAL_URL,
)
from bedboss.utils import download_file, convert_unit
from bedboss.exceptions import OpenSignalMatrixException


_LOGGER = logging.getLogger("bedboss")

SCHEMA_PATH_BEDSTAT = os.path.join(
    os.path.dirname(os.path.realpath(__file__)), "pep_schema.yaml"
)


def get_osm_path(genome: str, out_path: str = None) -> Union[str, None]:
    """
    By providing genome name download Open Signal Matrix

    :param genome: genome assembly
    :param out_path: working directory, where osm should be saved. If None, current working directory will be used
    :return: path to the Open Signal Matrix
    """
    # TODO: add more osm
    _LOGGER.info("Getting Open Signal Matrix file path...")
    if genome == "hg19" or genome == "GRCh37":
        osm_name = OS_HG19
    elif genome == "hg38" or genome == "GRCh38":
        osm_name = OS_HG38
    elif genome == "mm10" or genome == "GRCm38":
        osm_name = OS_MM10
    else:
        raise OpenSignalMatrixException(
            "For this genome open Signal Matrix was not found."
        )
    if not out_path:
        osm_folder = os.path.join(HOME_PATH, OPEN_SIGNAL_FOLDER_NAME)
    else:
        osm_folder = os.path.join(out_path, OPEN_SIGNAL_FOLDER_NAME)

    osm_path = os.path.join(osm_folder, osm_name)
    if not os.path.exists(osm_path):
        os.makedirs(osm_folder, exist_ok=True)
        download_file(
            url=f"{OPEN_SIGNAL_URL}{osm_name}",
            path=osm_path,
            no_fail=True,
        )
    return osm_path


def bedstat(
    bedfile: str,
    genome: str,
    outfolder: str,
    bed_digest: str = None,
    bigbed: str = None,
    ensdb: str = None,
    open_signal_matrix: str = None,
    just_db_commit: bool = False,
    pm: pypiper.PipelineManager = None,
) -> dict:
    """
    Run bedstat pipeline - pipeline for obtaining statistics about bed files
        and inserting them into the database

    :param str bedfile: the full path to the bed file to process
    :param str bigbed: the full path to the bigbed file. Defaults to None.
        (bigbed won't be created and some producing of some statistics will
        be skipped.)
    :param str bed_digest: the digest of the bed file. Defaults to None.
    :param str open_signal_matrix: a full path to the openSignalMatrix
        required for the tissue specificity plots
    :param str outfolder: The folder for storing the pipeline results.
    :param str genome: genome assembly of the sample
    :param str ensdb: a full path to the ensdb gtf file required for genomes
        not in GDdata
    :param pm: pypiper object

    :return: dict with statistics and plots metadata
    """
    # TODO why are we no longer using bbconf to get the output path?
    # outfolder_stats = bbc.get_bedstat_output_path()

    outfolder_stats = os.path.join(outfolder, OUTPUT_FOLDER_NAME, BEDSTAT_OUTPUT)
    try:
        os.makedirs(outfolder_stats)
    except FileExistsError:
        pass

    # find/download open signal matrix
    if not open_signal_matrix or not os.path.exists(open_signal_matrix):
        try:
            open_signal_matrix = get_osm_path(genome)
        except OpenSignalMatrixException:
            _LOGGER.warning(
                f"Open Signal Matrix was not found for {genome}. Skipping..."
            )
            open_signal_matrix = None

    # Used to stop pipeline bedstat is used independently
    if not pm:
        stop_pipeline = True
    else:
        stop_pipeline = False

    if not bed_digest:
        bed_digest = RegionSet(bedfile).identifier
    bedfile_name = os.path.split(bedfile)[1]

    fileid = os.path.splitext(os.path.splitext(bedfile_name)[0])[0]
    outfolder_stats_results = os.path.abspath(os.path.join(outfolder_stats, bed_digest))
    try:
        os.makedirs(outfolder_stats_results)
    except FileExistsError:
        pass
    json_file_path = os.path.abspath(
        os.path.join(outfolder_stats_results, fileid + ".json")
    )
    json_plots_file_path = os.path.abspath(
        os.path.join(outfolder_stats_results, fileid + "_plots.json")
    )
    bed_relpath = os.path.relpath(
        bedfile,
        os.path.abspath(os.path.join(outfolder_stats, os.pardir, os.pardir)),
    )
    bigbed_relpath = os.path.relpath(
        os.path.join(bigbed, fileid + ".bigBed"),
        os.path.abspath(os.path.join(outfolder_stats, os.pardir, os.pardir)),
    )
    if not just_db_commit:
        if not pm:
            pm_out_path = os.path.abspath(
                os.path.join(outfolder_stats, "pypiper", bed_digest)
            )
            try:
                os.makedirs(pm_out_path)
            except FileExistsError:
                pass
            pm = pypiper.PipelineManager(
                name="bedstat-pipeline",
                outfolder=pm_out_path,
                pipestat_sample_name=bed_digest,
            )

        rscript_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "bedstat",
            "tools",
            "regionstat.R",
        )
        assert os.path.exists(rscript_path), FileNotFoundError(
            f"'{rscript_path}' script not found"
        )
        command = (
            f"Rscript {rscript_path} --bedfilePath={bedfile} "
            f"--fileId={fileid} --openSignalMatrix={open_signal_matrix} "
            f"--outputFolder={outfolder_stats_results} --genome={genome} "
            f"--ensdb={ensdb} --digest={bed_digest}"
        )

        pm.run(cmd=command, target=json_file_path)

    data = {}
    if os.path.exists(json_file_path):
        with open(json_file_path, "r", encoding="utf-8") as f:
            data = json.loads(f.read())
    if os.path.exists(json_plots_file_path):
        with open(json_plots_file_path, "r", encoding="utf-8") as f_plots:
            plots = json.loads(f_plots.read())
    else:
        plots = []

    # unlist the data, since the output of regionstat.R is a dict of lists of
    # length 1 and force keys to lower to correspond with the
    # postgres column identifiers
    data = {k.lower(): v[0] if isinstance(v, list) else v for k, v in data.items()}
    data.update(
        {
            "bedfile": {
                "path": bed_relpath,
                "size": convert_unit(os.path.getsize(bedfile)),
                "title": "Path to the BED file",
            }
        }
    )

    if os.path.exists(os.path.join(bigbed, fileid + ".bigBed")):
        data.update(
            {
                "bigbedfile": {
                    "path": bigbed_relpath,
                    "size": convert_unit(
                        os.path.getsize(os.path.join(bigbed, fileid + ".bigBed"))
                    ),
                    "title": "Path to the big BED file",
                }
            }
        )

        if not os.path.islink(os.path.join(bigbed, fileid + ".bigBed")):
            digest = requests.get(
                f"http://refgenomes.databio.org/genomes/genome_digest/{genome}"
            ).text.strip('""')

            data.update(
                {
                    "genome": {
                        "alias": genome,
                        "digest": digest,
                    }
                }
            )
    else:
        data.update(
            {
                "genome": {
                    "alias": genome,
                    "digest": "",
                }
            }
        )

    for plot in plots:
        plot_id = plot["name"]
        del plot["name"]
        data.update({plot_id: plot})

    # deleting md5sum, because it is record_identifier
    if "md5sum" in data:
        del data["md5sum"]

    if stop_pipeline:
        pm.stop_pipeline()

    return data
