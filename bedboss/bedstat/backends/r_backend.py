import json
import logging
import os
import statistics
from pathlib import Path
from typing import Union

import pypiper
from gtars.models import RegionSet

from bedboss.bedstat.backends.base import StatBackend
from bedboss.bedstat.gc_content import calculate_gc_content, create_gc_plot
from bedboss.bedstat.r_service import RServiceManager
from bedboss.const import BEDSTAT_OUTPUT, OUTPUT_FOLDER_NAME
from bedboss.exceptions import BedBossException

_LOGGER = logging.getLogger("bedboss")


class RStatBackend(StatBackend):
    """R-based statistics backend using regionstat.R."""

    def __init__(self, r_service: RServiceManager = None, **kwargs):
        self._r_service = r_service

    @property
    def r_service(self) -> RServiceManager:
        return self._r_service

    @r_service.setter
    def r_service(self, value: RServiceManager):
        self._r_service = value

    def compute(
        self,
        bedfile: str,
        genome: str,
        outfolder: str,
        bed_digest: str = None,
        ensdb: str = None,
        open_signal_matrix: str = None,
        just_db_commit: bool = False,
        rfg_config: Union[str, Path] = None,
        pm: pypiper.PipelineManager = None,
    ) -> dict:
        outfolder_stats = os.path.join(outfolder, OUTPUT_FOLDER_NAME, BEDSTAT_OUTPUT)
        os.makedirs(outfolder_stats, exist_ok=True)

        bed_object = RegionSet(bedfile)
        if not bed_digest:
            bed_digest = bed_object.identifier

        outfolder_stats_results = os.path.abspath(
            os.path.join(outfolder_stats, bed_digest)
        )
        os.makedirs(outfolder_stats_results, exist_ok=True)

        json_file_path = os.path.abspath(
            os.path.join(outfolder_stats_results, bed_digest + ".json")
        )
        json_plots_file_path = os.path.abspath(
            os.path.join(outfolder_stats_results, bed_digest + "_plots.json")
        )

        # Used to stop pipeline if bedstat is used independently
        stop_pipeline = not pm

        if not just_db_commit:
            if not pm:
                pm_out_path = os.path.abspath(
                    os.path.join(outfolder_stats, "pypiper", bed_digest)
                )
                os.makedirs(pm_out_path, exist_ok=True)
                pm = pypiper.PipelineManager(
                    name="bedstat-pipeline",
                    outfolder=pm_out_path,
                    pipestat_sample_name=bed_digest,
                )

            rscript_path = os.path.join(
                os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                "tools",
                "regionstat_cli.R",
            )
            assert os.path.exists(rscript_path), FileNotFoundError(
                f"'{rscript_path}' script not found"
            )

            if not self._r_service:
                try:
                    _LOGGER.info("#=>>> Running local R instance!")
                    command = (
                        f"Rscript {rscript_path} --bedfilePath={bedfile} "
                        f"--openSignalMatrix={open_signal_matrix} "
                        f"--outputFolder={outfolder_stats_results} --genome={genome} "
                        f"--ensdb={ensdb} --digest={bed_digest}"
                    )
                    pm.run(cmd=command, target=json_file_path)
                except Exception as e:
                    _LOGGER.error(f"Pipeline failed: {e}")
                    raise BedBossException(f"Pipeline failed: {e}")
            else:
                _LOGGER.info("#=>>> Running R service ")
                self._r_service.run_file(
                    file_path=bedfile,
                    digest=bed_digest,
                    outpath=outfolder_stats_results,
                    genome=genome,
                    openSignalMatrix=open_signal_matrix,
                    gtffile=ensdb,
                )

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

        try:
            gc_contents = calculate_gc_content(
                bedfile=bed_object, genome=genome, rfg_config=rfg_config
            )
        except BaseException:
            gc_contents = None

        if gc_contents:
            gc_mean = statistics.mean(gc_contents)
            data["gc_content"] = round(gc_mean, 2)
            gc_plot = create_gc_plot(
                bed_id=bed_digest,
                gc_contents=gc_contents,
                outfolder=os.path.join(outfolder_stats_results),
                gc_mean=gc_mean,
            )
            plots.append(gc_plot)

        for plot in plots:
            plot_id = plot["name"]
            data.update({plot_id: plot})

        if "md5sum" in data:
            del data["md5sum"]
        if "name" in data:
            del data["name"]

        if stop_pipeline:
            pm.stop_pipeline()

        return data

    def cleanup(self):
        if self._r_service:
            self._r_service.terminate_service()
