from abc import ABC, abstractmethod
from pathlib import Path
from typing import Union

import pypiper


class StatBackend(ABC):
    """Interface for BED file statistics computation backends."""

    @abstractmethod
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
        """Compute statistics for a single BED file.

        Returns a dict with at minimum scalar keys matching BedStatsModel
        field names. May also include plot dicts and/or a 'distributions' key.
        """
        ...

    def cleanup(self):
        """Release any resources held by this backend."""
        pass
