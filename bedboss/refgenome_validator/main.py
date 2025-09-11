import logging
import os
from typing import Dict, List, Optional, Union

from gtars.models import Region as GRegionSet

from bedboss.exceptions import ValidatorException, BedBossException
from bedboss.refgenome_validator.const import GENOME_FILES
from bedboss.refgenome_validator.genome_model import GenomeModel
from bedboss.refgenome_validator.models import (
    ChromLengthStats,
    ChromNameStats,
    CompatibilityConcise,
    CompatibilityStats,
    RatingModel,
    SequenceFitStats,
)
from bedboss.refgenome_validator.utils import (
    get_bed_chrom_info,
    parse_IGD_output,
    run_igd_command,
)
from bedboss.refgenome_validator.refgenie_chrom_sizes import get_chrom_sizes

_LOGGER = logging.getLogger("bedboss")


class ReferenceValidator:
    """
    Primary class for creating a compatibility dict
    """

    def __init__(
        self,
        genome_models: Optional[List[GenomeModel]] = None,
        igd_path: Optional[str] = None,
    ):
        """
        Initialization method

        :param genome_models: this is a list of GenomeModels that will be checked against a bed file. Default: None
        :param igd_path: path to a local IGD file containing ALL excluded ranges intervals for IGD overlap assessment,
            if not provided these metrics are not computed. Default: None
        """

        if not genome_models:
            genome_models = self._build_default_models()
        elif isinstance(genome_models, str):
            genome_models = list(genome_models)
        elif not isinstance(genome_models, list):
            raise ValidatorException(
                reason="A list of GenomeModels must be provided to initialize the Validator class"
            )

        self.genome_models: List[GenomeModel] = genome_models
        self.igd_path = igd_path

    @staticmethod
    def calculate_chrom_stats(
        bed_chrom_sizes: dict, genome_chrom_sizes: dict
    ) -> CompatibilityStats:
        """
        Calculate overlap and sequence fit.
        Stats are associated with comparison of chrom names, chrom lengths, and sequence fits.

        :param bed_chrom_sizes: dict of a bedfile's chrom size
        :param genome_chrom_sizes: dict of a GenomeModel's chrom sizes

        :return: dictionary with information on Query vs Model, e.g. chrom names QueryvsModel
        """

        # Layer 1: Check names and Determine XS (Extra Sequences) via Calculation of Recall/Sensitivity
        # Q = Query, M = Model
        q_and_m = 0  # how many chrom names are in the query and the genome model?
        q_and_not_m = (
            0  # how many chrom names are in the query but not in the genome model?
        )
        not_q_and_m = (
            0  # how many chrom names are not in the query but are in the genome model?
        )
        passed_chrom_names = True  # does this bed file pass the first layer of testing?

        query_keys_present = []  # These keys are used for seq fit calculation
        for key in list(bed_chrom_sizes.keys()):
            if key not in genome_chrom_sizes:
                q_and_not_m += 1
            if key in genome_chrom_sizes:
                q_and_m += 1
                query_keys_present.append(key)

        for key in list(genome_chrom_sizes.keys()):
            if key not in bed_chrom_sizes:
                not_q_and_m += 1

        # Calculate the Jaccard Index for Chrom Names
        bed_chrom_set = set(list(bed_chrom_sizes.keys()))
        genome_chrom_set = set(list(genome_chrom_sizes.keys()))
        chrom_intersection = bed_chrom_set.intersection(genome_chrom_set)
        chrom_union = bed_chrom_set.union(chrom_intersection)
        chrom_jaccard_index = len(chrom_intersection) / len(chrom_union)

        # Alternative Method for Calculating Jaccard_index for binary classification
        # JI = TP/(TP+FP+FN)
        jaccard_binary = q_and_m / (q_and_m + not_q_and_m + q_and_not_m)

        # What is our threshold for passing layer 1?
        if q_and_not_m > 0:
            passed_chrom_names = False

        # Calculate sensitivity for chrom names
        # XS -> Extra Sequences
        sensitivity = q_and_m / (q_and_m + q_and_not_m)

        name_stats = ChromNameStats(
            xs=sensitivity,
            q_and_m=q_and_m,
            q_and_not_m=q_and_not_m,
            not_q_and_m=not_q_and_m,
            jaccard_index=chrom_jaccard_index,
            jaccard_index_binary=jaccard_binary,
            passed_chrom_names=passed_chrom_names,
        )

        # Layer 2:  Check Lengths, but only if layer 1 is passing [all chroms are in ref genome]
        if passed_chrom_names:
            chroms_beyond_range = False
            num_of_chrom_beyond = 0
            num_chrom_within_bounds = 0

            for key in list(bed_chrom_sizes.keys()):
                if key in genome_chrom_sizes:
                    if bed_chrom_sizes[key] > genome_chrom_sizes[key]:
                        num_of_chrom_beyond += 1
                        chroms_beyond_range = True
                    else:
                        num_chrom_within_bounds += 1

            # Calculate recall/sensitivity for chrom lengths
            # OOBR -> Out of Bounds Range
            sensitivity = num_chrom_within_bounds / (
                num_chrom_within_bounds + num_of_chrom_beyond
            )
            length_stats = ChromLengthStats(
                oobr=sensitivity,
                beyond_range=chroms_beyond_range,
                num_of_chrom_beyond=num_of_chrom_beyond,
                percentage_bed_chrom_beyond=(
                    100 * num_of_chrom_beyond / len(bed_chrom_set)
                ),
                percentage_genome_chrom_beyond=(
                    100 * num_of_chrom_beyond / len(genome_chrom_set)
                ),
            )

        else:
            length_stats = ChromLengthStats()

        # Layer 3 Calculate Sequence Fit if any query chrom names were present
        if len(query_keys_present) > 0:
            bed_sum = 0
            ref_genome_sum = 0
            for q_chr in query_keys_present:
                bed_sum += int(genome_chrom_sizes[q_chr])
            for g_chr in genome_chrom_sizes:
                ref_genome_sum += int(genome_chrom_sizes[g_chr])

            seq_fit_stats = SequenceFitStats(sequence_fit=bed_sum / ref_genome_sum)

        else:
            seq_fit_stats = SequenceFitStats(sequence_fit=None)

        return CompatibilityStats(
            chrom_name_stats=name_stats,
            chrom_length_stats=length_stats,
            chrom_sequence_fit_stats=seq_fit_stats,
        )

    def get_igd_overlaps(self, bedfile: str) -> Union[dict[str, dict], dict[str, None]]:
        """
        Third layer compatibility check.
        Run helper functions and execute an igd search query across an Integrated Genome Database.

        :param bedfile: path to the bedfile
        :return: dict of dicts containing keys (file names) and values (number of overlaps). Or if no overlaps are found,
        it returns an empty dict.

        # TODO: should be a pydantic model

        Currently for this function to work, the user must install the C version of IGD and have created a local igd file
        for the Excluded Ranges Bedset:
        https://github.com/databio/IGD
        https://bedbase.org/bedset/excluderanges

        """
        if not self.igd_path:
            return {"igd_stats": None}

        try:
            IGD_LOCATION = os.environ["IGD_LOCATION"]
        except:
            # Local installation of C version of IGD
            IGD_LOCATION = "/home/drc/GITHUB/igd/IGD/bin/igd"

        # Construct an IGD command to run as subprocess
        igd_command = IGD_LOCATION + f" search {self.igd_path} -q {bedfile}"

        returned_stdout = run_igd_command(igd_command)

        if not returned_stdout:
            return {"igd_stats": None}

        igd_overlap_data = parse_IGD_output(returned_stdout)

        if not igd_overlap_data:
            return {
                "igd_stats": {}
            }  # None tells us if the bed file never made it to layer 3 or perhaps igd errord, empty dict tells us that there were no overlaps found
        else:
            overlaps_dict = {}
            for datum in igd_overlap_data:
                if "file_name" in datum and "number_of_hits" in datum:
                    overlaps_dict.update({datum["file_name"]: datum["number_of_hits"]})

        return overlaps_dict

    def determine_compatibility(
        self,
        bedfile: Union[GRegionSet, str],
        ref_filter: Optional[List[str]] = None,
        concise: Optional[bool] = False,
    ) -> Union[Dict[str, CompatibilityStats], Dict[str, CompatibilityConcise]]:
        """
        Determine compatibility of the bed file.

        :param bedfile: path to bedfile
        :param ref_filter: list of ref genome aliases to filter on.
        :param concise: if True, only return a concise list of compatibility stats. Default: False
        :return: a dict with CompatibilityStats, or CompatibilityConcise model (depends if concise is set to True)
        """

        _LOGGER.info(f"Calculating reference genome stats for {bedfile}...")

        if ref_filter:
            # Filter out unwanted reference genomes to assess
            for genome_model in self.genome_models:
                if genome_model.genome_alias in ref_filter:
                    self.genome_models.remove(
                        genome_model
                    )  # TODO: remove it only for this analysis, not permanently
        try:
            bed_chrom_info = get_bed_chrom_info(bedfile)
        except Exception as e:
            raise BedBossException(
                f"Unable to open bed file or determine compatibility. Error: {str(e)}"
            )

        if not bed_chrom_info:
            raise ValidatorException("Incorrect bed file provided")

        model_compat_stats = {}

        for genome_model in self.genome_models:
            # First and Second Layer of Compatibility
            model_compat_stats[genome_model.genome_digest]: CompatibilityStats = (
                self.calculate_chrom_stats(bed_chrom_info, genome_model.chrom_sizes)
            )

            # Third layer - IGD, only if layer 1 and layer 2 have passed
            if (
                model_compat_stats[
                    genome_model.genome_digest
                ].chrom_name_stats.passed_chrom_names
                and not model_compat_stats[
                    genome_model.genome_digest
                ].chrom_length_stats.beyond_range
            ):
                model_compat_stats[genome_model.genome_digest].igd_stats = (
                    self.get_igd_overlaps(bedfile)
                )

            # Calculate compatibility rating
            model_compat_stats[genome_model.genome_digest].compatibility = (
                self.calculate_rating(model_compat_stats[genome_model.genome_digest])
            )
        if concise:
            concise_dict = {}
            for name, stats in model_compat_stats.items():
                concise_dict[name] = self._create_concise_output(stats)

            return concise_dict

        return model_compat_stats

    def calculate_rating(self, compat_stats: CompatibilityStats) -> RatingModel:
        """
        Determine the compatibility tier

        Tiers:
            - Tier1: Excellent compatibility, 0 pts
            - Tier2: Good compatibility, may need some processing, 1-3 pts
            - Tier3: Bed file needs processing to work (shifted hg38 to hg19?), 4-6 pts
            - Tier4: Poor compatibility, 7-9 pts

        :param compat_stats: CompatibilityStats with unprocess compatibility statistics
        :return: RatingModel - {
                    assigned_points: int
                    tier_ranking: int
                }
        """

        points_rating = 0

        # 1. Check extra sequences sensitivity.
        # sensitivity = 1 is considered great and no points should be assigned
        xs = compat_stats.chrom_name_stats.xs
        if xs < 0.3:
            points_rating += 6  # 3 + 1 + 1 + 1
        elif xs < 0.5:
            points_rating += 5  # 3 + 1 + 1
        elif xs < 0.7:
            points_rating += 4  # 3 + 1
        elif xs < 1:
            points_rating += 3
        else:
            pass

        # 2. Check OOBR and assign points based on sensitivity
        # only assessed if no extra chroms in query bed file
        if compat_stats.chrom_name_stats.passed_chrom_names:
            oobr = compat_stats.chrom_length_stats.oobr

            if oobr < 0.3:
                points_rating += 6  # 3 + 1 + 1 + 1
            elif oobr < 0.5:
                points_rating += 5  # 3 + 1 + 1
            elif oobr < 0.7:
                points_rating += 4  # 3 + 1
            elif oobr < 1:
                points_rating += 3
        else:
            # Do nothing here, points have already been added when Assessing XS if it is not == 1
            pass

        # 3. Check Sequence Fit - comparing lengths in queries vs lengths of queries in ref genome vs not in ref genome
        sequence_fit = compat_stats.chrom_sequence_fit_stats.sequence_fit
        if sequence_fit:
            # since this is only on keys present in both, ratio should always be less than 1
            # Should files be penalized here or actually awarded but only if the fit is really good?
            if sequence_fit < 0.90:
                points_rating += 1
            if sequence_fit < 0.60:
                points_rating += 1
            if sequence_fit < 0.60:
                points_rating += 1

        else:
            # if no chrom names were found during assessment
            points_rating += 4

        # Run analysis on igd_stats
        # WIP, currently only showing IGD stats for informational purposes
        if compat_stats.igd_stats and compat_stats.igd_stats != {}:
            self._process_igd_stats(compat_stats.igd_stats)

        tier_ranking = 0
        if points_rating == 0:
            tier_ranking = 1
        elif 1 <= points_rating <= 3:
            tier_ranking = 2
        elif 4 <= points_rating <= 6:
            tier_ranking = 3
        elif 7 <= points_rating:
            tier_ranking = 4
        else:
            _LOGGER.info(
                f"Catching points discrepancy,points = {points_rating}, assigning to Tier 4"
            )
            tier_ranking = 4

        return RatingModel(assigned_points=points_rating, tier_ranking=tier_ranking)

    def _process_igd_stats(self, igd_stats: dict):
        """
        Placeholder to process IGD Stats and determine if it should impact tier rating
        """
        ...

    @staticmethod
    def _build_default_models() -> list[GenomeModel]:
        """
        Build a default list of GenomeModels from the chrom.sizes folder.
        Uses file names as genome alias.

        return list[GenomeModel]
        """

        # OLD: TO BE DELETED IN FUTURE RELEASES
        # dir_path = os.path.dirname(os.path.realpath(__file__))
        #
        # chrm_sizes_directory = os.path.join(dir_path, "chrom_sizes")
        #
        # all_genome_models = []
        # for root, dirs, files in os.walk(chrm_sizes_directory):
        #     for file in files:
        #         if file.endswith(".sizes"):
        #             curr_genome_model = GenomeModel(
        #                 genome_alias=file, chrom_sizes_file=os.path.join(root, file)
        #             )
        #             all_genome_models.append(curr_genome_model)
        #
        # return all_genome_models
        return get_chrom_sizes()

    @staticmethod
    def _create_concise_output(output: CompatibilityStats) -> CompatibilityConcise:
        """
        Convert extended CompatibilityStats to concise output

        :param output: full compatibility stats
        :return:  concise compatibility stats
        """
        return CompatibilityConcise(
            xs=output.chrom_name_stats.xs,
            oobr=output.chrom_length_stats.oobr,
            sequence_fit=output.chrom_sequence_fit_stats.sequence_fit,
            assigned_points=output.compatibility.assigned_points,
            tier_ranking=output.compatibility.tier_ranking,
        )

    def predict(self, bedfile: str) -> Union[str, None]:
        """
        Predict compatibility of a bed file with reference genomes

        :param bedfile: path to bedfile

        :return: sring with the name of the reference genome that the bed file is compatible with or None, if no compatibility is found
        """

        _LOGGER.info(f"Predicting compatibility of {bedfile} with reference genomes...")
        compatibility_stats: Dict[str, CompatibilityConcise] = (
            self.determine_compatibility(bedfile, concise=True)
        )

        best_rankings = []

        for genome, prediction in compatibility_stats.items():
            if prediction.tier_ranking == 1:
                best_rankings.append(genome)

        if len(best_rankings) == 0:
            for genome, prediction in compatibility_stats.items():
                if prediction.tier_ranking == 2:
                    best_rankings.append(genome)

        if len(best_rankings) >= 1:
            return GENOME_FILES.get(best_rankings[0])
        return None
