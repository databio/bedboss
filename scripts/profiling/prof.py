import cProfile
import pstats


def runn():

    from bedboss.bedboss import run_all

    with cProfile.Profile() as pr:
        run_all(
            bedbase_config="/home/bnt4me/virginia/repos/bbuploader/config_db_local.yaml",
            outfolder="/home/bnt4me/virginia/repos/bbuploader/data",
            genome="hg38",
            # input_file="/home/bnt4me/virginia/repos/bedboss/test/data/bed/hg38/GSM6732293_Con_liver-IP2.bed",
            # input_file="/home/bnt4me/virginia/repos/bedboss/scripts/profiling/GSE253137_diff_peaks_PSIP_Jurkat_vs_IgG_Jurkat.MAPQ30_keepdupauto_hg38_peaks.narrowPeak.gz",
            input_file="/home/bnt4me/Downloads/test_chroms.bed.gz",
            input_type="bed",
            force_overwrite=True,
            upload_pephub=True,
            upload_s3=True,
            upload_qdrant=True,
            name="test",
        )

    stats = pstats.Stats(pr)
    stats.sort_stats(pstats.SortKey.TIME)
    stats.dump_stats(filename="test_profiling")


import time
from geniml.io import RegionSet
from gtars.models import RegionSet as GRegionSet
from geniml.bbclient import BBClient


def open_Gtars():
    print(1)
    b = GRegionSet(
        "/home/bnt4me/virginia/repos/bedboss/scripts/profiling/93d7b12fcf7c95668075395c46835f3a.bed.gz"
    )
    print(2)
    return b


def cache_file():
    bbc = BBClient()
    bbc.add_bed_to_cache(
        "/home/bnt4me/virginia/repos/bedboss/scripts/profiling/a07e8a86e232308ac2fdbc4b453c520d_w.bed.gz"
    )


def regionset_prof() -> None:

    # with cProfile.Profile() as pr:
    #     f = RegionSet("/home/bnt4me/virginia/repos/bedboss/scripts/profiling/93d7b12fcf7c95668075395c46835f3a.bed.gz")
    #     ident = f.identifier
    #
    # stats = pstats.Stats(pr)
    # stats.sort_stats(pstats.SortKey.TIME)
    # stats.dump_stats(filename="geniml_io_profiling")

    with cProfile.Profile() as pr:
        cache_file()

    stats = pstats.Stats(pr)
    stats.sort_stats(pstats.SortKey.TIME)
    stats.dump_stats(filename="gtars_profiling")


def time_widht():
    start = time.time()
    # f = GRegionSet("/home/bnt4me/virginia/repos/bedboss/scripts/profiling/93d7b12fcf7c95668075395c46835f3a.bed.gz")
    f = GRegionSet("/home/bnt4me/Downloads/combined_unsorted.bed.gz")

    width = f.mean_region_width()
    start1 = time.time()
    print(start1 - start)
    print(len(f))
    print(width)
    end = time.time()
    print(end - start1)


if __name__ == "__main__":
    # regionset_prof()
    # runn()
    time_widht()
