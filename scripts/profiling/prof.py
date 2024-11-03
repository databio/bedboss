def runn():
    import cProfile
    import pstats

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


if __name__ == "__main__":
    runn()
