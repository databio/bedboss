from bedboss.bbuploader.main import upload_gse


def runn():
    upload_gse(
        # gse="gse246900",
        # gse="gse247593",
        # gse="gse241222",
        # gse="gse266130",
        gse="gse256031",
        # gse="gse240325", # TODO: check if qc works
        # gse="gse229592", # mice
        bedbase_config="/home/bnt4me/virginia/repos/bbuploader/config_db_local.yaml",
        outfolder="/home/bnt4me/virginia/repos/bbuploader/data",
        # genome="HG38",
        rerun=True,
        run_failed=True,
        run_skipped=True,
    )
    # import pandas as pd

    # df = pd.read_csv("/home/bnt4me/Downloads/test_b.bed.gz", sep="\t",
    #                       header=None, nrows=4)
    # rf = pd.read_csv("/home/bnt4me/.bbcache/bedfiles/4/f/test.bed.gz", sep="\t",
    #                       header=None, nrows=4)
    # rf


def another_test():
    # time it:
    import time

    from bedboss.bbuploader.main import upload_gse

    time1 = time.time()
    upload_gse(
        gse="gse261411",
        # gse="gse261536",
        # gse="gse274130",
        # Genome hg19 and mm10
        # gse="gse280839",
        # gse="gse246900",  ## -- this is good. allways using it
        # gse="gse106049",  # This is interesting reference genome.
        # gse="gse292153",  # This is interesting reference genome.
        # gse="gse247593", # Big dataset
        # gse="gse241222",
        # gse="gse266130",
        # gse="gse209627",
        # gse="gse266949", # HG
        # gse="gse240325", # QC fails - good for testing qc
        # gse="gse229592", # mice
        # gse="gse217638", # same samples #1.
        # gse="gse217639",  # same samples #2.
        bedbase_config="/home/bnt4me/virginia/repos/bbuploader/config_db_local.yaml",
        outfolder="/home/bnt4me/virginia/repos/bbuploader/data",
        # genome="HG38",
        # rerun=True,
        run_failed=True,
        run_skipped=True,
        reinit_skipper=True,
        lite=True,
        # overwrite=True,
        overwrite_bedset=True,
    )
    time2 = time.time()
    print(f"Time taken: {time2 - time1}")


if __name__ == "__main__":
    # runn()

    another_test()
    # upload_time()


## cmd
# bedboss geo upload-all --outfolder /home/bnt4me/virginia/repos/bbuploader/data --start-date 2025/02/23 --end-date 2025/02/26 --no-use-skipper --lite --bedbase-config /home/bnt4me/virginia/repos/bbuploader/config_db_local.yaml --no-use-skipper --no-preload
##
