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
        # gse="gse261411",
        # gse="gse261536",
        # gse="gse274130",
        # Genome hg19 and mm10
        gse="gse280839",
        # gse="gse246900",
        # gse="gse247593",
        # gse="gse241222",
        # gse="gse266130",
        # gse="gse209627",
        # gse="gse266949",
        # gse="gse240325", # TODO: check if qc works
        # gse="gse229592", # mice
        bedbase_config="/home/bnt4me/virginia/repos/bbuploader/config_db_local.yaml",
        outfolder="/home/bnt4me/virginia/repos/bbuploader/data",
        # genome="HG38",
        rerun=True,
        run_failed=True,
        run_skipped=True,
        reinit_skipper=True,
        lite=False,
        overwrite=True,
        overwrite_bedset=True,
    )
    time2 = time.time()
    print(f"Time taken: {time2 - time1}")


def upload_time():
    from bedboss.bbuploader.main import upload_all

    upload_all(
        bedbase_config="/home/bnt4me/virginia/repos/bbuploader/config_db_local.yaml",
        outfolder="/home/bnt4me/virginia/repos/bbuploader/data",
        start_date="2020/06/01",
        end_date="2020/07/15",
        search_limit=1000,
        download_limit=10000,
        search_offset=0,
        # genome="hg38",
        rerun=True,
        run_skipped=True,
        lite=True,
    )


if __name__ == "__main__":
    # runn()

    another_test()
    # upload_time()
