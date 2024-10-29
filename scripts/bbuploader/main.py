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


if __name__ == "__main__":
    runn()
