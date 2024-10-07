import os

from bedboss.refgenome_validator.main import ReferenceValidator


FILE_DIR = os.path.dirname(os.path.realpath(__file__))
HG19_CORRECT_DIR = os.path.join(FILE_DIR, "test_data", "bed", "hg19", "correct")
FILE_PATH = f"{HG19_CORRECT_DIR}/sample1.bed.gz"


def test_main():
    # dict_result = ReferenceValidator().determine_compatibility(
    #     FILE_PATH,
    #     concise=True,
    # )
    dict_result = ReferenceValidator().determine_compatibility(
        "/home/bnt4me/.bbcache/bedfiles/0/7/0740332b148a613342603e2e483f53e5.bed.gz",
        concise=True,
    )

    assert dict_result


def test_another_test():
    from bedboss.bbuploader.main import upload_gse

    upload_gse(
        # gse="gse246900",
        # gse="gse247593",
        # gse="gse241222",
        # gse="gse266130",
        gse="gse99178",
        # gse="gse240325", # TODO: check if qc works
        # gse="gse229592", # mice
        bedbase_config="/home/bnt4me/virginia/repos/bbuploader/config_db_local.yaml",
        outfolder="/home/bnt4me/virginia/repos/bbuploader/data",
        # genome="HG38",
        rerun=True,
        run_failed=True,
        run_skipped=True,
    )
