from bedboss.bbuploader.main import upload_all, upload_gse


def test_manual():
    upload_gse(
        # gse="gse246900",
        # gse="gse247593",
        # gse="gse241222",
        # gse="gse266130",
        # gse="gse99178",
        gse="gse269114",
        # gse="gse240325", # TODO: check if qc works
        # gse="gse229592", # mice
        bedbase_config="/home/bnt4me/virginia/repos/bbuploader/config_db_local.yaml",
        outfolder="/home/bnt4me/virginia/repos/bbuploader/data",
        # genome="HG38",
        # rerun=True,
        standardize_pep=True,
        run_failed=True,
        run_skipped=True,
    )
    # upload_all(
    #     bedbase_config="/home/bnt4me/virginia/repos/bbuploader/config_db_local.yaml",
    #     outfolder="/home/bnt4me/virginia/repos/bbuploader/data",
    #     start_date="2024/01/21",
    #     end_date="2024/08/28",
    #     search_limit=2,
    #     search_offset=0,
    #     genome="GRCh38",
    #     rerun=True,
    # )


# upload_all(
#     bedbase_config="/home/bnt4me/virginia/repos/bbuploader/config_db_local.yaml",
#     outfolder="/home/bnt4me/virginia/repos/bbuploader/data",
#     start_date="2024/01/01",
#     # end_date="2024/03/28",
#     search_limit=200,
#     search_offset=0,
#     genome="GRCh38",
# )
