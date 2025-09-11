from bedboss.bbuploader.main import upload_all


def runn():
    upload_all(
        # gse="gse246900",
        # gse="gse247593",
        # gse="gse241222",
        # gse="gse266130",
        # gse="gse256031",
        # gse="gse240325", # TODO: check if qc works
        # gse="gse229592", # mice
        start_date="2021/10/25",
        end_date="2021/11/30",
        search_limit=1,
        download_limit=5,
        bedbase_config="/home/bnt4me/virginia/repos/bbuploader/config_db_local.yaml",
        outfolder="/home/bnt4me/virginia/repos/bbuploader/data",
        # genome="HG38",
        # rerun=True,
        run_failed=True,
        run_skipped=True,
        reinit_skipper=True,
        lite=True,
    )


def run_gse():
    # time it:
    import time

    from bedboss.bbuploader.main import upload_gse

    time1 = time.time()
    upload_gse(
        # gse="gse261411",
        # gse="gse261536",
        # gse="gse274130",
        # Genome hg19 and mm10
        # gse="gse280839",
        # gse="gse218680",  ### -- this is example in the api
        # gse="gse38163",  ### -- without genome
        gse="gse280208",
        # gse="gse246900",  ## -- this is good. allways using it
        # gse="gse32970", # - this data is encode https://www.encodeproject.org/experiments/ENCSR000ELR/
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
        bedbase_config="/home/bnt4me/virginia/repos/bedhost/config.yaml",
        outfolder="/home/bnt4me/virginia/repos/bbuploader/data",
        # genome="HG38",
        rerun=True,
        run_failed=True,
        run_skipped=True,
        reinit_skipper=True,
        lite=True,
        overwrite=True,
        overwrite_bedset=True,
    )
    time2 = time.time()
    print(f"Time taken: {time2 - time1}")


def reprocess_id():
    from bedboss.bedboss import reprocess_one, reprocess_all

    reprocess_one(
        identifier="b4712e0051aa975d450baf576a9aa6a2",
        bedbase_config="/home/bnt4me/virginia/repos/bbuploader/config_db_local.yaml",
        output_folder="/home/bnt4me/virginia/repos/bbuploader/data",
    )

    # reprocess_all(
    #     bedbase_config="/home/bnt4me/virginia/repos/bbuploader/config_db_local.yaml",
    #     output_folder="/home/bnt4me/virginia/repos/bbuploader/data",
    #     limit=2,
    # )


if __name__ == "__main__":
    # runn()

    run_gse()
    # reprocess_id()

## cmd
# bedboss geo upload-all --outfolder /home/bnt4me/virginia/repos/bbuploader/data --start-date 2025/02/23 --end-date 2025/02/26 --no-use-skipper --lite --bedbase-config /home/bnt4me/virginia/repos/bbuploader/config_db_local.yaml --no-use-skipper --no-preload
##
# bedboss run-pep --pep databio/excluderanges:default --outfolder /home/bnt4me/virginia/bedbase_output/ --bedbase-config /home/bnt4me/virginia/repos/bbuploader/config_db_local.yaml
#
# from geniml.region2vec.main import Region2VecExModel
#
# model = Region2VecExModel("databio/r2v-encode-hg38")
# from gtars.models import RegionSet
#
# rs = RegionSet(
#     "/home/bnt4me/Downloads/cf90c0bc424838cf4f720a024ca917b3.bed.gz"
# )  # --killed
# rs = RegionSet(
#     "/home/bnt4me/Downloads/55caa8fc3a1bcfa4643f9b7d964d84b4.bed.gz"
# )  # --killed
#
# rs = RegionSet("/home/bnt4me/Downloads/cfd43bfd74fe3b35ca1bd2d274362511.bed.gz")
# f = model.encode(rs)
