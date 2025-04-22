def unprocessed_run():
    from bedboss.bedboss import reprocess_all

    reprocess_all(
        bedbase_config="/home/bnt4me/virginia/repos/bbuploader/config_db_local.yaml",
        limit=5,
        output_folder="/home/bnt4me/virginia/repos/bbuploader/data",
    )


def reprocess_one():
    from bedboss.bedboss import reprocess_one

    reprocess_one(
        identifier="a0f1889fd8026780df8bba6a8ddac00e",
        bedbase_config="/home/bnt4me/virginia/repos/bbuploader/config_db_local.yaml",
        output_folder="/home/bnt4me/virginia/repos/bbuploader/data",
    )


def process_one():
    from bedboss.bedboss import run_all

    run_all(
        input_file="/home/bnt4me/Downloads/GSM8424583_ChIPBX114.final_hg38.broadPeak.gz",
        input_type="bed",
        outfolder="/home/bnt4me/virginia/repos/bbuploader/data",
        bedbase_config="/home/bnt4me/virginia/repos/bbuploader/config_db_local.yaml",
        genome="hg38",
    )


if __name__ == "__main__":
    # unprocessed_run()
    # reprocess_one()
    process_one()
