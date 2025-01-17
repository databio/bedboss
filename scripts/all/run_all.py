def unprocessed_run():
    from bedboss.bedboss import reprocess_all

    run_unprocessed_beds(
        bedbase_config="/home/bnt4me/virginia/repos/bbuploader/config_db_local.yaml",
        limit=10,
        output_folder="/home/bnt4me/virginia/repos/bbuploader/data",
    )


def reprocess_one():
    from bedboss.bedboss import reprocess_one

    reprocess_one(
        identifier="a0f1889fd8026780df8bba6a8ddac00e",
        bedbase_config="/home/bnt4me/virginia/repos/bbuploader/config_db_local.yaml",
        output_folder="/home/bnt4me/virginia/repos/bbuploader/data",
    )


if __name__ == "__main__":
    # unprocessed_run()
    reprocess_one()
