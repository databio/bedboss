def unprocessed_run():
    from bedboss.bedboss import run_unprocessed_beds

    run_unprocessed_beds(
        bedbase_config="/home/bnt4me/virginia/repos/bbuploader/config_db_local.yaml",
        limit=10,
        output_folder="/home/bnt4me/virginia/repos/bbuploader/data",
    )


if __name__ == "__main__":
    unprocessed_run()
