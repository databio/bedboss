def reindex():
    """
    Reindex all models in the database.
    """
    from bedboss.qdrant_index import add_to_qdrant

    add_to_qdrant(
        config="/home/bnt4me/virginia/repos/bbuploader/config_db_local.yaml",
    )


if __name__ == "__main__":

    reindex()
