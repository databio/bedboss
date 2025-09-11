from bbconf import BedBaseAgent


if __name__ == "__main__":
    bba = BedBaseAgent("/home/bnt4me/virginia/repos/bbuploader/config_db_local.yaml")

    bba.bed.reindex_semantic_search(purge=False)
    print("Embeddings downloaded successfully.")
