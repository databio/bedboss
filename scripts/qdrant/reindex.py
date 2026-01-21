from bbconf import BedBaseAgent

if __name__ == "__main__":
    bba = BedBaseAgent("/home/bnt4me/virginia/repos/bedhost/config.yaml")

    # bba.bed.reindex_semantic_search(purge=False)
    bba.bed.reindex_hybrid_search(purge=True)
    print("Embeddings downloaded successfully.")
