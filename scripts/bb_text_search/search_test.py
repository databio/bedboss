from geniml.search.backends import BiVectorBackend, QdrantBackend
from geniml.search.interfaces import BiVectorSearchInterface


def search_test():

    # backend for text embeddings and bed embeddings
    text_backend = QdrantBackend(
        dim=384,
        collection="bed_text",
        qdrant_host="",
        qdrant_api_key="",
    )  # dim of sentence-transformers embedding output
    bed_backend = QdrantBackend(
        collection="bedbase2",
        qdrant_host="",
        qdrant_api_key="",
    )

    import cProfile

    # import pstats
    #
    # from bedboss.bedboss import run_all
    #
    # with cProfile.Profile() as pr:
    # the search backend
    from time import time

    search_backend = BiVectorBackend(text_backend, bed_backend)

    # the search interface
    search_interface = BiVectorSearchInterface(
        backend=search_backend, query2vec="sentence-transformers/all-MiniLM-L6-v2"
    )
    time1 = time()
    # actual search
    result = search_interface.query_search(
        query="leukemia",
        limit=500,
        with_payload=True,
        with_vectors=False,
        p=1.0,
        q=1.0,
        distance=False,  # QdrantBackend returns similarity as the score, not distance
    )
    result
    time2 = time()
    print(time2 - time1)
    # stats = pstats.Stats(pr)
    # stats.sort_stats(pstats.SortKey.TIME)
    # stats.dump_stats(filename="test_profiling")


if __name__ == "__main__":
    search_test()
