__all__ = ["add_to_qdrant"]


def __getattr__(name):
    if name == "add_to_qdrant":
        from bedboss.qdrant_index.qdrant_index import add_to_qdrant

        return add_to_qdrant
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
