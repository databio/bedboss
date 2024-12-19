import os
import pickle

from geniml.search.backends.dbbackend import QdrantBackend
from qdrant_client import QdrantClient
from qdrant_client.http import models
from qdrant_client.models import Distance, PointStruct, VectorParams

TEXT_QDRANT_COLLECTION_NAME = "bed_text"

DEFAULT_QUANTIZATION_CONFIG = models.ScalarQuantization(
    scalar=models.ScalarQuantizationConfig(
        type=models.ScalarType.INT8,
        quantile=0.99,
        always_ram=True,
    ),
)


def upload_text_embeddings():
    # lab qdrant client
    # qc = QdrantClient(
    #   host=os.environ.get("QDRATN_HOST"),
    #   api_key=os.environ.get("QDRANT_API_KEY")
    # )

    qc = QdrantBackend(
        dim=384,
        collection=TEXT_QDRANT_COLLECTION_NAME,
        qdrant_host="",
        qdrant_api_key="",
    )
    qc = QdrantClient(
        url="",
        api_key="",
    )

    # load metadata embedddings into new collection
    with open("./text_loading.pkl", "rb") as f:
        text_vectors, payloads = pickle.load(f)

    ids = list(range(0, len(payloads)))

    points = [
        PointStruct(id=ids[i], vector=text_vectors[i].tolist(), payload=payloads[i])
        for i in range(len(payloads))
    ]

    qc.upsert(collection_name=TEXT_QDRANT_COLLECTION_NAME, points=points)


if __name__ == "__main__":
    upload_text_embeddings()
