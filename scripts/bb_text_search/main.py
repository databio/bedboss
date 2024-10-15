from qdrant_client import QdrantClient
from qdrant_client.models import Distance, PointStruct, VectorParams
import pickle
import os
from qdrant_client.http import models
from geniml.search.backends.dbbackend import QdrantBackend


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
    qc = QdrantClient(
        url="https://4ff085d3-09ad-4f6e-902a-a3c69bbca6dd.us-east4-0.gcp.cloud.qdrant.io",
        api_key="WtCknIsU_en1U6JK6DSjvPDLjxwio2fFLkpDEiuZo_CDefEMYsNOPA",
    )

    # qc = QdrantBackend(
    #     config = VectorParams(size=384, distance=Distance.COSINE),
    #     collection=TEXT_QDRANT_COLLECTION_NAME,
    #     qdrant_host="https://4ff085d3-09ad-4f6e-902a-a3c69bbca6dd.us-east4-0.gcp.cloud.qdrant.io",
    #     qdrant_api_key="WtCknIsU_en1U6JK6DSjvPDLjxwio2fFLkpDEiuZo_CDefEMYsNOPA",
    #
    # )

    # load metadata embedddings into new collection
    with open("./text_bed.pkl", "rb") as f:
        text_vectors, payloads = pickle.load(f)

    ids = list(range(0, len(payloads)))

    points = [
        PointStruct(id=ids[i], vector=text_vectors[i].tolist(), payload=payloads[i])
        for i in range(len(payloads))
    ]

    qc.upsert(collection_name=TEXT_QDRANT_COLLECTION_NAME, points=points)


if __name__ == "__main__":
    upload_text_embeddings()
