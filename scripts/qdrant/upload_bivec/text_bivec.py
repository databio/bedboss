from bbconf import BedBaseAgent
from qdrant_client.models import PointStruct
import pickle


def upload_text_of_bivec():
    bba = BedBaseAgent("/home/bnt4me/virginia/repos/bedhost/config.yaml")

    # load metadata embedddings into new collection
    with open("text_loading.pkl", "rb") as f:
        text_vectors, payloads = pickle.load(f)

    ids = list(range(0, len(payloads)))

    batch_size = 1000
    for start in range(0, len(payloads), batch_size):
        end = min(start + batch_size, len(payloads))
        batch_ids = ids[start:end]
        batch_vectors = text_vectors[start:end]
        batch_payloads = payloads[start:end]

        points = [
            PointStruct(
                id=batch_ids[i],
                vector=batch_vectors[i].tolist(),
                payload=batch_payloads[i],
            )
            for i in range(len(batch_ids))
        ]

        bba.config._qdrant_advanced_engine.upsert(
            collection_name=bba.config.config.qdrant.text_collection, points=points
        )
    print("Text embeddings uploaded successfully.")


if __name__ == "__main__":
    upload_text_of_bivec()
