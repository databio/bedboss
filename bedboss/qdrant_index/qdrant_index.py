import logging

from bbconf.bbagent import BedBaseAgent

_LOGGER = logging.getLogger("bedboss")


def add_to_qdrant(config: str) -> None:
    """
    Reindex all bed files in qdrant

    :param config: path to the config file
    """
    agent = BedBaseAgent(config=config)
    agent.bed.reindex_qdrant()


# TODO: work on reindexing qdrant (speeding up)
# def reindex_qdrant1(config: str) -> None:
#     """
#     Reindex all bed files in qdrant
#
#     :param config: path to the config file
#     """
#     bb_client = BBClient()
#
#     agent = BedBaseAgent(config=config)
#     annotation_result = agent.bed.get_ids_list(limit=10, genome="hg38")
#
#     if not annotation_result.results:
#         _LOGGER.error("No bed files found.")
#         return None
#
#     embedding_list = []
#     payload_list = []
#     id_list = []
#
#     with tqdm(total=len(annotation_result.results), position=0, leave=True) as pbar:
#         for record in annotation_result.results:
#             try:
#                 bed_region_set_obj = GRegionSet(bb_client.seek(record.id))
#             except FileNotFoundError:
#                 _LOGGER.info(f"File {record.id} not found. Fetching from bedbase...")
#                 bed_region_set_obj = bb_client.load_bed(record.id)
#
#             # pbar.set_description(f"Processing file: {record.id}")
#
#             id_list.append(record.id)
#             embedding_list.append(agent.bed._embed_file(bed_region_set_obj))
#             payload_list.append(record.annotation.model_dump() if record.annotation else {})
#
#             pbar.write(f"File: {record.id} indexed successfully")
#             pbar.update(1)
#
#     _LOGGER.info("Uploading embeddings to qdrant...")
#
#     points = [
#         PointStruct(id=id_list[i], vector=embedding_list[i].tolist(), payload=payload_list[i])
#         for i in range(len(id_list))
#     ]
#
#     agent.config.qdrant_engine.qd_client.upsert(collection_name=agent.config.config.qdrant.file_collection, points=points)
#
#     return None
