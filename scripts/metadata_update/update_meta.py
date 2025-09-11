from bbconf import BedBaseAgent
import json
from tqdm import tqdm

bbconf = BedBaseAgent(
    config="/home/bnt4me/virginia/repos/bbuploader/config_db_local.yaml"
)

#     bbconf.bed.update(
#         identifier="d765fa5763bc9e71079aa5bb16c202de",
#         metadata={"description": "CTCF ChIP-seq on Prostate carcinoma cell line."},
#     )


def update_description(bed_id: str, new_description: str):
    bbconf.bed.update(
        identifier=bed_id,
        metadata={"description": new_description},
        upload_pephub=False,
        upload_s3=False,
        upload_qdrant=False,
    )
    print("Description updated successfully.")


def update_assay(bed_id: str, new_assay: str):
    bbconf.bed.update(
        identifier=bed_id,
        metadata={"assay": new_assay},
        upload_pephub=False,
        upload_s3=False,
        upload_qdrant=False,
    )
    print(f"Assay updated successfully for {bed_id}.")


def update_all_descriptions():

    with open(
        "/home/bnt4me/virginia/repos/bedboss/scripts/eval/bedbase_bed_descriptions.json",
        "r",
    ) as file:
        query_relevance = json.load(file)

    total_items = len(query_relevance)
    success_count = 0
    failed_count = 0

    for bed_id, description in tqdm(
        query_relevance.items(), total=total_items, desc="Updating descriptions"
    ):
        try:
            update_description(bed_id, description)
            success_count += 1
        except Exception as e:
            failed_count += 1
            print(f"Failed to update {bed_id}: {e}")


def update_all_assays():

    import pandas as pd

    meta = pd.read_csv(
        "/home/bnt4me/virginia/repos/bedboss/scripts/metadata_update/encode_metadata/out.csv"
    )

    info_dict = meta.set_index("id")["assay"].to_dict()

    info_dict

    total_items = len(info_dict)
    success_count = 0
    failed_count = 0

    for bed_id, assay in tqdm(
        info_dict.items(), total=total_items, desc="Updating assays"
    ):
        try:
            update_assay(bed_id, assay)
            success_count += 1
        except Exception as e:
            failed_count += 1
            print(f"Failed to update {bed_id}: {e}")


if __name__ == "__main__":
    # update_all_descriptions()

    update_all_assays()
