from tqdm import tqdm
from typing import List
from bbconf import BedBaseAgent

from bedboss.refgenome_validator.main import ReferenceValidator

from sqlalchemy import select
from sqlalchemy.orm import Session
from bbconf.db_utils import Bed
from geniml.bbclient import BBClient


def update_all_bedbase(purge: bool = False):
    """
    Update all genomes in BedBase with the latest sequence collections from RefGenie.

    :param purge: If True, purge database, and set as it wasn't updated.
    """

    rv = ReferenceValidator()

    # bbagent = BedBaseAgent("/home/bnt4me/virginia/repos/bedhost/config.yaml")
    bbagent = BedBaseAgent(
        "/project/shefflab/brickyard/results_pipeline/bedbase_jobs/bedbase_config.yaml"
    )

    bbclient = BBClient()

    select_not_updated = select(Bed).where(Bed.indexed == False).limit(20)
    with Session(bbagent.bed._sa_engine) as session:

        if purge:
            session.query(Bed).update({Bed.indexed: False})
            session.commit()

        beds_to_update: List[Bed] = session.scalars(select_not_updated)

        beds_to_update = [bed for bed in beds_to_update]

        updated_number = 0

        for bed in tqdm(beds_to_update, desc="Updating beds in BedBase..."):
            identifier = bed.id
            try:
                bed_path = bbclient.seek(identifier)
            except FileNotFoundError:
                print(f"Bed file for {identifier} not found.")
                bbclient.load_bed(identifier)
                bed_path = bbclient.seek(identifier)

            compat = rv.determine_compatibility(bed_path, concise=True)
            compatitil = {}

            for k, v in compat.items():
                if v.tier_ranking < 4:
                    compatitil[k] = v

            # Update the bed file in BedBase
            bbagent.bed.update(
                identifier=identifier,
                ref_validation=compatitil,
                upload_pephub=False,
            )

            # Mark the bed as indexed
            bed.indexed = True

            updated_number += 1

            if updated_number % 100 == 0:
                session.commit()

        session.commit()


if __name__ == "__main__":
    update_all_bedbase(purge=False)
