"""
Backfill missing cell_line values in bed_metadata table.

Usage:
    python scripts/update_cell_line.py <config.yaml>
    python scripts/update_cell_line.py <config.yaml> --dry-run
"""

import argparse
import logging

from sqlalchemy.orm import Session
from sqlalchemy import select, or_

from bbconf.bbagent import BedBaseAgent
from bbconf.db_utils import Bed, BedMetadata
from bedboss.bbuploader.metadata_extractor import find_cell_line

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BATCH_SIZE = 500


def main():
    parser = argparse.ArgumentParser(
        description="Backfill missing cell_line in bed_metadata"
    )
    parser.add_argument("config", help="Path to bbconf YAML config file")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview changes without writing to the database",
    )
    args = parser.parse_args()

    agent = BedBaseAgent(config=args.config)

    with Session(agent.config.db_engine.engine) as session:
        stmt = (
            select(Bed)
            .join(BedMetadata)
            .where(
                or_(
                    BedMetadata.cell_line == "",
                    BedMetadata.cell_line.is_(None),
                )
            )
        )
        beds = session.scalars(stmt).all()
        total = len(beds)
        logger.info(f"Found {total} records with missing cell_line")

        updated = 0
        for i, bed in enumerate(beds, 1):
            parts = [bed.description, bed.annotations.original_file_name, bed.name]
            combined = " ".join(p for p in parts if p)
            cell_line = find_cell_line(combined)

            if cell_line:
                logger.info(
                    f"  [{bed.id}] '{combined[:80]}...' -> cell_line='{cell_line}'"
                )
                if not args.dry_run:
                    bed.annotations.cell_line = cell_line
                updated += 1

            if not args.dry_run and i % BATCH_SIZE == 0:
                session.commit()
                logger.info(f"  Committed batch ({i}/{total} processed so far)")

        if not args.dry_run:
            session.commit()
            logger.info(f"Committed final batch. Updated {updated}/{total} records.")
        else:
            logger.info(f"[DRY RUN] Would update {updated}/{total} records")


if __name__ == "__main__":
    main()
