## FILE with functions to download chrom sizes from refgenie and validate them against the genome model

import json
import logging
import os
import warnings
from functools import lru_cache
from typing import Any

import requests
from bbconf import BedBaseAgent
from geniml.bbclient.const import DEFAULT_CACHE_FOLDER
from pydantic import BaseModel
from tqdm import tqdm

from bedboss.const import CANONICAL_GENOME_DIGESTS, PKG_NAME
from bedboss.exceptions import BedBossException
from bedboss.refgenome_validator.genome_model import GenomeModel

BASE_URL = "https://api.refgenie.org"
GENOMES_URL = os.path.join(BASE_URL, "v4/genomes?limit=1000")
SEQ_COL_URL = os.path.join(
    BASE_URL, "seqcol/collection/{digest}?collated=true&attribute=name_length_pairs"
)
SEQ_COL_URL_JSON_URL = (
    "https://huggingface.co/databio/bedbase-umap/resolve/main/genome_seqcol.json"
)

_LOGGER = logging.getLogger(PKG_NAME)


class SeqCol(BaseModel):
    length: int
    name: str


class SeqColGenome(BaseModel):
    genome: str | None = None
    digest: str
    description: str
    collection: list[SeqCol]


class Genomes(BaseModel):
    genomes: list[SeqColGenome]


def run_requests(url, timeout=60) -> Any:
    """
    Run a GET request to the specified URL and return the response.
    """
    try:
        response = requests.get(url, timeout=timeout)
        response.raise_for_status()  # Raise an error for bad responses
        return response.json()
    except requests.RequestException as e:
        _LOGGER.error(f"Error fetching data from {url}: {e}")

        warnings.warn(f"Error fetching data from {url}: {e}")
        return None


def get_genome_list() -> list[dict]:
    """
    Fetch the list of genomes from Refgenie.
    """

    genome_data = run_requests(GENOMES_URL)
    if genome_data:
        return genome_data.get("items", [])
    warnings.warn("Failed to fetch genomes from Refgenie, returning empty list.")
    return []


def seq_col_from_digest(digest: str) -> list[SeqCol]:
    """

    Fetch sequence collection from Refgenie using the genome digest.

    Args:
        digest: The digest of the genome to fetch the sequence collection for.

    Returns:
        A list of SeqCol objects containing sequence names and lengths.
    """
    url = SEQ_COL_URL.format(digest=digest)

    # Getting first level of genome info
    response = run_requests(url)
    return_list = []

    for item in response:
        return_list.append(SeqCol(**item))

    return return_list


def get_seq_col() -> Genomes:
    """
    Fetch sequence collections for all genomes from Refgenie and return them as a Genomes object
    containing a list of SeqColGenome objects.

    This function retrieves the list of genomes, fetches their sequence collections,

    Returns:
        Genomes object containing SeqColGenome objects for each genome.

    """

    genomes = get_genome_list()
    if not genomes:
        raise BedBossException("Failed to fetch genome list from Refgenie.")

    return_list = []
    # genomes = genomes[500:-1]  # TEMP for testing
    for info in tqdm(genomes, desc="Downloading genomes from refgenie..."):
        digest = info["digest"]
        seq_col = seq_col_from_digest(digest)

        return_list.append(
            SeqColGenome(
                genome=info["aliases"][0],
                digest=digest,
                description=info["description"],
                collection=seq_col,
            )
        )

    return Genomes(genomes=return_list)


def save_seq_col_to_json(genomes: Genomes, output_path: str = "genome_seqcol.json"):
    """
    Save sequence collections to a JSON file.

    Args:
        genomes: Genomes object to save
        output_path: Path to save the JSON file
    """

    _LOGGER.info("Saving Refgenie genomes for later reuse...")
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(genomes.model_dump(), f, indent=4)
    _LOGGER.info(f"Saved sequence collections to {output_path}")


def read_seq_col_from_json(input_path: str = "genome_seqcol.json") -> Genomes:
    """
    Read sequence collections from a JSON file.

    Args:
        input_path: Path to the JSON file

    Returns:
        Genomes object containing the sequence collections
    """

    # os.makedirs(os.path.dirname(input_path), exist_ok=True)
    with open(input_path, "r") as f:
        data = json.load(f)
    return Genomes(**data)


def read_seq_col_from_url(input_path: str = "genome_seqcol.json") -> Genomes:
    """
    Read sequence collections from a JSON URL.

    :param input_path: URL to the JSON file.
    :return: Genomes object containing the sequence collections.
    """
    data = run_requests(input_path)
    if not data:
        raise BedBossException(
            f"Failed to fetch sequence collections from URL: {input_path}"
        )
    return Genomes(**data)


def modify_for_analysis(genomes: Genomes) -> list[GenomeModel]:
    """
    Modify the genomes data for analysis.

    Args:
        genomes: Genomes object to modify

    Returns:
        List of dictionaries with genome information
    """
    genome_list = []

    for genome in genomes.genomes:
        g_size = {}
        for seq in genome.collection:
            g_size[seq.name] = seq.length

        genome_list.append(
            GenomeModel(
                genome_alias=genome.genome,
                chrom_sizes_file=g_size,
                genome_digest=genome.digest,
            )
        )

    return genome_list


@lru_cache(maxsize=1)
def _load_genomes_cached() -> Genomes:
    """
    Load the seqcol genome data once per process and memoize it.

    Reads the on-disk cache if present, otherwise downloads from Refgenie (or
    the fallback URL) and saves it. Both the reference-genome validator and the
    bigBed chrom.sizes lookup share this single load — this is the "all genomes
    read in" data.
    """
    cached_file_path = os.path.join(DEFAULT_CACHE_FOLDER, "genome_seqcol.json")

    try:
        ret = read_seq_col_from_json(input_path=cached_file_path)
    except FileNotFoundError:
        try:
            _LOGGER.info("No genome_seqcol.json found, downloading from refgenie...")
            ret = get_seq_col()
        except Exception as e:
            _LOGGER.info(f"Failed to fetch genome data from Refgenie: {e}")
            ret = read_seq_col_from_url(input_path=SEQ_COL_URL_JSON_URL)
        save_seq_col_to_json(ret, output_path=cached_file_path)
    return ret


def get_chrom_sizes() -> list[GenomeModel]:
    """
    Get chromosome sizes from Refgenie and return them as a list of GenomeModel objects.

    The underlying seqcol data is loaded once and cached; a fresh list of
    GenomeModel objects is built on each call so callers (e.g. the validator's
    ``ref_filter``) can mutate their own list without corrupting the cache.
    """
    return modify_for_analysis(_load_genomes_cached())


def chrom_sizes_dict_for(
    genome_alias: str,
    genome_models: list[GenomeModel] | None = None,
) -> dict[str, int] | None:
    """
    Return the canonical genome's ``{chrom: length}`` dict for a genome alias.

    Resolves the alias to the digest pinned in ``CANONICAL_GENOME_DIGESTS`` and
    returns that genome's chrom sizes — the same seqcol sizes the reference
    genome validator uses. This is the source of chrom.sizes for bigBed (and the
    WIG conversion).

    Args:
        genome_alias: Standardized genome alias (e.g. ``hg38``).
        genome_models: Optional list of already-loaded GenomeModels (e.g. a live
            ``ReferenceValidator.genome_models``) to reuse directly. If omitted,
            the shared cached seqcol data is used.

    Returns:
        The chrom sizes dict, or ``None`` if the alias has no canonical digest or
        that digest is not present in the loaded data.
    """
    digest = CANONICAL_GENOME_DIGESTS.get(genome_alias)
    if digest is None:
        return None

    if genome_models is not None:
        for genome_model in genome_models:
            if genome_model.genome_digest == digest:
                return genome_model.chrom_sizes
        return None

    for genome in _load_genomes_cached().genomes:
        if genome.digest == digest:
            return {seq.name: seq.length for seq in genome.collection}
    return None


def update_db_genomes(bbagent: BedBaseAgent) -> None:
    """
    Update the database with the latest genome information from Refgenie.
    This function fetches the sequence collections and updates the database accordingly.
    """

    _LOGGER.info("Updating database with genome information from Refgenie...")

    from bbconf.db_utils import ReferenceGenome, Session, select

    genome_list = get_chrom_sizes()

    with Session(bbagent.bed._sa_engine) as session:
        available_genomes_statement = select(ReferenceGenome.digest)
        available_genomes_return = session.execute(available_genomes_statement).all()
        available_genomes_list = [item[0] for item in available_genomes_return]

        for genome in genome_list:
            if genome.genome_digest in available_genomes_list:
                continue

            new_genome = ReferenceGenome(
                alias=genome.genome_alias,
                digest=genome.genome_digest,
            )

            session.add(new_genome)
        session.commit()

    _LOGGER.info("Database update completed.")
