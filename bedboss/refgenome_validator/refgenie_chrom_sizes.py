## FILE with functions to download chrom sizes from refgenie and validate them against the genome model

import os
import requests
from pydantic import BaseModel
import json
from typing import List, Union, Any
from tqdm import tqdm
import logging
import warnings

from bedboss.refgenome_validator.genome_model import GenomeModel
from geniml.bbclient.const import DEFAULT_CACHE_FOLDER
from bbconf import BedBaseAgent
from bedboss.const import PKG_NAME
from bedboss.exceptions import BedBossException

BASE_URL = "https://api.refgenie.org"
GENOMES_URL = os.path.join(BASE_URL, "v4/genomes?limit=1000")
SEQ_COL_URL = os.path.join(
    BASE_URL, "seqcol/collection/{digest}?collated=true&attribute=name_length_pairs"
)

_LOGGER = logging.getLogger(PKG_NAME)


class SeqCol(BaseModel):
    length: int
    name: str


class SeqColGenome(BaseModel):
    genome: Union[str, None] = None
    digest: str
    description: str
    collection: List[SeqCol]


class Genomes(BaseModel):
    genomes: List[SeqColGenome]


def run_requests(url, timeout=10) -> Any:
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


def get_genome_list() -> List[dict]:
    """
    Fetch the list of genomes from Refgenie.
    """

    genome_data = run_requests(GENOMES_URL)
    return genome_data.get("items", [])


def seq_col_from_digest(digest: str) -> List[SeqCol]:
    """

    Fetch sequence collection from Refgenie using the genome digest.

    :param digest: The digest of the genome to fetch the sequence collection for.

    :return: A list of SeqCol objects containing sequence names and lengths.
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

    :return: Genomes object containing SeqColGenome objects for each genome.

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
    with open(input_path, "r") as f:
        data = json.load(f)
    return Genomes(**data)


def modify_for_analysis(genomes: Genomes) -> List[GenomeModel]:
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


def get_chrom_sizes() -> list[GenomeModel]:
    """
    Get chromosome sizes from Refgenie and return them as a list of GenomeModel objects.

    If the cached file exists, it reads from there; otherwise, it downloads the data and saves it.
    """
    cached_file_path = os.path.join(DEFAULT_CACHE_FOLDER, "genome_seqcol.json")

    try:
        ret = read_seq_col_from_json(input_path=cached_file_path)
    except FileNotFoundError:
        _LOGGER.info("No genome_seqcol.json found, downloading from refgenie...")
        ret = get_seq_col()

        save_seq_col_to_json(ret, output_path=cached_file_path)
    return modify_for_analysis(ret)


def update_db_genomes(bbagent: BedBaseAgent) -> None:
    """
    Update the database with the latest genome information from Refgenie.
    This function fetches the sequence collections and updates the database accordingly.
    """

    _LOGGER.info("Updating database with genome information from Refgenie...")

    from bbconf.db_utils import Session, ReferenceGenome, select

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
