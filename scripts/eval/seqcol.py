# THIS SCRIPT IS WORKING SPACE TO RETRIEVE ALL SEQCLOS FROM REFGENIE

import os
import requests
from pydantic import BaseModel
import json
from typing import List, Union

from bbconf import BedBaseAgent

bbagent = BedBaseAgent("/home/bnt4me/virginia/repos/bbuploader/config_db_local.yaml")


from bedboss.refgenome_validator.genome_model import GenomeModel
from bedboss.refgenome_validator.main import ReferenceValidator


BASE_URL = "https://api.refgenie.org"
SEQ_COL_URL = "https://api.refgenie.org/seqcol/collection/{digest}?collated=true&attribute=name_length_pairs"


identifier = "dcc005e8761ad5599545cc538f6a2a4d"
bed_file_path = f"/home/bnt4me/Downloads/{identifier}.bed.gz"


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


def run_requests(url, timeout=10) -> dict:
    """
    Run a GET request to the specified URL and return the response.
    """
    try:
        response = requests.get(url, timeout=timeout)
        response.raise_for_status()  # Raise an error for bad responses
        return response.json()
    except requests.RequestException as e:
        print(f"Error fetching data from {url}: {e}")
        return None


def get_genome_list() -> dict:
    genome_url = os.path.join(BASE_URL, "v4/genomes")
    genome_data = run_requests(genome_url)

    list_of_genomes = [k["digest"] for k in genome_data]

    string = ""

    for genome in list_of_genomes:
        string += f"{genome}\n"

    return genome_data


def seq_col_from_digest(digest: str) -> List[SeqCol]:
    url = SEQ_COL_URL.format(digest=digest)

    response = run_requests(url)
    return_list = []

    for item in response:
        return_list.append(SeqCol(**item))

    return return_list


def get_seq_col():

    genomes = get_genome_list()

    return_list = []

    for info in genomes[400:-1]:
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
    with open(output_path, "w") as f:
        json.dump(genomes.model_dump(), f, indent=4)
    print(f"Saved sequence collections to {output_path}")


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
            GenomeModel(genome_alias=genome.genome, chrom_sizes_file=g_size)
        )

    return genome_list


if __name__ == "__main__":
    ret = get_seq_col()
    # save_seq_col_to_json(ret, output_path="genome_seqcol.json")
    modified_list = modify_for_analysis(ret)
    modified_list

    rv = ReferenceValidator(
        genome_models=modified_list,
    )

    compat = rv.determine_compatibility(bed_file_path, concise=True)
    compatitil = {}

    for k, v in compat.items():
        if v.tier_ranking < 4:
            compatitil[k] = v

    import pprint

    pp = pprint.pprint(compatitil)

    bbagent.bed.update(
        identifier=identifier,
        ref_validation=compatitil,
        upload_pephub=False,
    )
