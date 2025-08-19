# THIS SCRIPT IS WORKING SPACE TO RETRIEVE ALL SEQCLOS FROM REFGENIE

import os
import requests
from pydantic import BaseModel
import json
from typing import List, Union
from tqdm import tqdm

from bbconf import BedBaseAgent

from bedboss.refgenome_validator.genome_model import GenomeModel
from bedboss.refgenome_validator.main import ReferenceValidator


bbagent = BedBaseAgent("/home/bnt4me/virginia/repos/bedhost/config.yaml")

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

    for info in tqdm(genomes[500:-1], desc="Downloading genomes from refgenie..."):
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

    print("Saving Refgenie genomes for later reuse...")
    with open(output_path, "w") as f:
        json.dump(genomes.model_dump(), f, indent=4)
    print(f"Saved sequence collections to {output_path}")


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


if __name__ == "__main__":
    try:
        ret = read_seq_col_from_json()
    except FileNotFoundError:
        print("No genome_seqcol.json found, downloading from refgenie...")
        ret = get_seq_col()
        save_seq_col_to_json(ret, output_path="genome_seqcol.json")
    modified_list = modify_for_analysis(ret)

    modified_list

    from bbconf.db_utils import Session, ReferenceGenome

    ####
    # for genome in modified_list:
    #
    #     with Session(bbagent.bed._sa_engine) as session:
    #         new_genome = ReferenceGenome(
    #             alias=genome.genome_alias,
    #             digest=genome.genome_digest,
    #         )
    #         session.add(new_genome)
    #         session.commit()

    ###

    rv = ReferenceValidator(
        genome_models=modified_list,
    )

    import time

    start_time = time.time()

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
    end_time = time.time()

    print(f"Time taken: {end_time - start_time} seconds")


# def get_chrom_genome_index(list_of_genomes: Genomes) -> dict:
#     """
#     Create a dictionary mapping chromosome names to their respective genomes.
#
#     Args:
#         list_of_genomes: Genomes object containing sequence collections
#
#     Returns:
#         Dictionary with chromosome names as keys and lists of genomes as values
#     """
#     chrom_genome_index = {}
#
#     for genome in list_of_genomes.genomes:
#         for seq in genome.collection:
#             if seq.name not in chrom_genome_index:
#                 chrom_genome_index[seq.name] = []
#             chrom_genome_index[seq.name].append(genome.genome)
#
#     return chrom_genome_index
#
#
# chr_list = ["chr2", "chr3", "chr4", "chrBFD", "chr1"]
# genome_chr_dict = {
#     "chr1": ["hg38", "fi2", "mm20"],
#     "BNM": ["aklsdjf1j", "adsf1aew"],
#     "chr2": ["hg38", "hg28", "mm334"],
# }
#
# # output:
# genomes_to_compare = ["hg38", "hg28", "mm20"]
#
#
# ret = read_seq_col_from_json()
# genome_chr_dict = get_chrom_genome_index(ret)
#
# def get_genomes_for_chr(chr_list: list, genome_Chr_dict: dict) -> dict:
#     result = set()
#     for chr_name in chr_list:
#         if chr_name in genome_Chr_dict:
#             result.update(genome_Chr_dict[chr_name])
#
#     return result
#
# if __name__ == "__main__":
#     import time
#
#     start_time = time.time()
#     result = get_genomes_for_chr(chr_list, genome_chr_dict)
#     end_time = time.time()
#     print(f"Result: {result}")
#     print(f"Time taken: {end_time - start_time:.4f} seconds")
#     # Output: {'chr2': ['hg38', 'hg28', 'mm20']}
