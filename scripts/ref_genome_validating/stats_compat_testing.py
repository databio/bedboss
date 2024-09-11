# This script is for creating graphs for the ref_genome compat testing
# PEP : donaldcampbelljr/refgenome_compat_testing:default
import copy
import os
import numpy as np
import requests
import pephubclient
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
from scipy.cluster.hierarchy import linkage

try:
    PEP_URL = os.environ["PEP_URL"]
except:
    # pep url
    PEP_URL = "donaldcampbelljr/refgenome_compat_testing:default"


def main():

    # pull from tier rating column to get the final assessment
    tier_rating_keys = ["mm10", "hg38", "hg19"]

    # reported_ref_genome column should have one of these values
    human_keys = [
        "GRCh38",
        "hg38",
        "hg19",
        "GRCh38/hg38",
        "grch38",
        "grch38/hg38",
    ]
    human_hg38_keys = [
        "GRCh38",
        "hg38",
        "GRCh38/hg38",
        "grch38",
        "grch38/hg38",
    ]
    mouse_keys = ["mm10"]

    results_path = os.path.abspath("stats_results")
    project = pephubclient.PEPHubClient()
    peppyproject = project.load_project(project_registry_path=PEP_URL)
    df = copy.deepcopy(peppyproject.sample_table)

    df["reported_ref_genome"] = (
        df["reported_ref_genome"].astype(str).str.strip().str.upper()
    )
    df["reported_ref_genome"] = (
        df["reported_ref_genome"].astype(str).str.replace(r"\s+", "", regex=True)
    )  # remove the leading character, it causes issues

    # Standardize hg38 and mm10 for now, ignore entries that have two genomes
    # Standardize the human hg38 data
    for human_designator in human_hg38_keys:
        df["reported_ref_genome"] = (
            df["reported_ref_genome"]
            .astype(str)
            .str.replace(human_designator.upper(), "hg38")
        )

    # Standardize the mouse data
    for mouse_designator in mouse_keys:
        df["reported_ref_genome"] = (
            df["reported_ref_genome"]
            .astype(str)
            .str.replace(mouse_designator.upper(), "mm10")
        )
    # print(df["reported_ref_genome"].head(200))
    # print(df.columns)
    # df.to_csv("/home/drc/Downloads/export_test.csv")
    # df2 = pd.read_csv("/home/drc/Downloads/export_test.csv")

    result = extract_tier_ratings(df)


def extract_tier_ratings(df):
    """
    Extracts the tier ratings from one of the columns depending on if the genome is one of the desired (e.g. hg38, mm10)

    :param df
    :returns dict
    """

    # Convert tier_rating column to dict
    # df['tier_rating'] = df['tier_rating'].apply(lambda x: eval(x))
    # df["reported_ref_genome"] = df["reported_ref_genome"].astype(str).str.strip()
    # df["reported_ref_genome"] = df["reported_ref_genome"].apply(lambda s: s.astype(str))
    print(df["reported_ref_genome"].head())

    # Why don't these three attempts work?
    # hg38_data = df[df["reported_ref_genome"].astype(str).str.strip() == "hg38"]
    # hg38_data = df.query('reported_ref_genome == hg38')
    # hg38_data = df[df["reported_ref_genome"].isin(["hg38"])]

    # But this one does?
    hg38_data = df[df["reported_ref_genome"].str.contains("hg38")]
    mm10_data = df[df["reported_ref_genome"].str.contains("mm10")]

    # hg38_data.to_csv("/home/drc/Downloads/hg38_export_test.csv")
    # print(type(df["reported_ref_genome"].iat[1]))

    print(hg38_data)


if __name__ == "__main__":
    main()
