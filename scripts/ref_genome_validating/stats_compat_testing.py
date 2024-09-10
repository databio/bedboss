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
    mouse_keys = [" mm10"]

    results_path = os.path.abspath("stats_results")
    project = pephubclient.PEPHubClient()
    peppyproject = project.load_project(project_registry_path=PEP_URL)
    df = copy.deepcopy(peppyproject.sample_table)
    import re

    df["reported_ref_genome"] = (
        df["reported_ref_genome"].astype(str).str.strip().str.upper()
    )
    df["reported_ref_genome"] = (
        df["reported_ref_genome"].astype(str).str.replace(r"\s+", "", regex=True)
    )  # remove the leading character, it causes issues
    # df["reported_ref_genome"] = df["reported_ref_genome"].astype(str).str.replace(
    #     "GRCH38", "human"
    # )
    # Standardize the human data
    for human_designator in human_keys:
        df["reported_ref_genome"] = (
            df["reported_ref_genome"]
            .astype(str)
            .str.replace(human_designator.upper(), "human")
        )
    #     df['reported_ref_genome'] = df['reported_ref_genome'].replace({" GRCh38": 'human'})
    # df['reported_ref_genome'] = df['reported_ref_genome'].apply(lambda x: 'human' if x in human_keys else x)
    # df['reported_ref_genome'] = df['reported_ref_genome'].str.lower().replace({human_designator: 'human'})

    print(df["reported_ref_genome"].head())
    # df.to_csv("/home/drc/Downloads/export_test.csv")


if __name__ == "__main__":
    main()
