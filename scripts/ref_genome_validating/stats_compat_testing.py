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
    PEP_URL = "donaldcampbelljr/ref_genome_compat_testing_small:default"


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

    print(df)
    df = df.reset_index(drop=True)  # do this to get rid of extra sample_name columns
    print(df)

    # df["reported_ref_genome"] = (
    #     df["reported_ref_genome"].astype(str).str.strip().str.upper()
    # )
    # df["reported_ref_genome"] = (
    #     df["reported_ref_genome"].astype(str).str.replace(r"\s+", "", regex=True)
    # )  # remove the leading character, it causes issues
    #
    # # Standardize hg38 and mm10 for now, ignore entries that have two genomes
    # # Standardize the human hg38 data
    # for human_designator in human_hg38_keys:
    #     df["reported_ref_genome"] = (
    #         df["reported_ref_genome"]
    #         .astype(str)
    #         .str.replace(human_designator.upper(), "hg38")
    #     )
    #
    # # Standardize the mouse data
    # for mouse_designator in mouse_keys:
    #     df["reported_ref_genome"] = (
    #         df["reported_ref_genome"]
    #         .astype(str)
    #         .str.replace(mouse_designator.upper(), "mm10")
    #     )
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
    # print(df["reported_ref_genome"].head())

    # Why don't these three attempts work?
    # hg38_data = df[df["reported_ref_genome"].astype(str).str.strip() == "hg38"]
    # hg38_data = df.query('reported_ref_genome == hg38')
    # hg38_data = df[df["reported_ref_genome"].isin(["hg38"])]

    # But this one does?
    # hg38_data = df[df["reported_ref_genome"].str.contains("hg38")]
    # mm10_data = df[df["reported_ref_genome"].str.contains("mm10")]

    # hg38_data.to_csv("/home/drc/Downloads/hg38_export_test.csv")
    # print(type(df["reported_ref_genome"].iat[1]))
    # df_tiers = df[["sample_name", "tier_rating"]]

    mm10_values = []
    hg38_values = []
    hg19_values = []
    dm6_values = []
    ucsc_hg38_values = []
    ucsc_pantro6_values = []
    for index, row in df.iterrows():
        (
            mm10_tier,
            hg38_tier,
            hg19_tier,
            ucsc_dm6_tier,
            ucsc_hg38_tier,
            ucsc_pantro6_tier,
        ) = extract_values(row["tier_rating"])
        mm10_values.append(mm10_tier)
        hg38_values.append(hg38_tier)
        hg19_values.append(hg19_tier)
        dm6_values.append(ucsc_dm6_tier)
        ucsc_hg38_values.append(ucsc_hg38_tier)
        ucsc_pantro6_values.append(ucsc_pantro6_tier)

    # df_tiers.rename(columns={0: 'tier_rating'}, inplace=True)
    # df_tiers['mm10_tier'] = None
    # df_tiers['hg38_tier'] = None
    # df_tiers[['mm10_tier', 'hg38_tier']] = df_tiers['tier_rating'].apply(lambda x: extract_values(x))
    # results1 = df_tiers.apply(lambda x: extract_values(x))
    # df_numeric = df_tiers.applymap(lambda x: x['mm10']['tier_ranking'])
    # print(mm10_values)

    df2 = pd.DataFrame().assign(sample_name=df["sample_name"])

    df2["hg38_ncbi_rating"] = hg38_values
    df2["hg38_ucsc_rating"] = ucsc_hg38_values
    df2["hg19_ucsc_rating"] = hg19_values
    df2["pantro6_ucsc_rating"] = ucsc_pantro6_values
    df2["mm10_ncbi_rating"] = mm10_values
    df2["dm6_ucsc_rating"] = dm6_values

    print(df2)
    df2.to_csv("/home/drc/Downloads/export_test.csv")

    # make heatmap
    create_heatmap(df2)

    # print(df_tiers)
    # print(df_tiers.head())


def extract_values(dictionary):
    import json

    dictionary = json.loads(dictionary)

    mm10_tier = dictionary["mm10"]["tier_ranking"]
    hg38_tier = dictionary["hg38"]["tier_ranking"]
    hg19_tier = dictionary["hg19"]["tier_ranking"]
    ucsc_dm6_tier = dictionary["ucsc_dm6"]["tier_ranking"]
    ucsc_hg38_tier = dictionary["ucsc_hg38"]["tier_ranking"]
    ucsc_pantro6_tier = dictionary["ucsc_pantro6"]["tier_ranking"]

    return (
        mm10_tier,
        hg38_tier,
        hg19_tier,
        ucsc_dm6_tier,
        ucsc_hg38_tier,
        ucsc_pantro6_tier,
    )


def create_heatmap(df):
    num_bins = 4
    min_val = 1
    max_val = 4
    bounds = np.linspace(min_val, max_val, 15)
    colors = [*sns.color_palette("mako_r", num_bins)]  # Lightgray for -1
    cmap = mcolors.LinearSegmentedColormap.from_list("mycmap", colors)
    norm = mcolors.BoundaryNorm(bounds, cmap.N)

    # df_pivoted = df.pivot(index='sample_name', columns="hg38_ncbi_rating")

    df["sample_name"] = [
        "Homosapiens (hg38)",
        "Homosapiens (hg19)",
        "Pan troglodytes (panTro6)",
        "Mus Musculus (mm10)",
        "Drosophila melanogaster (dm6)",
    ]

    df.set_index(["sample_name", df.index], inplace=True)
    # Create the heatmap
    plt.figure(figsize=(50, 30))
    # plt.yticks(rotation=30, ha="right")
    ax = sns.heatmap(
        df,
        cmap=cmap,
        norm=norm,
        annot=False,
        linewidths=2,
    )

    ax.tick_params(axis="x", labelsize=10)
    ax.tick_params(axis="y", labelsize=10)
    ax.set_xticklabels(ax.get_xticklabels(), rotation=30)
    ax.set_yticklabels(ax.get_yticklabels(), rotation=30)
    plt.title("Tier Rating: Bed File vs Ref Genome")
    plt.xlabel("Reference Genomes", fontsize=12)
    plt.ylabel("Query Bed Files", fontsize=12)
    # plt.grid(True)
    plt.show()

    pass


if __name__ == "__main__":
    main()
