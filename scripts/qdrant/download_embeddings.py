from bedboss.scripts import make_umap


if __name__ == "__main__":
    make_umap.get_embeddings(
        "/home/bnt4me/virginia/repos/bedhost/config.yaml",
        output_file="test_umap2d.json",
        n_components=2,
        plot_name="test_umap2d.svg",
        plot_label="cell_line",
        top_assays=15,
        top_cell_lines=15,
    )
