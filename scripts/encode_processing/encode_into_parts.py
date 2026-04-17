import polars as pl


def divide_csv(file_path):
    df = pl.read_csv(file_path)

    df = df.with_row_index("index")

    n_parts = 15
    chunk_size = len(df) // n_parts

    for i in range(n_parts):
        part = df.filter(
            (pl.col("index") >= i * chunk_size)
            & (pl.col("index") < (i + 1) * chunk_size)
        )
        part.write_csv(f"part_{i}.csv")


if __name__ == "__main__":
    divide_csv(
        "/home/bnt4me/virginia/repos/bedboss/scripts/encode_processing/encode_bed_hg38.csv"
    )
