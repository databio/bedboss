"""
Basic analysis and visualization of output.csv (ENCODE BED file metadata).
"""

import matplotlib
import matplotlib.pyplot as plt
import pandas as pd

matplotlib.use("Agg")
from pathlib import Path

import numpy as np

OUTPUT_DIR = Path("analysis_output_csv")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ── Load data ──────────────────────────────────────────────────────────────────
print("Loading data...")
df = pd.read_csv("encode_bed_hg38.csv", low_memory=False)
print(f"Total rows: {len(df):,}")
print(f"Columns: {list(df.columns)}\n")

# ── 1. Basic summary statistics ────────────────────────────────────────────────
print("=" * 60)
print("BASIC SUMMARY")
print("=" * 60)
print(f"Unique samples:     {df['sample_name'].nunique():,}")
print(f"Unique experiments: {df['experiment_id'].nunique():,}")
print(f"Unique assays:      {df['assay'].nunique():,}")
print(f"Unique genomes:     {df['genome'].nunique():,}")
print()

print("Missing values per column:")
print(df.isnull().sum().to_string())
print()

# ── 2. File size statistics ────────────────────────────────────────────────────
print("=" * 60)
print("FILE SIZE STATISTICS (bytes)")
print("=" * 60)
print(df["file_size"].describe().to_string())
print(f"\nTotal data volume: {df['file_size'].sum() / 1e12:.2f} TB")
print()

# ── 3. Assay distribution ─────────────────────────────────────────────────────
assay_counts = df["assay"].value_counts()
print("=" * 60)
print("ASSAY DISTRIBUTION (top 20)")
print("=" * 60)
print(assay_counts.head(20).to_string())
print()

fig, ax = plt.subplots(figsize=(12, 6))
top_assays = assay_counts.head(15)
bars = ax.barh(range(len(top_assays)), top_assays.values, color="steelblue")
ax.set_yticks(range(len(top_assays)))
ax.set_yticklabels(top_assays.index)
ax.set_xlabel("Number of samples")
ax.set_title("Top 15 Assay Types")
ax.invert_yaxis()
for bar, val in zip(bars, top_assays.values):
    ax.text(
        val + max(top_assays.values) * 0.01,
        bar.get_y() + bar.get_height() / 2,
        f"{val:,}",
        va="center",
        fontsize=9,
    )
plt.tight_layout()
plt.savefig(OUTPUT_DIR / "01_assay_distribution.png", dpi=150)
plt.close()

# ── 4. Genome distribution ────────────────────────────────────────────────────
genome_counts = df["genome"].value_counts()
print("=" * 60)
print("GENOME DISTRIBUTION")
print("=" * 60)
print(genome_counts.to_string())
print()

fig, ax = plt.subplots(figsize=(8, 5))
ax.bar(genome_counts.index[:10], genome_counts.values[:10], color="coral")
ax.set_xlabel("Genome Assembly")
ax.set_ylabel("Number of samples")
ax.set_title("Genome Assembly Distribution")
plt.xticks(rotation=45, ha="right")
plt.tight_layout()
plt.savefig(OUTPUT_DIR / "02_genome_distribution.png", dpi=150)
plt.close()

# ── 5. File size distribution ─────────────────────────────────────────────────
fig, axes = plt.subplots(1, 2, figsize=(14, 5))

# Histogram (log scale)
sizes_mb = df["file_size"].dropna() / 1e6
axes[0].hist(sizes_mb, bins=100, color="seagreen", edgecolor="white", linewidth=0.3)
axes[0].set_xlabel("File size (MB)")
axes[0].set_ylabel("Count")
axes[0].set_title("File Size Distribution")
axes[0].set_yscale("log")

# Log-scale histogram
log_sizes = np.log10(df["file_size"].dropna())
axes[1].hist(
    log_sizes, bins=100, color="mediumpurple", edgecolor="white", linewidth=0.3
)
axes[1].set_xlabel("log10(file size in bytes)")
axes[1].set_ylabel("Count")
axes[1].set_title("File Size Distribution (log10)")

plt.tight_layout()
plt.savefig(OUTPUT_DIR / "03_file_size_distribution.png", dpi=150)
plt.close()

# ── 6. File size by assay (box plot) ──────────────────────────────────────────
top_n_assays = assay_counts.head(10).index
df_top = df[df["assay"].isin(top_n_assays)].copy()
df_top["file_size_mb"] = df_top["file_size"] / 1e6

fig, ax = plt.subplots(figsize=(14, 6))
assay_order = (
    df_top.groupby("assay")["file_size_mb"].median().sort_values(ascending=False).index
)
df_top["assay"] = pd.Categorical(df_top["assay"], categories=assay_order, ordered=True)
df_top.boxplot(column="file_size_mb", by="assay", ax=ax, vert=True, showfliers=False)
ax.set_xlabel("Assay")
ax.set_ylabel("File size (MB)")
ax.set_title("File Size by Assay Type (top 10, outliers hidden)")
fig.suptitle("")
plt.xticks(rotation=45, ha="right")
plt.tight_layout()
plt.savefig(OUTPUT_DIR / "04_file_size_by_assay.png", dpi=150)
plt.close()

# ── 7. Tissue distribution ────────────────────────────────────────────────────
tissue_counts = df["tissue"].dropna().value_counts()
print("=" * 60)
print("TOP 20 TISSUES")
print("=" * 60)
print(tissue_counts.head(20).to_string())
print()

fig, ax = plt.subplots(figsize=(12, 7))
top_tissues = tissue_counts.head(20)
bars = ax.barh(range(len(top_tissues)), top_tissues.values, color="goldenrod")
ax.set_yticks(range(len(top_tissues)))
ax.set_yticklabels(top_tissues.index, fontsize=9)
ax.set_xlabel("Number of samples")
ax.set_title("Top 20 Tissues")
ax.invert_yaxis()
plt.tight_layout()
plt.savefig(OUTPUT_DIR / "05_tissue_distribution.png", dpi=150)
plt.close()

# ── 8. Cell line distribution ─────────────────────────────────────────────────
cell_line_counts = df["cell_line"].dropna().value_counts()
print("=" * 60)
print("TOP 20 CELL LINES")
print("=" * 60)
print(cell_line_counts.head(20).to_string())
print()

fig, ax = plt.subplots(figsize=(12, 6))
top_cells = cell_line_counts.head(20)
bars = ax.barh(range(len(top_cells)), top_cells.values, color="salmon")
ax.set_yticks(range(len(top_cells)))
ax.set_yticklabels(top_cells.index, fontsize=9)
ax.set_xlabel("Number of samples")
ax.set_title("Top 20 Cell Lines")
ax.invert_yaxis()
plt.tight_layout()
plt.savefig(OUTPUT_DIR / "06_cell_line_distribution.png", dpi=150)
plt.close()

# ── 9. Target distribution (e.g., TF binding targets) ─────────────────────────
target_counts = df["target"].dropna().value_counts()
print("=" * 60)
print("TOP 20 TARGETS")
print("=" * 60)
print(target_counts.head(20).to_string())
print()

if len(target_counts) > 0:
    fig, ax = plt.subplots(figsize=(12, 7))
    top_targets = target_counts.head(20)
    bars = ax.barh(range(len(top_targets)), top_targets.values, color="teal")
    ax.set_yticks(range(len(top_targets)))
    ax.set_yticklabels(top_targets.index, fontsize=9)
    ax.set_xlabel("Number of samples")
    ax.set_title("Top 20 Targets")
    ax.invert_yaxis()
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / "07_target_distribution.png", dpi=150)
    plt.close()

# ── 10. Assay × Genome heatmap ────────────────────────────────────────────────
cross = pd.crosstab(df["assay"], df["genome"])
top_assays_for_heat = assay_counts.head(15).index
top_genomes = genome_counts.head(10).index
cross_sub = cross.loc[
    cross.index.isin(top_assays_for_heat),
    cross.columns.isin(top_genomes),
]
if cross_sub.shape[0] > 1 and cross_sub.shape[1] > 1:
    fig, ax = plt.subplots(figsize=(10, 7))
    im = ax.imshow(np.log10(cross_sub.values + 1), aspect="auto", cmap="YlOrRd")
    ax.set_xticks(range(cross_sub.shape[1]))
    ax.set_xticklabels(cross_sub.columns, rotation=45, ha="right")
    ax.set_yticks(range(cross_sub.shape[0]))
    ax.set_yticklabels(cross_sub.index)
    ax.set_title("Assay × Genome (log10 count)")
    plt.colorbar(im, ax=ax, label="log10(count + 1)")
    # annotate cells
    for i in range(cross_sub.shape[0]):
        for j in range(cross_sub.shape[1]):
            val = cross_sub.iloc[i, j]
            if val > 0:
                ax.text(
                    j,
                    i,
                    f"{val:,}",
                    ha="center",
                    va="center",
                    fontsize=7,
                    color="white" if np.log10(val + 1) > 3 else "black",
                )
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / "08_assay_genome_heatmap.png", dpi=150)
    plt.close()

# ── 11. Samples with treatment ────────────────────────────────────────────────
treated = df["treatment"].notna().sum()
untreated = df["treatment"].isna().sum()
print("=" * 60)
print("TREATMENT")
print("=" * 60)
print(f"With treatment:    {treated:,} ({100*treated/len(df):.1f}%)")
print(f"Without treatment: {untreated:,} ({100*untreated/len(df):.1f}%)")
print()

fig, ax = plt.subplots(figsize=(6, 6))
ax.pie(
    [untreated, treated],
    labels=["No treatment", "With treatment"],
    autopct="%1.1f%%",
    colors=["lightsteelblue", "tomato"],
    startangle=90,
)
ax.set_title("Samples With vs Without Treatment")
plt.tight_layout()
plt.savefig(OUTPUT_DIR / "09_treatment_pie.png", dpi=150)
plt.close()

# ── 12. Assay counts over experiment scale ────────────────────────────────────
samples_per_exp = df.groupby("experiment_id").size()
print("=" * 60)
print("SAMPLES PER EXPERIMENT")
print("=" * 60)
print(samples_per_exp.describe().to_string())
print()

fig, ax = plt.subplots(figsize=(10, 5))
ax.hist(
    samples_per_exp.values,
    bins=range(1, samples_per_exp.max() + 2),
    color="slategray",
    edgecolor="white",
    linewidth=0.3,
)
ax.set_xlabel("Samples per experiment")
ax.set_ylabel("Number of experiments")
ax.set_title("Distribution of Samples per Experiment")
ax.set_yscale("log")
plt.tight_layout()
plt.savefig(OUTPUT_DIR / "10_samples_per_experiment.png", dpi=150)
plt.close()

# ── 13. Duplicate analysis (sample_name + file_size) ──────────────────────────
print("=" * 60)
print("DUPLICATE / NON-UNIQUE DATA ANALYSIS")
print("=" * 60)

# Duplicate sample_name
dup_name = df["sample_name"].duplicated(keep=False)
n_dup_name = dup_name.sum()
n_unique_name = len(df) - n_dup_name
print(f"Duplicate sample_name rows:  {n_dup_name:,} ({100*n_dup_name/len(df):.2f}%)")
print(f"Unique sample_name rows:     {n_unique_name:,}")

# Duplicate file_size
dup_size = df["file_size"].duplicated(keep=False)
n_dup_size = dup_size.sum()
print(f"Duplicate file_size rows:    {n_dup_size:,} ({100*n_dup_size/len(df):.2f}%)")

# Duplicate (sample_name, file_size) pairs
dup_both = df.duplicated(subset=["sample_name", "file_size"], keep=False)
n_dup_both = dup_both.sum()
print(f"Duplicate (name+size) rows:  {n_dup_both:,} ({100*n_dup_both/len(df):.2f}%)")
print()

# How many distinct sample_names appear more than once?
name_counts = df["sample_name"].value_counts()
repeated_names = name_counts[name_counts > 1]
print(f"sample_names appearing >1 time: {len(repeated_names):,}")
if len(repeated_names) > 0:
    print(f"  Max occurrences: {repeated_names.iloc[0]} ('{repeated_names.index[0]}')")
    print("  Top 10 repeated sample_names:")
    print(repeated_names.head(10).to_string())
print()

# How many distinct file_sizes appear more than once?
size_counts = df["file_size"].value_counts()
repeated_sizes = size_counts[size_counts > 1]
print(f"file_sizes appearing >1 time: {len(repeated_sizes):,}")
if len(repeated_sizes) > 0:
    print(
        f"  Max occurrences: {repeated_sizes.iloc[0]} (size={repeated_sizes.index[0]})"
    )
print()

# ── Plot ──
fig, axes = plt.subplots(1, 3, figsize=(18, 6))

# Panel 1: Pie — unique vs duplicate sample_name
axes[0].pie(
    [n_unique_name, n_dup_name],
    labels=["Unique sample_name", "Duplicate sample_name"],
    autopct="%1.2f%%",
    colors=["#66b3ff", "#ff6666"],
    startangle=90,
)
axes[0].set_title("sample_name Uniqueness")

# Panel 2: Distribution of how many times a sample_name repeats
repeat_dist = name_counts.value_counts().sort_index()
axes[1].bar(
    repeat_dist.index, repeat_dist.values, color="darkorange", edgecolor="white"
)
axes[1].set_xlabel("Times a sample_name appears")
axes[1].set_ylabel("Number of distinct sample_names")
axes[1].set_title("sample_name Repetition Distribution")
axes[1].set_yscale("log")
for x, y in zip(repeat_dist.index, repeat_dist.values):
    axes[1].text(x, y * 1.1, f"{y:,}", ha="center", va="bottom", fontsize=8)

# Panel 3: Distribution of how many times a file_size repeats
size_repeat_dist = size_counts.value_counts().sort_index()
top_size_repeat = size_repeat_dist[size_repeat_dist.index <= 50]  # cap x-axis
axes[2].bar(
    top_size_repeat.index,
    top_size_repeat.values,
    color="mediumseagreen",
    edgecolor="white",
)
axes[2].set_xlabel("Times a file_size appears")
axes[2].set_ylabel("Number of distinct file_sizes")
axes[2].set_title("file_size Repetition Distribution (count ≤ 50)")
axes[2].set_yscale("log")

plt.tight_layout()
plt.savefig(OUTPUT_DIR / "11_duplicate_analysis.png", dpi=150)
plt.close()

# ── 14. Date created analysis ────────────────────────────────────────────────
print("=" * 60)
print("DATE CREATED ANALYSIS")
print("=" * 60)

df["date_created"] = pd.to_datetime(df["date_created"], errors="coerce", utc=True)
valid_dates = df["date_created"].notna().sum()
print(f"Valid dates: {valid_dates:,} / {len(df):,}")
if valid_dates > 0:
    print(f"Earliest: {df['date_created'].min()}")
    print(f"Latest:   {df['date_created'].max()}")
    print()

    # Monthly counts
    df["year_month"] = df["date_created"].dt.to_period("M")
    monthly = df.groupby("year_month").size()

    print("SAMPLES PER MONTH (top 20)")
    print(monthly.sort_values(ascending=False).head(20).to_string())
    print()

    # Yearly counts
    df["year"] = df["date_created"].dt.year
    yearly = df.groupby("year").size()
    print("SAMPLES PER YEAR")
    print(yearly.to_string())
    print()

    # Plot 1: Monthly timeline
    fig, ax = plt.subplots(figsize=(14, 5))
    monthly_sorted = monthly.sort_index()
    x_labels = [str(p) for p in monthly_sorted.index]
    ax.bar(
        range(len(monthly_sorted)),
        monthly_sorted.values,
        color="steelblue",
        edgecolor="white",
        linewidth=0.3,
    )
    # Show every Nth label to avoid overlap
    n_ticks = min(20, len(x_labels))
    step = max(1, len(x_labels) // n_ticks)
    ax.set_xticks(range(0, len(x_labels), step))
    ax.set_xticklabels(
        [x_labels[i] for i in range(0, len(x_labels), step)],
        rotation=45,
        ha="right",
        fontsize=8,
    )
    ax.set_xlabel("Month")
    ax.set_ylabel("Number of samples")
    ax.set_title("Samples Created per Month")
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / "12_date_created_monthly.png", dpi=150)
    plt.close()

    # Plot 2: Yearly bar chart
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.bar(yearly.index.astype(int), yearly.values, color="coral", edgecolor="white")
    ax.set_xlabel("Year")
    ax.set_ylabel("Number of samples")
    ax.set_title("Samples Created per Year")
    for x, y in zip(yearly.index.astype(int), yearly.values):
        ax.text(
            x,
            y + max(yearly.values) * 0.01,
            f"{y:,}",
            ha="center",
            va="bottom",
            fontsize=9,
        )
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / "13_date_created_yearly.png", dpi=150)
    plt.close()

    # Plot 3: Cumulative samples over time
    fig, ax = plt.subplots(figsize=(14, 5))
    cumulative = monthly_sorted.cumsum()
    ax.fill_between(
        range(len(cumulative)), cumulative.values, color="mediumpurple", alpha=0.5
    )
    ax.plot(
        range(len(cumulative)), cumulative.values, color="mediumpurple", linewidth=1.5
    )
    ax.set_xticks(range(0, len(x_labels), step))
    ax.set_xticklabels(
        [x_labels[i] for i in range(0, len(x_labels), step)],
        rotation=45,
        ha="right",
        fontsize=8,
    )
    ax.set_xlabel("Month")
    ax.set_ylabel("Cumulative samples")
    ax.set_title("Cumulative Samples Over Time")
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / "14_date_created_cumulative.png", dpi=150)
    plt.close()

    # Plot 4: Top 5 assays over time (yearly stacked)
    top5_assays = assay_counts.head(5).index
    df_top5 = df[df["assay"].isin(top5_assays) & df["year"].notna()]
    if len(df_top5) > 0:
        assay_year = df_top5.groupby(["year", "assay"]).size().unstack(fill_value=0)
        fig, ax = plt.subplots(figsize=(12, 6))
        assay_year.plot(kind="bar", stacked=True, ax=ax, colormap="tab10")
        ax.set_xlabel("Year")
        ax.set_ylabel("Number of samples")
        ax.set_title("Top 5 Assays by Year")
        ax.legend(title="Assay", bbox_to_anchor=(1.05, 1), loc="upper left")
        plt.tight_layout()
        plt.savefig(OUTPUT_DIR / "15_assay_by_year.png", dpi=150)
        plt.close()

    # Clean up temp columns
    df.drop(columns=["year_month", "year"], inplace=True)

print()

# ── Done ───────────────────────────────────────────────────────────────────────
print("=" * 60)
print(f"All plots saved to: {OUTPUT_DIR}/")
print("=" * 60)
