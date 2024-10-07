import os

CHROM_SIZES_DIRECTORY = (
    "/home/drc/GITHUB/bedboss/bedboss/scripts/ref_genome_validating/chrom_sizes"
)


def count_duplicates(directory):
    """Counts duplicate chromosome sizes in multiple chrom.sizes files."""

    sizes = []
    for filename in os.listdir(directory):
        if filename.endswith(".chrom.sizes"):
            with open(os.path.join(directory, filename), "r") as f:
                for line in f:
                    size = int(line.split()[1])
                    sizes.append(size)

    size_counts = {}
    for size in sizes:
        size_counts[size] = size_counts.get(size, 0) + 1

    return size_counts


directory = CHROM_SIZES_DIRECTORY
duplicates = count_duplicates(directory)

print("Duplicate chromosome sizes:")
for size, count in sorted(duplicates.items()):
    if count > 1:
        print(f"{size}: {count}")
