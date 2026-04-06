from pathlib import Path

from gtars.models import RegionSet

rs = RegionSet("/home/bnt4me/Downloads/combined_unsorted.bed")
rs = RegionSet("/home/bnt4me/virginia/rustlings/sorted.bed")


path = Path("/home/bnt4me/virginia/rustlings/sorted.bed")

size = Path("/home/bnt4me/virginia/rustlings/hg38.chrom.sizes")
