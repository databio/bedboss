from geofetch import Finder

gse_obj = Finder()

# Optionally: provide filter string and max number of retrieve elements
gse_obj = Finder(
    filters="((bed) OR narrow peak) AND Drosophila melanogaster[Organism]", retmax=10
)
gse_list = gse_obj.get_gse_by_date(start_date="2016/08/01", end_date="2020/08/01")

gse_obj.generate_file(
    "/home/drc/GITHUB/bedboss/bedboss/scripts/ref_genome_validating/data/fly/bedfileslist.txt"
)
