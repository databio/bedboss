from bbconf import BedBaseAgent


def upload_references():
    from bedboss.refgenome_validator.refgenie_chrom_sizes import update_db_genomes

    bbagent = BedBaseAgent("/home/bnt4me/virginia/repos/bedboss/config.yaml")
    update_db_genomes(bbagent)


if __name__ == "__main__":

    upload_references()
