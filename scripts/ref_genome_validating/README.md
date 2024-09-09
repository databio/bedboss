# Testing Scripts for Reference Genome validation and Exclude Ranges Overlap Testing

## Scripts

Note that some of these may have paths hard coded for running locally!

`pull1000bedfiles.py`  -> uses geofetch to pull gses of a certain species and exports gses to a list in a txt file.
`process_exclude_ranges.py` -> takes a gse list and pulls only bed files, for each bedfile runs `igd search` on igd file of excluded ranges bedset. Exports results to PEPHUB.
`stats_exclude_ranges.py` -> processes exclude ranges results from a PEP on PEPhub. Creates heatmaps.

`validate_genome.py` -> takes a directory of bedfiles and attempt to assign compatibility rating to each.

## Steps for running test script validate_genome.py

#### Environment Variable Set Up

Set path to refgenie config file and initialize it:
```console
export REFGENIE=/home/drc/GITHUB/bedboss/bedboss/scripts/ref_genome_validating/genome_folder/genome_config.yaml
refgenie init -c $REFGENIE
```

Ensure [IGD](https://github.com/databio/IGD) is installed and set env variable to executable location:
```console
export IGD_LOCATION = f"/home/drc/GITHUB/igd/IGD/bin/igd"
```

Set location of created igd file (see steps below for creation):
```console
export IGD_DB_PATH = "/home/drc/Downloads/igd_database.igd"
```

Set location for a directory of unzipped bedfiles for testing:
```console
export BEDFILE_DIRECTORY =  "/home/drc/GITHUB/bedboss/bedboss/scripts/ref_genome_validating/results"
```

Set PEPhub url for reporting final results to PEPhub:
```console
export PEP_URL =  "donaldcampbelljr/refgenome_compat_testing:default"
```

To run script
```console

cd /home/drc/GITHUB/bedboss/bedboss/scripts/ref_genome_validating
python3 validate_genome.py
```


### Cache testing bedfiles:

for excludedranges coverage testing

pull 1000 bed files for a few different species (human, mouse, rat, bovine)
`python3 pull1000bedfiles.py -f "((bed) OR narrow peak) AND Bos taurus[Organism]" -s "cow"`

### Create igd from excluded ranges bedset:
cache excluded ranges using geniml
`export BBCLIENT_CACHE="/home/drc/GITHUB/bedboss/bedboss/scripts/ref_genome_validating/data/excludedranges/cachedfiles/"`
`geniml bbclient cache-bedset excluderanges"`

create igd database from excluded ranges cache (must install C version)
```console
./bin/igd create "/home/drc/GITHUB/bedboss/bedboss/scripts/ref_genome_validating/data/excludedranges/cachedfiles/all_beds/" "/home/drc/GITHUB/bedboss/bedboss/scripts/ref_genome_validating/data/excludedranges/igd" "EXCLUDED_RANGES_IGD_DATABASE" 
```
```console
nCtgs, nRegions, nTiles: 37      280204  216259
Save igd database to /home/drc/GITHUB/bedboss/bedboss/scripts/ref_genome_validating/data/excludedranges/igd/EXCLUDED_RANGES_IGD_DATABASE.igd
Total intervals, l_avg:  94982    32011.179
```