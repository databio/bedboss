```console
export REFGENIE=/home/drc/GITHUB/bedboss/bedboss/scripts/ref_genome_validating/genome_folder/genome_config.yaml
refgenie init -c $REFGENIE
```

To run script
```console

cd /home/drc/GITHUB/bedboss/bedboss/scripts/ref_genome_validating
python3 validate_genome.py
```


### Set up:

for excludedranges coverage testing

1. pull 1000 bed files for a few different species (human, mouse, rat, bovine)
`python3 pull1000bedfiles.py -f "((bed) OR narrow peak) AND Bos taurus[Organism]" -s "cow"`

2. cache excluded ranges using geniml
`export BBCLIENT_CACHE="/home/drc/GITHUB/bedboss/bedboss/scripts/ref_genome_validating/data/excludedranges/cachedfiles/"`
`geniml bbclient cache-bedset excluderanges"`

3. create igd database from excluded ranges cache (must install C version)
```console
./bin/igd create "/home/drc/GITHUB/bedboss/bedboss/scripts/ref_genome_validating/data/excludedranges/cachedfiles/all_beds/" "/home/drc/GITHUB/bedboss/bedboss/scripts/ref_genome_validating/data/excludedranges/igd" "EXCLUDED_RANGES_IGD_DATABASE" 
```
```console
nCtgs, nRegions, nTiles: 37      280204  216259
Save igd database to /home/drc/GITHUB/bedboss/bedboss/scripts/ref_genome_validating/data/excludedranges/igd/EXCLUDED_RANGES_IGD_DATABASE.igd
Total intervals, l_avg:  94982    32011.179
```