args <- commandArgs(trailingOnly = FALSE)
script_path <- sub("--file=", "", args[grep("--file=", args)]) # Extract script path
script_dir <- dirname(normalizePath(script_path))
source(file.path(script_dir, "regionstat.R"))

library(optparse)
option_list = list(
  make_option(c("--bedfilePath"), type="character", default=NULL,
              help="full path to a BED file to process", metavar="character"),
  make_option(c("--openSignalMatrix"), type="character",
              help="path to the open signal matrix required for the tissue specificity plot", metavar="character"),
  make_option(c("--digest"), type="character", default=NULL,
              help="digest of the BED file", metavar="character"),
  make_option(c("--outputFolder"), type="character", default="output",
              help="base output folder for results", metavar="character"),
  make_option(c("--genome"), type="character", default="hg38",
              help="genome reference to calculate against", metavar="character"),
  make_option(c("--ensdb"), type="character",
              help="path to the Ensembl annotation gtf file", metavar="character")
)

opt_parser = OptionParser(option_list=option_list);
opt = parse_args(opt_parser);

if (is.null(opt$bedfilePath)) {
  print_help(opt_parser)
  stop("Bed file input missing.")
}

if (is.null(opt$digest)) {
  print_help(opt_parser)
  stop("digest input missing.")
}

if (is.null(opt$digest)) {
  print_help(opt_parser)
  stop("digest input missing.")
}

runBEDStats(opt$bedfilePath, opt$digest, opt$outputFolder, opt$genome, opt$openSignalMatrix, opt$ensdb)
