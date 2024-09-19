library(optparse)
library(data.table)
library(GenomicRanges)
library(LOLA)
library(ggplot2)
library(conflicted)
library(R.utils)

option_list = list(
    make_option(c("--bedfilelist"), type="character", default=NULL, 
                help="path to a txt file with list of BED files to process", 
                metavar="character"),
    make_option(c("--outputfolder"), type="character", default="output",
                help="base output folder for results", metavar="character"),
    make_option(c("--json"), type="character", default="output",
                help="path to the target JSON file", metavar="character"),
    make_option(c("--id"), type="character", default=NULL,
                help="BED set human-readable ID to use for output files prefix", 
                metavar="character")
)
opt_parser = OptionParser(option_list=option_list)
opt = parse_args(opt_parser)

if (is.null(opt$bedfilelist)) {
    print_help(opt_parser)
    stop("bedfilelist input missing.")
}

if (is.null(opt$outputfolder)) {
    print_help(opt_parser)
    stop("outputfolder input missing.")
}

if (is.null(opt$id)) {
    print_help(opt_parser)
    stop("id input missing.")
}

if (is.null(opt$json)) {
    print_help(opt_parser)
    stop("json input missing.")
}

#' Generate a universe matrix
#' 
#' Generates a universe matrix based on a list of refgionsets
#'
#' @param queryList 
#'
#' @return matrix where rows are regions and cols are a binary indications 
#' whether a regionset includes the region
#' 
#' @export
.getUniverseMtx <- function(queryList) {
    message("creating universe...")
    universe = (Reduce(c, queryList))
    mtx = matrix(data=0, nrow=length(universe), ncol=length(queryList))
    message("finding overlaps...")
    hits = sapply(queryList, function(x) (findOverlaps(x, universe)))
    for(e in seq_along(hits)){
        mtx[hits[[e]]@to, e] = 1
    }
    mtx
}

#' Calculate region commonality in a regionset
#'
#' Calculates how many regionsets (bedfiles) overlap at least said percentage 
#' of regions included in the universe. The universe is considered a union of 
#' all regionsets (bedfiles) in the collection of 
#' regionsets (bedset, or set of bedfiles)
#'
#' @param queryList GRangesList object with regionsets to be considered
#'
#' @return data.table with two columns: Perc with percentages and Counts with 
#' number of regionsets having at least this percentage of overlaps with 
#' the universe
#' 
#' @export
calcRegionCommonality <- function(queryList){
    mtx = .getUniverseMtx(queryList)
    per = (colSums(mtx)/dim(mtx)[1])*100
    x = unique(c(0, per))
    a=c()
    for(i in seq_along(x)){
        a[i] = length(which(per >= x[i]))
    }
    df = data.table(Perc=x, Counts=a)
    df
}

#' Plot region commonality in a regionset
#'
#' @param percCounts data.table with two columns: Perc with percentages and Counts with 
#' number of regionsets having at least this percentage of overlaps with 
#' the universe
#'
#' @return ggplot object
#' 
#' @export
plotRegionCommonality <- function(percCounts) {
    g = ggplot(percCounts, aes(x=Perc, y=Counts)) + 
        geom_point() +
        theme_bw() +
        geom_line(linetype="dotted", linewidth=0.1) +
        theme(aspect.ratio=1) + 
        xlab("Percentage of regions in universe (BED set) covered") +
        ylab("Regionset (BED file) count") +
        ggtitle("Region commonality") +
        xlim(0, 100) +
        ylim(0, 100)
    return(g)
}

plotBoth <- function(plotId, g){
    pth = paste0(opt$outputfolder, "/", opt$id, "_", plotId)
    print(paste0("Plotting: ", pth))
    ggplot2::ggsave(paste0(pth, ".png"), g, device="png", width=8, height=8, units="in")
    ggplot2::ggsave(paste0(pth, ".pdf"), g, device="pdf", width=8, height=8, units="in")
}

getPlotReportDF <- function(plotId, title){
    pth = paste0(opt$outputfolder, "/", opt$id, "_", plotId)
    print(paste0("Writing: ", pth))
    rel_pth = getRelativePath(pth, paste0(opt$outputfolder, ""))
    print(paste0("Writing: ", rel_pth))
    newPlot = data.frame(
        "name"=plotId, 
        "title"=title, 
        "thumbnail_path"=paste0(rel_pth, ".png"), 
        "path"=paste0(rel_pth, ".pdf"),
        stringsAsFactors = FALSE
    )
    return(newPlot)
}

doItAll <- function(opt) {
    bedlist = read.table(file=opt$bedfilelist, stringsAsFactors=FALSE)
    grl = GRangesList()
    for(i in seq_len(NROW(bedlist))){
        bed_path = paste0(bedlist[i, 1])
        if(!file.exists(bed_path)) stop("File not found: ", bed_path)
        message("reading BED: ", bed_path)
        grl[[i]] = LOLA::readBed(bed_path)
    }
    plotBoth("region_commonality", plotRegionCommonality(calcRegionCommonality(grl)))
    print(paste0("done plotting "))
    plots = getPlotReportDF("region_commonality", "BED region commonality in BED set")
    # Note: names of the list elements MUST match what's defined in: https://github.com/databio/bbconf/blob/master/bbconf/schemas/bedsets_schema.yaml
    write(jsonlite::toJSON(list(plots=plots), pretty=TRUE), opt$json)
    message("Saved JSON: ", opt$json)
}

bedlist = opt$bedfilelist
doItAll(opt=opt)
