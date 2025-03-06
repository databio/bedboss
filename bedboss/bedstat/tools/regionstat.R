# R Script that generates statistics for a bedfile

library(GenomicDistributions)
# library(GenomicDistributionsData)
# library(GenomeInfoDb)
# library(ensembldb)

library(tools)
library(R.utils)
library(rjson)

trim <- IRanges::trim

myPartitionList <- function(gtffile){
  features = c("gene", "exon", "three_prime_utr", "five_prime_utr")
  geneModels = getGeneModelsFromGTF(gtffile, features, TRUE)
  partitionList = genomePartitionList(geneModels$gene,
                                      geneModels$exon,
                                      geneModels$three_prime_utr,
                                      geneModels$five_prime_utr)

  return (partitionList)
}


myChromSizes <- function(genome){
  if (requireNamespace(BSgm, quietly=TRUE)){
    library (BSgm, character.only = TRUE)
    BSG = eval(as.name(BSgm))
  } else {
    library (BSg, character.only = TRUE)
    BSG = eval(as.name(BSg))
  }
  chromSizesGenome = seqlengths(BSG)
  return(chromSizesGenome)
}

plotBoth <- function(plotId, g, digest, outfolder){
  pth = paste0(outfolder, "/", digest, "_", plotId)
  print(paste0("Plotting: ", pth))
  ggplot2::ggsave(paste0(pth, ".png"), g, device="png", width=8, height=8, units="in")
  ggplot2::ggsave(paste0(pth, ".pdf"), g, device="pdf", width=8, height=8, units="in")
}

getPlotReportDF <- function(plotId, title, digest, outfolder){
  pth = paste0(outfolder, "/", digest, "_", plotId)
  rel_pth = getRelativePath(pth, paste0(outfolder, "/../../../"))
  print(paste0("Writing plot json: ", rel_pth))
  newPlot = data.frame(
    "name"=plotId,
    "title"=title,
    "thumbnail_path"=paste0(rel_pth, ".png"),
    "path"=paste0(rel_pth, ".pdf")
  )
  return(newPlot)
}


doItAll <- function(query, digest, genome, openSignalMatrix, outfolder, BSg, BSgm, gtffile) {
  plots = data.frame(stringsAsFactors=F)
  bsGenomeAvail = ifelse((requireNamespace(BSg, quietly=TRUE) | requireNamespace(BSgm, quietly=TRUE)), TRUE, FALSE)

  # check if json file exist for the input bed file
  meta_path = paste0(outfolder, "/", digest, ".json")
  plot_path = paste0(outfolder, "/", digest, "_plots.json")
  if (file.exists(meta_path)){
    bedmeta = fromJSON(file=meta_path)
    plots = fromJSON(file=plot_path)
    plots = as.data.frame(do.call(rbind, plots))
  } else{
    plots = data.frame(stringsAsFactors=F)
  }

  run_plot = TRUE
  # TSS distance plot
  if (exists("bedmeta")){
    if ("median_TSS_dist" %in% names(bedmeta)){
      run_plot = FALSE
    } else {
      run_plot = TRUE
    }
  } else {
    run_plot = TRUE
  }
  query_new = GenomeInfoDb::keepStandardChromosomes(query, pruning.mode="coarse")
  if (run_plot){
    tryCatch(
      expr = {
        if (!(genome %in% c("hg19", "hg38", "mm10", "mm9")) && gtffile == "None"){
          message("Ensembl annotation gtf file not provided. Skipping TSS distance plot ... ")
        } else{
          if (genome %in% c("hg19", "hg38", "mm10", "mm9")) {
            TSSdist = calcFeatureDistRefTSS(query_new, genome)
            plotBoth("tss_distance", plotFeatureDist( TSSdist, featureName="TSS"), digest, outfolder)
          } else {
            tss = getTssFromGTF(gtffile, TRUE)
            TSSdist = calcFeatureDist(query_new, tss)
            plotBoth("tss_distance", plotFeatureDist( TSSdist, featureName="TSS"), digest, outfolder)
          }
          plots = rbind(plots, getPlotReportDF("tss_distance", "Region-TSS distance distribution", digest, outfolder))
          message("Successfully calculated and plot TSS distance.")
        }
        if (exists("bedmeta")){
          tss <- list(median_TSS_dist = signif(median(abs(TSSdist), na.rm=TRUE), digits = 4))
          bedmeta = append(bedmeta, tss)
        }

      },
      error = function(e){
        message('Caught an error in creating: TSS distance plot!')
        print(e)
      }
    )
  }

  # Chromosomes region distribution plot
  if (!exists("bedmeta") ){
    tryCatch(
      expr = {
        if (genome %in% c("mm39", "dm3", "dm6", "ce10", "ce11", "danRer10", "danRer10", "T2T")){
          chromSizes = myChromSizes(genome)
          genomeBins  = getGenomeBins(chromSizes)
          plotBoth("chrombins", plotChromBins(calcChromBins(query, genomeBins)), digest, outfolder)
        } else{
          plotBoth("chrombins", plotChromBins(calcChromBinsRef(query_new, genome)), digest, outfolder)
        }

        plots = rbind(plots, getPlotReportDF("chrombins", "Regions distribution over chromosomes", digest, outfolder))
        message("Successfully calculated and plot chromosomes region distribution.")
      },
      error = function(e){
        message('Caught an error in creating: Chromosomes region distribution plot!')
        print(e)
      }
    )
  }

# We are calculating this differently now
#   # OPTIONAL: Plot GC content only if proper BSgenome package is installed.
#   if (exists("bedmeta")){
#     if ("gc_content" %in% names(bedmeta)){
#       run_plot = FALSE
#     } else {
#       run_plot = TRUE
#     }
#   } else {
#       run_plot = TRUE
#   }
#
#   if (run_plot){
#     if (bsGenomeAvail) {
#       tryCatch(
#         expr = {
#           if (requireNamespace(BSgm, quietly=TRUE)){
#             library (BSgm, character.only = TRUE)
#             bsg = eval(as.name(BSgm))
#             gcvec = calcGCContent(query, bsg)
#           } else {
#             library (BSg, character.only = TRUE)
#             bsg = eval(as.name(BSg))
#             gcvec = calcGCContent(query, bsg)
#           }
#           plotBoth("gccontent", plotGCContent(gcvec))
#           if (exists("bedmeta")){
#             gc_content <- list(gc_content = signif(mean(gcvec), digits = 4))
#             bedmeta = append(bedmeta, gc_content)
#           }
#           plots = rbind(plots, getPlotReportDF("gccontent", "GC content"))
#           message("Successfully calculated and plot GC content.")
#         },
#         error = function(e){
#           message('Caught an error in creating: GC content plot!')
#           print(e, gcvec)
#         }
#       )
#     }
#   }


  # Partition plots, default to percentages
  if (exists("bedmeta")){
    if ("exon_frequency" %in% names(bedmeta)){
      run_plot = FALSE
    } else {
      run_plot = TRUE
    }
  } else {
      run_plot = TRUE
  }

  if (run_plot){
    tryCatch(
      expr = {
        if (!(genome %in% c("hg19", "hg38", "mm10")) && gtffile == "None"){
          message("Ensembl annotation gtf file not provided. Skipping partition plot ... ")
        } else {
          if (genome %in% c("hg19", "hg38", "mm10")) {
            gp = calcPartitionsRef(query, genome)
            plotBoth("partitions", plotPartitions(gp), digest, outfolder)
          } else {
            partitionList = myPartitionList(gtffile)
            gp = calcPartitions(query, partitionList)
            plotBoth("partitions", plotPartitions(gp), digest, outfolder)
          }
          plots = rbind(plots, getPlotReportDF("partitions", "Regions distribution over genomic partitions", digest, outfolder))
          # flatten the result returned by the function above
          partiotionNames = as.vector(gp[,"partition"])
          partitionsList = list()
          for(i in seq_along(partiotionNames)){
            partitionsList[[paste0(partiotionNames[i], "_frequency")]] =
              as.vector(gp[,"Freq"])[i]
            partitionsList[[paste0(partiotionNames[i], "_percentage")]] =
              as.vector(gp[,"Freq"])[i]/length(query)
          }
          if (exists("bedmeta")){
            bedmeta = append(bedmeta, partitionsList)
          }
          message("Successfully calculated and plot regions distribution over genomic partitions.")
        }
      },
      error = function(e){
        message('Caught an error in creating: Partition plot!')
        print(e)
      }
    )
  }


  # Expected partition plots
  if (!exists("bedmeta") ){
    tryCatch(
      expr = {
        if (!(genome %in% c("hg19", "hg38", "mm10")) && gtffile == "None"){
          message("Ensembl annotation gtf file not provided. Skipping expected partition plot ... ")
        } else{
          if (genome %in% c("hg19", "hg38", "mm10")) {
            plotBoth("expected_partitions", plotExpectedPartitions(calcExpectedPartitionsRef(query, genome)), digest, outfolder)
          } else {
            partitionList = myPartitionList(gtffile)
            chromSizes = myChromSizes(genome)
            genomeSize = sum(chromSizes)
            plotBoth("expected_partitions", plotExpectedPartitions(calcExpectedPartitions(query, partitionList, genomeSize)), digest, outfolder)
          }
          plots = rbind(plots, getPlotReportDF("expected_partitions", "Expected distribution over genomic partitions", digest, outfolder))
          message("Successfully calculated and plot expected distribution over genomic partitions.")
        }
      },
      error = function(e){
        message('Caught an error in creating: Expected partition plot!')
        print(e)
      }
    )
  }

  # Cumulative partition plots
  if (!exists("bedmeta") ){
    tryCatch(
      expr = {
        if (!(genome %in% c("hg19", "hg38", "mm10")) && gtffile == "None"){
          message("Ensembl annotation gtf file not provided. Skipping cumulative partition plot ... ")
        } else{
          if (genome %in% c("hg19", "hg38", "mm10")) {
            plotBoth("cumulative_partitions", plotCumulativePartitions(calcCumulativePartitionsRef(query, genome)), digest, outfolder)
          } else{
            partitionList = myPartitionList(gtffile)
            plotBoth("cumulative_partitions", plotCumulativePartitions(calcCumulativePartitions(query, partitionList)), digest, outfolder)
          }
          plots = rbind(plots, getPlotReportDF("cumulative_partitions", "Cumulative distribution over genomic partitions", digest, outfolder))
          message("Successfully calculated and plot cumulative distribution over genomic partitions.")
        }
      },
      error = function(e){
        message('Caught an error in creating: Cumulative partition plot!')
        print(e)
      }
    )
  }

  # QThis plot
  if (exists("bedmeta")){
    if ("mean_region_width" %in% names(bedmeta)){
      run_plot = FALSE
    } else {
      run_plot = TRUE
    }
  } else {
      run_plot = TRUE
  }

  if (run_plot){
    tryCatch(
      expr = {
        widths = calcWidth(query)
        plotBoth("widths_histogram", plotQTHist(widths), digest, outfolder)
        if (exists("bedmeta")){
          mean_region_width <- list(mean_region_width = signif(mean(widths), digits = 4))
          bedmeta = append(bedmeta, mean_region_width)
        }
        plots = rbind(plots, getPlotReportDF("widths_histogram", "Quantile-trimmed histogram of widths", digest, outfolder))
        message("Successfully calculated and plot quantile-trimmed histogram of widths.")
      },
      error = function(e){
        message('Caught an error in creating: Quantile-trimmed histogram of widths plot!')
        print(e, widths)
      }
    )
  }

  # Neighbor regions distance plots
  if (!exists("bedmeta") ){
    tryCatch(
      expr = {
        plotBoth("neighbor_distances", plotNeighborDist(calcNeighborDist(query)), digest, outfolder)
        plots = rbind(plots, getPlotReportDF("neighbor_distances", "Distance between neighbor regions", digest, outfolder))
        message("Successfully calculated and plot distance between neighbor regions.")
      },
      error = function(e){
        message('Caught an error in creating: Distance between neighbor regions plot!')
        print(e)
      }
    )
  }

  ## This part is heavy and if needed can be skipped
  # Tissue specificity plot if open signal matrix is provided
  if (!exists("bedmeta") ){
    if (openSignalMatrix == "None") {
      message("open signal matrix not provided. Skipping tissue specificity plot ... ")
    } else {
      tryCatch(
        expr = {
          plotBoth("open_chromatin", plotSummarySignal(calcSummarySignal(query, data.table::fread(openSignalMatrix))), digest, outfolder)
          plots = rbind(plots, getPlotReportDF("open_chromatin", "Cell specific enrichment for open chromatin", digest, outfolder))
          message("Successfully calculated and plot cell specific enrichment for open chromatin.")
        },
        error = function(e){
          message('Caught an error in creating: Cell specific enrichment for open chromatin plot!')
          print(e)
        }
      )
    }
  }


  # Note: names of the list elements MUST match what's defined in: https://github.com/databio/bbconf/blob/master/bbconf/schemas/bedfiles_schema.yaml
  if (exists("bedmeta")){
    write(jsonlite::toJSON(bedmeta, pretty=TRUE), meta_path)
    write(jsonlite::toJSON(plots, pretty=TRUE, auto_unbox = TRUE), plot_path)
  } else {
    bedmeta = list(
      name=digest,
      number_of_regions=length(query),
      mean_region_width=ifelse(exists('widths'), signif(mean(widths), digits = 4), NA),
      md5sum=digest
    )
    if (exists('gcvec') && !isEmpty(gcvec)){
      gc_content <- list(gc_content = signif(mean(gcvec), digits = 4))
      bedmeta = append(bedmeta, gc_content)
    }
    if (exists('TSSdist') && !all(is.na(TSSdist))){
      tss <- list(median_TSS_dist = signif(median(abs(TSSdist), na.rm=TRUE), digits = 4))
      bedmeta = append(bedmeta, tss)
    }
    if (exists('partitionsList')){
      write(jsonlite::toJSON(c(bedmeta, partitionsList), pretty=TRUE), meta_path)
    } else {
      write(jsonlite::toJSON(c(bedmeta), pretty=TRUE), meta_path)
    }

    if (exists('plots')){
      write(jsonlite::toJSON(plots, pretty=TRUE, auto_unbox = TRUE), plot_path)
    }
  }
  }

runBEDStats = function (bedPath, digest, outfolder, genome, openSignalMatrix, gtffile) {
  # define values and output folder for doitall()
  message("R message =>  Running regionstat for: ", bedPath)
    message("R message =>  digest: ", digest)
    message("R message =>  outfolder: ", outfolder)
    message("R message =>  genome: ", genome)
    message("R message => openSignalMatrix: ", openSignalMatrix)
    message("R message =>  gtffile: ", gtffile)


  # build BSgenome package ID to check whether it's installed
    if ( startsWith(genome, "T2T")){
      BSg = "BSgenome.Hsapiens.NCBI.T2T.CHM13v2.0"
    } else {
      if (startsWith(genome, "hg") | startsWith(genome, "grch")) {
        orgName = "Hsapiens"
      } else if (startsWith(genome, "mm") | startsWith(genome, "grcm")){
        orgName = "Mmusculus"
      } else if (startsWith(genome, "dm")){
        orgName = "Dmelanogaster"
      } else if (startsWith(genome, "ce")){
        orgName = "Celegans"
      } else if (startsWith(genome, "danRer")){
        orgName = "Drerio"
      }  else if (startsWith(genome, "TAIR")){
        orgName = "Athaliana"
      } else {
        orgName = "Undefined"
      }
      BSg = paste0("BSgenome.", orgName , ".UCSC.", genome)
    }

  BSgm = paste0(BSg, ".masked")

  query = LOLA::readBed(bedPath)
  doItAll(query, digest, genome, openSignalMatrix, outfolder, BSg, BSgm, gtffile)
}
