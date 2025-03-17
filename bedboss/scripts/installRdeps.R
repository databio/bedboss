.install_pkg = function(p, bioc=FALSE) {
    if(!require(package = p, character.only=TRUE)) {
        if(bioc) {
            BiocManager::install(pkgs = p)
        } else {
            install.packages(pkgs = p)   
        }
    }
}

.install_pkg("R.utils")
.install_pkg("BiocManager")
.install_pkg("optparse")
.install_pkg("devtools")
install.packages("https://cran.r-project.org/src/contrib/Archive/rjson/rjson_0.2.20.tar.gz", repos = NULL, type = "source")
.install_pkg("GenomicRanges", bioc=TRUE)
.install_pkg("GenomicFeatures", bioc=TRUE)
.install_pkg("ensembldb", bioc=TRUE)
.install_pkg("LOLA", bioc=TRUE)
.install_pkg("BSgenome", bioc=TRUE)
.install_pkg("ExperimentHub", bioc=TRUE)
.install_pkg("AnnotationHub", bioc=TRUE)
.install_pkg("conflicted")
.install_pkg("svSocket")
if(!require(package = "GenomicDistributions", character.only=TRUE)) {
    devtools::install_github("databio/GenomicDistributions", ref="dev_kinga")
}
options(timeout=2000)
if(!require(package = "GenomicDistributionsData", character.only=TRUE)) {
    install.packages("http://big.databio.org/GenomicDistributionsData/GenomicDistributionsData_0.0.2.tar.gz", repos=NULL)
}
