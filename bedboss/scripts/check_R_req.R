check_required_packages <- function() {
  # ANSI escape codes for colors
  red <- "\033[31m"
  green <- "\033[32m"
  reset <- "\033[0m"

  # List of required R packages
  required_packages <- c(
    "optparse", "devtools", "ensembldb", "ExperimentHub", "AnnotationHub",
    "AnnotationFilter", "BSgenome", "GenomicFeatures", "GenomicDistributions",
    "GenomicDistributionsData", "GenomeInfoDb", "tools", "R.utils", "LOLA", "conflicted"
  )

  # Check if each package is installed
  missing_packages <- required_packages[!sapply(required_packages, requireNamespace, quietly = TRUE)]

  # Get the installed packages
  installed_packages <- required_packages[sapply(required_packages, requireNamespace, quietly = TRUE)]

  # If there are missing packages, print them
  if (length(missing_packages) > 0) {
    cat(paste0(red, "The following packages are missing:\n", reset))
    cat(paste0(red, missing_packages, collapse = "\n"), "\n", reset)  # Print missing packages vertically
  } else {
    cat(paste0(green, "All R required packages are installed.\n", reset))
  }

  # Print installed packages vertically
  cat(paste0(green, "\nThe following packages are installed:\n", reset))
  cat(paste0(green, installed_packages, collapse = "\n"), "\n", reset)  # Print installed packages vertically

  # Return missing packages
  return(missing_packages)
}

# Call the function
missing_packages <- check_required_packages()

# Optionally handle missing packages
if (length(missing_packages) > 0) {
  quit(status = 1)
}
quit(status = 0)