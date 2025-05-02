## HERE I AM sourcing the helper functions
# Get the script's directory when running with Rscript
args <- commandArgs(trailingOnly = FALSE)
script_path <- sub("--file=", "", args[grep("--file=", args)]) # Extract script path
script_dir <- dirname(normalizePath(script_path))              # Get directory

# Source the helper functions
source(file.path(script_dir, "regionstat.R"))

runAnalysis = function(arguments) {
    message("R message => Running analysis with arguments: ", arguments)

    items <- strsplit(arguments, ", ")[[1]]

    bedPath <- items[1]
    digest <- items[2]
    outfolder <- items[3]
    genome <- items[4]
    openSignalMatrix <- items[5]
    gtffile <- items[6]

    if (!file.exists(bedPath)) {
		message("R message => File not found: ", path)
		setStatus("idle")
		return(1)
	}

    runBEDStats(bedPath, digest, outfolder, genome, openSignalMatrix, gtffile)

    return(0)
}

setStatus = function(status) {
	# message("R message => Setting status to ", status)
	assign("STATUS", status, .GlobalEnv)
}

# This function should run the process
processBED = function(path, client, port) {
	# message("R message => Signal received: ", path)

	if (path == "check") { # Status check signal
		# message("R message => Sending status to client: ", STATUS)
		# message("R message => socket client:", client)
		svSocket::send_socket_clients(STATUS, sockets=client)
		return(0)
	}

	if (path == "done") {  # Secret shutdown signal
		message("R message => Received done signal")
		assign("done", TRUE, envir=.GlobalEnv)
		return(0)
	}

	setStatus("processing")

    tryCatch({
        runAnalysis(path)
    }, error = function(e) {
        message("R message => Error: ", conditionMessage(e))
    }, finally = {
        # message("R message => Finished processing")
    })

	setStatus("idle")
	return(1)
}

message("R message => Starting R server")
STATUS = ""
svSocket::start_socket_server(procfun=processBED)
setStatus("idle")
message ("R message => R server started")

while (!exists("done")) Sys.sleep(1)

setStatus("finished")

message("R message => Shutting down R service")
