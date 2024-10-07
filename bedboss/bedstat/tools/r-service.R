# This function should run the process
processBED = function(path, client, port) {
	message("Processing BED file: ", path)
	if (path == "done") {  # Secret shutdown signal
		message("Received done signal")
		assign("done", TRUE, envir=.GlobalEnv)
		return(0)
	}	
	if (!file.exists(path)) {
		message("File not found: ", path)
		return(1)
	}
	return(1)
}

message("Starting R server")
svSocket::start_socket_server(procfun=processBED)
message ("R server started")
while (!exists("done")) Sys.sleep(1)

message("Shutting down R service")
