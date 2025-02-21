setStatus = function(status) {
	# write status to status lock file
	message("Setting status to ", status)
	STATUS <<- status
}

# This function should run the process
processBED = function(path, client, port) {
	sc = svSocket::get_socket_clients()
	message("Signal received: ", path)
	if (path == "check") {
		message("Sending status to client: ", STATUS)
		message("socket client:", client)
		svSocket::send_socket_clients(STATUS, sockets=client)
		return(0)
	}

	if (path == "done") {  # Secret shutdown signal
		message("Received done signal")
		assign("done", TRUE, envir=.GlobalEnv)
		return(0)
	}

	message("Processing BED file: ", path)
	setStatus("processing")
	Sys.sleep(5)
	if (!file.exists(path)) {
		message("File not found: ", path)
		setStatus("idle")
		return(1)
	}

	#  process the bed file
	setStatus("idle")
	return(1)
}

message("Starting R server")
STATUS = ""
setStatus("starting")
setStatus("idle")
svSocket::start_socket_server(procfun=processBED)

message ("R server started")
while (!exists("done")) Sys.sleep(1)

setStatus("finished")

message("Shutting down R service")


