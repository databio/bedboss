setStatus = function(status) {
	message("Setting status to ", status)
	assign("STATUS", status, .GlobalEnv)
}

# This function should run the process
processBED = function(path, client, port) {
	message("Signal received: ", path)

	if (path == "check") { # Status check signal
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

	Sys.sleep(5)  # Simulate BED processing 

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
setStatus("idle")
svSocket::start_socket_server(procfun=processBED)

message ("R server started")
while (!exists("done")) Sys.sleep(1)

setStatus("finished")

message("Shutting down R service")


