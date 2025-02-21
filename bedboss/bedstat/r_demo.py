import subprocess
import socket
import signal
import os
import re


class RServiceManager:
    """
    A class to manage the lifecycle of an R service, allowing files to be processed through the service.

    Attributes:
        r_script_path (str): Path to the R script that starts the service.
        host (str): Host address for the socket connection.
        port (int): Port number for the socket connection.
        process (subprocess.Popen): The process running the R service.
    """

    def __init__(self, r_script_path="tools/r-service.R", host="127.0.0.1", port=8888):
        """
        Initializes the RServiceManager with the given R script path, host, and port.

        Args:
            r_script_path (str): Path to the R script that starts the service.
            host (str): Host address for the socket connection. Default is "127.0.0.1".
            port (int): Port number for the socket connection. Default is 8888.
        """
        self.r_script_path = r_script_path
        self.host = host
        self.port = port
        self.process = None

    def start_service(self):
        """
        Starts the R service by running the R script in a subprocess.
        """
        cmd = ["Rscript", self.r_script_path]
        self.process = subprocess.Popen(cmd, shell=False, preexec_fn=os.setsid)
        print(f"Running R process with PID: {self.process.pid}")

    def run_file(self, file_path):
        """
        Sends a file path to the R service for processing.

        Args:
            file_path (str): The path to the file to be processed by the R service.
        """
        try:
            s = socket.socket()
            s.connect((self.host, self.port))
            s.send(f"{file_path}\n".encode())
            s.close()
            return s 
        except ConnectionRefusedError:
            print("Connection refused. Make sure the R service is running.")

    def check_status(self):
        """
        Checks the status of the R service by sending a "check" message to the service.
        """
        try:
            s = socket.socket()
            s.connect((self.host, self.port))
            s.send("check\n".encode())
            msg = s.recv(1024).decode()
            key = re.split(r'[\r\n]', msg)[0]
            print(f"Received message: {key}")
            # s.shutdown(socket.SHUT_WR)
            s.close()
            return key
        except ConnectionRefusedError:
            print("Connection refused. Make sure the R service is running.")

    def terminate_service(self):
        """
        Terminates the R service by sending a termination signal and ensuring the process is stopped.
        """
        self.run_file("done")  # send secrete "terminate" code
        if self.process:
            self.process.terminate()
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                os.killpg(os.getpgid(self.process.pid), signal.SIGTERM)
            print("R process terminated.")


# TODO: "tools/r-service.R" needs to be embedded in the package


# Start the R service at the beginning of the pipeline
rsm = RServiceManager("tools/r-service.R")
rsm.start_service()

st = rsm.check_status()

# Run any BED files through bedstat
res = rsm.run_file("bedstat/data/beds/bed1.bed")
rsm.run_file("../../test/data/bed/simpleexamples/bed1.bed")

# After the pipeline finishes, terminate the R service
rsm.terminate_service()
