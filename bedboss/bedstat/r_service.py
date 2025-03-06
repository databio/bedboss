import subprocess
import socket
import signal
import os
import re
import time
from typing import Union

from bedboss.const import PKG_NAME
from logging import getLogger

_LOGGER = getLogger(PKG_NAME)

script_path = os.path.join(
    os.path.dirname(os.path.realpath(__file__)), "tools/r-service.R"
)


class RServiceManager:
    """
    A class to manage the lifecycle of an R service, allowing files to be processed through the service.

    Attributes:
        r_script_path (str): Path to the R script that starts the service.
        host (str): Host address for the socket connection.
        port (int): Port number for the socket connection.
        process (subprocess.Popen): The process running the R service.
    """

    def __init__(self, host="127.0.0.1", port=8888):
        """
        Initializes the RServiceManager with the given R script path, host, and port.

        Args:
            r_script_path (str): Path to the R script that starts the service.
            host (str): Host address for the socket connection. Default is "127.0.0.1".
            port (int): Port number for the socket connection. Default is 8888.
        """
        self.r_script_path = script_path
        self.host = host
        self.port = port
        self.process = None

        self.start_service()

    def start_service(self):
        """
        Starts the R service by running the R script in a subprocess.
        """
        _LOGGER.info("RService: Starting R service...")
        cmd = ["Rscript", self.r_script_path]
        self.process = subprocess.Popen(cmd, shell=False, preexec_fn=os.setsid)

        while True:
            if self.check_status() == "idle":
                _LOGGER.info(
                    f"RService: Running R process with PID: {self.process.pid}"
                )
                break
            time.sleep(2)

    def run_file(
        self,
        file_path: str,
        digest: str,
        outpath: str,
        genome: str,
        openSignalMatrix: Union[str, None],
        gtffile: Union[str, None],
    ):
        """
        Sends a file path to the R service for processing.

        :param file_path: Path to the file to be processed.
        :param digest: Digest of the file.
        :param outpath: Path to the output directory.
        :param genome: Genome assembly.
        :param openSignalMatrix: Path to the Open Signal Matrix file.
        :param gtffile: Path to the GTF file.

        :return: None
        :exit: 1 if the connection is refused.
        """
        return self.run_command(
            f"{file_path}, {digest}, {outpath}, {genome}, {openSignalMatrix}, {gtffile}\n"
        )

    def run_command(self, command):
        """
        Sends a command to the R service for processing.
        """
        try:
            s = socket.socket()
            s.connect((self.host, self.port))

            _LOGGER.info(f"RService: Sending command: {command}")
            s.send(command.encode())

            while True:
                msg = self.check_status()
                if msg == "idle":
                    _LOGGER.debug(f"RService: Message recieved: {msg}")
                    break
                else:
                    _LOGGER.info(f"RService: Message recieved: {msg}")
                time.sleep(1)
            s.close()
            return s
        except ConnectionRefusedError:
            # BedBossException("Connection refused. Make sure the R service is running.")
            _LOGGER.error(
                "RService: Connection refused. Make sure the R service is running. Unable to send command."
            )
            exit(1)

    def check_status(self):
        """
        Checks the status of the R service by sending a "check" message to the service.
        """
        try:
            s = socket.socket()
            s.connect((self.host, self.port))
            s.send("check\n".encode())
            msg = s.recv(1024).decode()
            key = re.split(r"[\r\n]", msg)[0]

            _LOGGER.debug(f"RService: Received message: {key}")
            s.close()

            return key
        except ConnectionRefusedError:
            _LOGGER.warning("Connection refused. Make sure the R service is running.")

    def terminate_service(self):
        """
        Terminates the R service by sending a termination signal and ensuring the process is stopped.
        """
        self.run_command("done")  # send secrete "terminate" code
        if self.process:
            self.process.terminate()
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                os.killpg(os.getpgid(self.process.pid), signal.SIGTERM)
            _LOGGER.info("RService: R process terminated.")

    def __del__(self):
        self.terminate_service()
