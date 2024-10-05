

import subprocess
import socket
import signal
from subprocess import Popen, PIPE, STDOUT
import os 

def run_bed_through_R(path, host = "127.0.0.1", port = 8888):
    s = socket.socket()
    s.connect((host, port))
    s.send(path.encode())
    s.close()

cmd = ["Rscript", "tools/r-service.R"]
p = subprocess.Popen(cmd, shell=False, preexec_fn=os.setsid)
print(f"Running R process with PID: {p.pid}")

# Run any BED files through bedstat

run_bed_through_R("bedstat/data/beds/bed1.bed") 

# End the R process
run_bed_through_R("done")

# Should probably do something like p.communicate() or p.wait() or something to wait until it ends.
p.terminate()





# Make sure it's terminated
try:
    run_bed_through_R("test")
    # Force kill it from Python because the done signal failed
    os.killpg(os.getpgid(p.pid), signal.SIGTERM)
except ConnectionErrorRefused:
    pass  # it's was killed

