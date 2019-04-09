import os
import signal
import subprocess
import sys
import time
from multiprocessing import Process, Pipe


def spawn_server_bg(pipe):
    proc = subprocess.Popen(["python", "./syncpri/server.py"],
                            env=os.environ.copy(),
                            stdout=subprocess.PIPE)

    pipe.send([proc.pid])
    while True:
        out = proc.stdout.read(1)
        if out == '' and proc.poll() != None:
            break
        if out != '':
            try:
                sys.stdout.write(out)
            except:
                pass
            sys.stdout.flush()


def with_server_in_background(fn):
    def wrapper(*args, **kwargs):
        parent_conn, child_conn = Pipe()
        p = Process(target=spawn_server_bg, args=(child_conn,))
        p.start()
        time.sleep(1)
        pid = parent_conn.recv()
        fn(*args, **kwargs)
        os.kill(int(pid[0]), signal.SIGTERM)
        p.kill()
    return wrapper
