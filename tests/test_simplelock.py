import subprocess
import sys
import os
from threading import Thread

import pytest
import pexpect
import redis
import time
from multiprocessing import Pool, Pipe
from functools import partial
import time

from multiprocessing import Process

def proc(lock_name, x):
    print(lock_name)
    rds = redis.Redis(port=7878)
    rds.ping()
    lock = rds.execute_command('LOCK', lock_name, 10)
    rds.rpush('rtest', x)
    rds.execute_command('RELEASE', lock_name, lock[0])

    return x


proc_1 = partial(proc, 'rtest1')
proc_2 = partial(proc, 'rtest2')
proc_3 = partial(proc, 'rtest3')
proc_4 = partial(proc, 'rtest4')

def spawn_server():
    child = pexpect.spawn('python ./syncpri/server.py')
    child.expect('Serving on')
    return child

def _test_server_running():
    child = spawn_server()
    rds = redis.Redis(port=7878)
    rds.ping()


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

def test_lock_server():
    parent_conn, child_conn = Pipe()
    p = Process(target=spawn_server_bg, args=(child_conn,))
    p.start()
    time.sleep(1)
    pid = parent_conn.recv()
    print("pid", pid)

    #
    rds = redis.Redis(port=7878)
    print(rds.ping())
    rds.delete('rtest')
    print("delete")
    #
    pool = Pool(processes=5)

    lock1 = rds.execute_command('LOCK', 'rtest1', 10)
    lock2 = rds.execute_command('LOCK', 'rtest2', 10)
    lock3 = rds.execute_command('LOCK', 'rtest3', 10)
    lock4 = rds.execute_command('LOCK', 'rtest4', 10)

    res = [
        pool.apply_async(proc_2, (2,)),
        pool.apply_async(proc_1, (1,)),
        pool.apply_async(proc_4, (4,)),
        pool.apply_async(proc_3, (3,)),
    ]
    print("sleep")
    time.sleep(1)
    print("done")
    rds.execute_command('RELEASE', 'rtest1', lock1[0])
    rds.execute_command('RELEASE', 'rtest2', lock2[0])
    rds.execute_command('RELEASE', 'rtest3', lock3[0])
    rds.execute_command('RELEASE', 'rtest4', lock4[0])
    print("released")

    for r in res:
        r.get(timeout=1)
    #
    assert [1,2,3,4] == [int(i) for i in rds.lrange('rtest', 0, 100)]

    import signal
    os.kill(int(pid[0]), signal.SIGTERM)
    p.kill()

if __name__ == '__main__':
    test_lock_server()