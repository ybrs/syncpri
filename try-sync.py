import redis
import time

rds = redis.Redis(port=7878)

lock = rds.execute_command("LOCK", "foo", 10)
while True:
    print("locked")
    time.sleep(3)
    print("sleeping done", lock)
    print(rds.execute_command('RELEASE', 'foo'))
    break