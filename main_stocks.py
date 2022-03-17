# import classes.Live.getLive as l

# # Get Live Data
# l.getData()
import time


def job():
    a = 1+1
    print("thread is alive", a)


while True:
    job()
    time.sleep(86400)
