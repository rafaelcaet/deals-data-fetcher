from helpers.poll import *
import schedule
import time
import os


def run_poll():
    poll = Poll()
    poll.start()


def main():

    # Running with 1 minute schedule

    # run_poll()
    # schedule.every(1).minute.do(run_poll)
    # while True:
    #     schedule.run_pending()
    #     time.sleep(1)

    # Running without schedule

    poll = Poll()
    poll.start()


if __name__ == "__main__":
    main()
