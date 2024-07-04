from helpers.poll import *
import schedule
import time


def run_poll():
    poll = Poll()
    poll.start()


def main():

    print('>>> Initializing Poll <<<')
    run_poll()
    schedule.every(1).minute.do(run_poll)

    # Loop para manter a execução contínua
    while True:
        schedule.run_pending()
        time.sleep(1)


if __name__ == "__main__":
    main()
