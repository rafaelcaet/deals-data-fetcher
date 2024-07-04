from helpers.poll import *
import schedule
import time
import os
import sys


def run_poll():
    poll = Poll()
    poll.start()

def clearScreen():
    os.system("clear")

def main():
    
    print('>>> Initializing Poll <<<')
    run_poll()
    schedule.every(1).minute.do(run_poll)
    
    i = 0
    
    # Loop para manter a execução contínua
    clearScreen()
    while True:
        i += 1
        schedule.run_pending()
        time.sleep(1)
        print(i)

    ## 
    # poll = Poll()
    # poll.start()

if __name__ == "__main__":
    main()
