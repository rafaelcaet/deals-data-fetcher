import os
from datetime import datetime


def logger(date, msg='', status='ok', e=''):

    if not os.path.exists('logs'): os.makedirs('logs')
    with open('./logs/log.txt', 'a') as f:
        match status:
            case 'ok':
                f.write(f"[OK] Poll started at {date}\n")
                pass
            case 'paused':
                f.write(f"[PAUSED] {msg} {date}\n")
            case 'crashed':
                f.write(f"[ERROR] {msg} {date} :: {e}\n")
